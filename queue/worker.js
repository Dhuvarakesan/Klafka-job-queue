const fs = require('fs');
const path = require('path');
const express = require('express');
const { Kafka } = require('kafkajs');
const pLimit = require('p-limit').default;
const db = require('../db');

const MONITOR_PORT = 3002;
const app = express();

/* ============================================================
   CONFIG
============================================================ */
const MAX_COMPLETED_LOGS = 5;

/* ============================================================
   DYNAMIC CONCURRENCY
============================================================ */
const getLimitValue = () => {
  const day = new Date().getDay();
  const isWeekend = day === 6 || day === 0;
  return isWeekend ? 5 : 3;
};

let limit = pLimit(getLimitValue());

/* ============================================================
   SCHEDULER STATE (FOR LOGGING ONLY)
============================================================ */
const schedulerState = {
  running: new Map(),   // slotNumber -> { jobId, duration }
  waiting: [],
  completed: [],
};

/* ============================================================
   SLOT HELPERS
============================================================ */
function getSlotSnapshot() {
  const total = limit.concurrency;
  const used = schedulerState.running.size;

  const slots = [];
  for (let i = 1; i <= total; i++) {
    if (schedulerState.running.has(i)) {
      const job = schedulerState.running.get(i);
      slots.push({
        slot: `Slot ${i}`,
        status: `${job.jobId} (RUNNING – ${job.duration}s)`
      });
    } else {
      slots.push({
        slot: `Slot ${i}`,
        status: 'FREE'
      });
    }
  }

  return {
    totalSlots: total,
    usedSlots: used,
    freeSlots: total - used,
    slots,
  };
}

/* ============================================================
   MARKDOWN TABLE LOGGER
============================================================ */
function printSchedulerTable(reason) {
  const snapshot = getSlotSnapshot();

  console.log('\n```md');
  console.log(`### Scheduler Update → ${reason}\n`);

  console.log(`| Metric | Value |`);
  console.log(`|-------|-------|`);
  console.log(`| Concurrency | ${snapshot.totalSlots} |`);
  console.log(`| Used Slots | ${snapshot.usedSlots} |`);
  console.log(`| Free Slots | ${snapshot.freeSlots} |`);

  console.log(`\n| Slot | Status |`);
  console.log(`|------|--------|`);
  snapshot.slots.forEach(s => {
    console.log(`| ${s.slot} | ${s.status} |`);
  });

  console.log(`\n| Waiting Queue |`);
  console.log(`|---------------|`);
  console.log(`| ${schedulerState.waiting.join(', ') || '—'} |`);

  console.log(`\n| Completed (latest) |`);
  console.log(`|--------------------|`);
  console.log(
    `| ${schedulerState.completed.slice(-MAX_COMPLETED_LOGS).join(', ') || '—'} |`
  );

  console.log('```\n');
}

/* ============================================================
   KAFKA CONSUMER
============================================================ */
const kafka = new Kafka({
  clientId: 'file-worker',
  brokers: ['localhost:9092'],
  retry: { retries: 10 }, //If Kafka broker is temporarily unavailable → it retries 10 times.
});

const consumer = kafka.consumer({
  groupId: 'file-upload-workers',
});

/* ============================================================
   JOB PROCESSOR
============================================================ */
async function processFile(job) {
  try {
    await db.query(
      `UPDATE jobs
       SET status='PROCESSING', started_at=NOW()
       WHERE job_id=$1`,
      [job.jobId]
    );

    const duration = Number(job.processingTime || 60000);
    await new Promise(r => setTimeout(r, duration));

    const { rows } = await db.query(
      `SELECT attachment_id, file_name
       FROM attachments
       WHERE job_id=$1`,
      [job.jobId]
    );

    if (!rows.length) throw new Error('No attachment found');

    const attachment = rows[0];
    const storageDir = path.join(__dirname, 'storage');
    if (!fs.existsSync(storageDir)) fs.mkdirSync(storageDir, { recursive: true });

    const filePath = path.join(
      storageDir,
      `${job.jobId}-${attachment.file_name}`
    );

    fs.writeFileSync(filePath, Buffer.from(job.buffer, 'base64'));

    await db.query(
      `UPDATE attachments
       SET storage_path=$2
       WHERE attachment_id=$1`,
      [attachment.attachment_id, filePath]
    );

    await db.query(
      `UPDATE jobs
       SET status='COMPLETED', completed_at=NOW()
       WHERE job_id=$1`,
      [job.jobId]
    );
  } catch (err) {
    await db.query(
      `UPDATE jobs
       SET status='FAILED', error_message=$2
       WHERE job_id=$1`,
      [job.jobId, err.message]
    );
  }
}

/* ============================================================
   START WORKER + TABLE LOGGER SCHEDULER
============================================================ */
(async () => {
  await consumer.connect();
  await consumer.subscribe({
    topic: 'file-upload-queue',
    fromBeginning: false,
  });

  app.listen(MONITOR_PORT, () => {
    console.log(`📊 Worker monitor running on port ${MONITOR_PORT}`);
  });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      const job = JSON.parse(message.value.toString());

      /* ---- Persist job ---- */
      await db.query(
        `INSERT INTO jobs (job_id, status, created_at)
         VALUES ($1, 'QUEUED', NOW())
         ON CONFLICT (job_id) DO NOTHING`,
        [job.jobId]
      );

      /* ---- Kafka ACK ---- */
      await consumer.commitOffsets([
        {
          topic,
          partition,
          offset: (Number(message.offset) + 1).toString(),
        },
      ]);

      /* ---- Scheduler queue ---- */
      schedulerState.waiting.push(job.jobId);
      limit.concurrency = getLimitValue();

      printSchedulerTable(`Job ${job.jobId} received`);

      limit(async () => {
        /* assign slot */
        const slot =
          [...Array(limit.concurrency).keys()]
            .map(i => i + 1)
            .find(i => !schedulerState.running.has(i));

        schedulerState.waiting =
          schedulerState.waiting.filter(j => j !== job.jobId);

        schedulerState.running.set(slot, {
          jobId: job.jobId,
          duration: (job.processingTime || 60000) / 1000,
        });

        printSchedulerTable(`Slot ${slot} allocated to ${job.jobId}`);

        await processFile(job);

        schedulerState.running.delete(slot);
        schedulerState.completed.push(job.jobId);

        printSchedulerTable(`Job ${job.jobId} completed`);
      });
    },
  });
})();

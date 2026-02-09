const express = require('express');
const multer = require('multer');
const { Kafka } = require('kafkajs');
const { v4: uuidv4 } = require('uuid');
const db = require('../db');

const app = express();
const PORT = 3001;

/* --------------------------------------------------
   Logger
--------------------------------------------------- */
function consoleLog(payload) {
  console.log(
    '\n' +
      JSON.stringify({
        service: 'upload-api',
        port: PORT,
        timestamp: new Date().toISOString(),
        ...payload,
      }) +
      '\n'
  );
}

/* --------------------------------------------------
   Multer (memory only)
--------------------------------------------------- */
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    files: 1,
    fileSize: 10 * 1024 * 1024, // 10MB
  },
});

/* --------------------------------------------------
   Kafka Producer
--------------------------------------------------- */
const kafka = new Kafka({
  clientId: 'file-upload-api',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

/* --------------------------------------------------
   Upload Route
--------------------------------------------------- */
app.post('/upload', upload.single('file'), async (req, res) => {
  if (!req.file) {
    consoleLog({ stage: 'REQUEST_FAILED', message: 'No file received' });
    return res.status(400).json({ error: 'No file uploaded' });
  }
  console.log('job id: ', req.query.jobId)
  const jobId = req.query.jobId
  const attachmentId = uuidv4();

  /* --------------------------------------------------
     VARIABLE PROCESSING TIME (for validation)
     Example: /upload?duration=600000
  --------------------------------------------------- */
  const processingTime =
    Number(req.query.duration) || 60000; // default 1 min

  try {
    /* --------------------------------------------------
       1️⃣ Persist JOB first (source of truth)
    --------------------------------------------------- */
    await db.query(
      `INSERT INTO jobs (job_id, status, created_at)
       VALUES ($1, 'QUEUED', NOW())`,
      [jobId]
    );

    /* --------------------------------------------------
       2️⃣ Persist ATTACHMENT metadata
    --------------------------------------------------- */
    await db.query(
      `INSERT INTO attachments
       (attachment_id, job_id, file_name, file_size, mime_type, created_at)
       VALUES ($1, $2, $3, $4, $5, NOW())`,
      [
        attachmentId,
        jobId,
        req.file.originalname,
        req.file.size,
        req.file.mimetype,
      ]
    );

    /* --------------------------------------------------
       3️⃣ Send message to Kafka
    --------------------------------------------------- */
    const kafkaPayload = {
      jobId,
      attachmentId,
      fileName: req.file.originalname,
      size: req.file.size,
      buffer: req.file.buffer.toString('base64'),
      processingTime, // 👈 IMPORTANT
      createdAt: new Date().toISOString(),
    };

    const ack = await producer.send({
      topic: 'file-upload-queue',
      messages: [
        {
          key: jobId, // ensures partition affinity
          value: JSON.stringify(kafkaPayload),
        },
      ],
    });

    consoleLog({
      stage: 'KAFKA_PRODUCED',
      jobId,
      attachmentId,
      partition: ack[0].partition,
      offset: ack[0].baseOffset,
      processingTimeMs: processingTime,
    });

    /* --------------------------------------------------
       4️⃣ Respond immediately
    --------------------------------------------------- */
    return res.status(202).json({
      status: 'QUEUED',
      jobId,
      attachmentId,
      processingTimeMs: processingTime,
      message: 'File accepted and queued for processing',
    });

  } catch (error) {
    consoleLog({
      stage: 'UPLOAD_FAILED',
      jobId,
      error: error.message,
    });

    return res.status(500).json({
      error: 'Failed to queue upload',
    });
  }
});

/* --------------------------------------------------
   Start Server
--------------------------------------------------- */
(async () => {
  await producer.connect();

  app.listen(PORT, () => {
    consoleLog({
      stage: 'SERVICE_STARTED',
      message: 'Upload API started',
    });
  });
})();

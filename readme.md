### 1. The "Big Three" Concepts Explained

#### **A. Partitions (The Parallelism Key)**

* **What it is:** A Topic is a folder; a Partition is a single file inside that folder.
* **Senior Insight:** One partition can only be read by **one** consumer in a group.
* **Example:** If you have 1 Topic with 1 Partition, but you have 10 Worker Servers, **9 servers will do nothing.** To use all 10 servers, you must have at least 10 partitions.
* **The Rule:** Partitions = Maximum possible parallel workers.

#### **B. Heartbeat & Session Timeout (The "Liveness" Contract)**

* **Heartbeat:** The worker saying "I'm still here!"
* **Session Timeout:** Kafka saying "If I don't hear a heartbeat for X seconds, I assume the worker died and I will give its jobs to someone else."
* **The Conflict:** If your file upload takes **60 seconds**, but your `sessionTimeout` is **30 seconds**, Kafka will think the worker is dead *while it is still uploading*. It will trigger a "Rebalance," stopping your work.

#### **C. Acks (The "Safety" Receipt)**

* **`acks: 0`**: Fire and forget. No guarantee.
* **`acks: 1`**: Leader writes to disk and confirms. (Risky if leader crashes).
* **`acks: -1` (or `all`)**: Leader + all followers write to disk. This is the **Production Standard**.

---

### 2. Senior-Level Producer Configuration

This configuration ensures **Idempotency** (no duplicate messages) and **Durability** (no lost messages).

```javascript
const { Kafka, CompressionTypes } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'sophia-producer-prod',
  brokers: ['localhost:9092'],
  retry: {
    initialRetryTime: 100,
    retries: 10 // Senior tip: Be persistent with network blips
  }
});

const producer = kafka.producer({
  // 1. Idempotent: Ensures that even if network retries happen, 
  // Kafka won't save the same file metadata twice.
  idempotent: true,
  
  // 2. Max In-Flight: How many messages can be sent without waiting for ACK. 
  // Set to 5 for high performance without losing order.
  maxInFlightRequests: 5,
  
  // 3. Transaction Timeout: Max time to wait for a transaction to finish.
  transactionTimeout: 30000
});

async function sendToQueue(payload) {
  await producer.send({
    topic: 'file-upload-queue',
    // 4. ACKS -1: Wait for ALL brokers to confirm receipt.
    acks: -1, 
    // 5. Compression: Reduces network load significantly for large JSONs.
    compression: CompressionTypes.GZIP,
    messages: [
      { 
        key: payload.jobId, // Use jobId as key to keep all related file parts in same partition
        value: JSON.stringify(payload) 
      }
    ],
  });
}

```

---

### 3. Senior-Level Consumer Configuration

This handles your **1-minute blocking job** without crashing the group.

```javascript
const consumer = kafka.consumer({
  groupId: 'sophia-worker-group',
  
  // 1. Session Timeout: Must be > than your longest job (60s).
  // We set 120s (2 min) so we have a buffer.
  sessionTimeout: 120000, 
  
  // 2. Heartbeat: Should be 1/3 of the session timeout.
  // KafkaJS sends this in the background, but the worker must not be "blocked".
  heartbeatInterval: 3000, 
  
  // 3. Rebalance Timeout: How long to wait for other members to join.
  rebalanceTimeout: 60000, 
  
  // 4. Max Bytes: Don't pull too much data at once into memory.
  maxBytesPerPartition: 1048576 (1MB)
});

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'file-upload-queue', fromBeginning: false });

  await consumer.run({
    // 5. Auto Commit False: DON'T tell Kafka we are done until the 1-min job finishes.
    // This provides "At Least Once" delivery.
    autoCommit: false, 

    eachMessage: async ({ topic, partition, message, heartbeat }) => {
      const job = JSON.parse(message.value.toString());

      try {
        console.log(`Processing ${job.jobId}...`);

        // 6. Heartbeat Manual: During a long 1-min job, call heartbeat()
        // to tell Kafka "I'm still working, don't kick me out!"
        const hbInterval = setInterval(async () => {
           await heartbeat();
        }, 5000);

        // --- THE 1 MINUTE JOB ---
        await performHeavyUpload(job); 

        clearInterval(hbInterval);

        // 7. Manual Commit: NOW we tell Kafka "Job Done, move the offset."
        await consumer.commitOffsets([
          { topic, partition, offset: (parseInt(message.offset) + 1).toString() }
        ]);

      } catch (err) {
        // If it fails, we DON'T commit. The job will be retried.
        console.error("Critical Job Failure", err);
      }
    },
  });
};

```

---

### 4. Comparison: Default vs. Senior Production

| Parameter | Default Setting | Senior Production Setting | Impact |
| --- | --- | --- | --- |
| **`acks`** | `1` | `-1` (all) | Prevents data loss if a broker dies. |
| **`autoCommit`** | `true` | `false` | Prevents "losing" a job if the server crashes mid-upload. |
| **`idempotent`** | `false` | `true` | Prevents duplicate files if the network retries a send. |
| **`sessionTimeout`** | `30000` (30s) | `120000` (120s) | Allows 60s jobs to finish without Kafka "panicking". |
| **`compression`** | None | `GZIP` | Saves 30-50% on cloud network costs (egress fees). |

import { Kafka } from "kafkajs";
import config from "../../config/config.js";
import jobService from "../services/job.service.js";
import scheduler from "../services/scheduler.service.js";
import logger from "../utils/logger.js";

const kafka = new Kafka({
  clientId: config.kafka.clientId,
  brokers: config.kafka.brokers,
  retry: config.kafka.retry,
  logLevel: config.kafka.logLevel,
  logCreator:
    () =>
    ({ namespace, level, label, log }) => {
      const { message, ...extra } = log;
      logger.info(`[Kafka] ${label} ${message} ${JSON.stringify(extra)}`);
    },
});

const consumer = kafka.consumer({
  groupId: config.kafka.groupId,
  sessionTimeout: config.kafka.sessionTimeout,
  heartbeatInterval: config.kafka.heartbeatInterval,
});

async function startConsumer() {
  await consumer.connect();

  await consumer.subscribe({
    topic: config.kafka.topic,
    fromBeginning: false,
  });

  logger.info("Kafka Consumer started");

  await consumer.run({
    autoCommit: false,

    eachMessage: async ({ topic, partition, message }) => {
      const job = JSON.parse(message.value.toString());

      logger.info(`Received job ${job.jobId}`);

      try {
        const result = await scheduler.run(
          () => jobService.processJob(job),
          job.jobId,
        );

        if (result.success) {
          await consumer.commitOffsets([
            {
              topic,
              partition,
              offset: (Number(message.offset) + 1).toString(),
            },
          ]);

          logger.info(`Offset committed for job ${job.jobId}`);
        } else {
          throw new Error(result.error);
        }
      } catch (error) {
        logger.error(`Job ${job.jobId} failed in consumer`, error);
      }
    },
  });
}

async function stopConsumer() {
  await consumer.disconnect();
  logger.info("Kafka Consumer stopped");
}

export default {
  startConsumer,
  stopConsumer,
};

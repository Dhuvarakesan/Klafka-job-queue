import app from "./app/app.js";
import db from "./app/database/db.js";
import consumer from "./app/kafka/consumer.js";
import logger from "./app/utils/logger.js";
import config from "./config/config.js";

async function start() {
  try {
    // Start Kafka consumer
    await consumer.startConsumer();

    // Start HTTP server (monitoring only)
    app.listen(config.app.port, () => {
      logger.info(
        `Server running on port ${config.app.port}`
      );
    });

  } catch (error) {
    logger.error("Failed to start service", error);
    process.exit(1);
  }
}

// Graceful shutdown
async function shutdown() {
  logger.info("Shutting down service...");

  try {
    await consumer.stopConsumer();
    await db.closePool();
  } catch (error) {
    logger.error("Shutdown error", error);
  }

  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);

start();

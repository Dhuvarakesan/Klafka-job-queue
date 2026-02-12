import db from "../database/db.js";
import logger from "../utils/logger.js"; // Assuming a logger utility exists

/**
 * Mark job as PROCESSING
 */
async function markProcessing(jobId) {
  try {
    return await db.withClient(async (client) => {
      return client.query(
        `
        UPDATE jobs
        SET status = 'PROCESSING',
            started_at = NOW()
        WHERE id = $1
        `,
        [jobId]
      );
    });
  } catch (error) {
    logger.error(`Error marking job ${jobId} as PROCESSING: ${error.message}`);
    throw error;
  }
}

/**
 * Mark job as COMPLETED
 */
async function markCompleted(jobId) {
  try {
    return await db.withClient(async (client) => {
      return client.query(
        `
        UPDATE jobs
        SET status = 'COMPLETED',
            completed_at = NOW()
        WHERE id = $1
        `,
        [jobId]
      );
    });
  } catch (error) {
    logger.error(`Error marking job ${jobId} as COMPLETED: ${error.message}`);
    throw error;
  }
}

/**
 * Mark job as FAILED
 */
async function markFailed(jobId, errorMessage) {
  try {
    return await db.withClient(async (client) => {
      return client.query(
        `
        UPDATE jobs
        SET status = 'FAILED',
            error_message = $2,
            completed_at = NOW()
        WHERE id = $1
        `,
        [jobId, errorMessage]
      );
    });
  } catch (error) {
    logger.error(`Error marking job ${jobId} as FAILED: ${error.message}`);
    throw error;
  }
}

export default {
  markProcessing,
  markCompleted,
  markFailed,
};
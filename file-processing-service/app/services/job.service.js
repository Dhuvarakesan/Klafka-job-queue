import jobModel from "../models/job.model.js";
import { updateStatus } from "../models/upload.model.js";
import logger from "../utils/logger.js";
import { processExternalJob } from "./externalApi.service.js";

async function processJob(job) {
  const { jobId } = job;
  logger.info(`Job ${jobId} → Starting processing`);
  try {
    // Update status to PROCESSING
    await jobModel.markProcessing(jobId);
    await updateStatus(jobId, "PROCESSING");

    // Call external API

    const externalResult = await processExternalJob(job);

    // Mark COMPLETED
    await jobModel.markCompleted(jobId);
    await updateStatus(jobId, "COMPLETED");

    logger.info(`Job ${jobId} → Completed successfully`);

    return { success: true, data: externalResult };
  } catch (error) {
    logger.error(`Job ${jobId} → Failed`, error);

    // Mark FAILED
    await jobModel.markFailed(jobId, error.message);
    await updateStatus(jobId, "FAILED");

    return { success: false, error: error.message };
  }
}

export default {
  processJob,
};

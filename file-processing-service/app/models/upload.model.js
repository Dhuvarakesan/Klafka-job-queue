import db from "../database/db.js";
import logger from "../utils/logger.js";

/**
 * Update the status of an upload
 */
export async function updateStatus(jobId, status) {
  try {
    return await db.withClient(async (client) => {
      return client.query(
        `
        UPDATE uploads
        SET status = $2,
            modified_on = NOW()
        WHERE job_id = $1
        `,
        [jobId, status]
      );
    });
  } catch (error) {
    logger.error(`Error updating status for job ${jobId}: ${error.message}`);
    throw error;
  }
}

/**
 * Update the neo4j_path of an upload
 */
export async function updateNeo4jPath(jobId, neo4jPath) {
  try {
    return await db.withClient(async (client) => {
      return client.query(
        `
        UPDATE uploads
        SET neo4j_path = $2,
            modified_on = NOW()
        WHERE job_id = $1
        `,
        [jobId, neo4jPath]
      );
    });
  } catch (error) {
    logger.error(`Error updating neo4j_path for job ${jobId}: ${error.message}`);
    throw error;
  }
}

/**
 * Update the faiss_path of an upload
 */
export async function updateFaissPath(jobId, faissPath) {
  try {
    return await db.withClient(async (client) => {
      return client.query(
        `
        UPDATE uploads
        SET faiss_path = $2,
            modified_on = NOW()
        WHERE job_id = $1
        `,
        [jobId, faissPath]
      );
    });
  } catch (error) {
    logger.error(`Error updating faiss_path for job ${jobId}: ${error.message}`);
    throw error;
  }
}

/**
 * Get upload details by jobId
 */
export async function getUploadByJobId(jobId) {
  try {
    return await db.withClient(async (client) => {
      const result = await client.query(
        `
        SELECT *
        FROM uploads
        WHERE job_id = $1
        `,
        [jobId]
      );
      return result.rows[0];
    });
  } catch (error) {
    logger.error(`Error fetching upload for job ${jobId}: ${error.message}`);
    throw error;
  }
}


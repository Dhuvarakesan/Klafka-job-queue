import pkg from "pg";
import config from "../../config/config.js";
import logger from "../utils/logger.js";

const { Pool, Client } = pkg;

/* ------------------------------------
   Create Pool (recommended for app)
------------------------------------- */
const pool = new Pool({
  user: config.database.user,
  host: config.database.host,
  database: config.database.name,
  password: config.database.password,
  port: config.database.port,
  ssl: config.database.ssl
    ? { rejectUnauthorized: false }
    : false,
});

/* ------------------------------------
   Pool Event Listeners
------------------------------------- */
pool.on("connect", () => {
  logger.info("PostgreSQL connected");
});

pool.on("error", (err) => {
  logger.error("PostgreSQL Pool Error", err);
});

/* ------------------------------------
   Safe Client Usage (Transactional / Scoped)
------------------------------------- */
export async function withClient(callback) {
  const client = await pool.connect();

  const enableNotices = process.env.DB_DEBUG_NOTICES === "true";

  const noticeListener = (msg) => {
    logger.debug("PG NOTICE", msg.message);
  };

  if (enableNotices) {
    client.on("notice", noticeListener);
  }

  try {
    const schema = process.env.DB_SCHEMA || "public";
    await client.query(`SET search_path TO ${schema};`);

    return await callback(client);

  } catch (error) {
    logger.error("Database operation failed", error);
    throw error;

  } finally {
    if (enableNotices) {
      client.removeListener("notice", noticeListener);
    }

    client.release();
  }
}

/* ------------------------------------
   Direct Query (for simple cases)
------------------------------------- */
export async function query(text, params) {
  return pool.query(text, params);
}

/* ------------------------------------
   Standalone Client (rarely needed)
------------------------------------- */
export function createClient() {
  return new Client({
    user: config.database.user,
    host: config.database.host,
    database: config.database.name,
    password: config.database.password,
    port: config.database.port,
    ssl: config.database.ssl
      ? { rejectUnauthorized: false }
      : false,
  });
}

/* ------------------------------------
   Graceful Shutdown
------------------------------------- */
export async function closePool() {
  await pool.end();
  logger.info("PostgreSQL pool closed");
}

export default {
  query,
  withClient,
  createClient,
  closePool,
};

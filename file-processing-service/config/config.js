import env from "dotenv";
env.config();
function required(key) {
  const value = process.env[key];
  if (!value) {
    throw new Error(`❌ Missing required environment variable: ${key}`);
  }
  return value;
}

const config = {
  app: {
    port: Number(process.env.PORT || 3000),
    env: process.env.NODE_ENV || "development",
  },

  kafka: {
    clientId: process.env.KAFKA_CLIENT_ID || "file-processing-service",
    brokers: required("KAFKA_BROKERS").split(","),
    topic: process.env.KAFKA_TOPIC || "file-upload-queue",
    groupId: process.env.KAFKA_GROUP_ID || "file-processing-group",

    sessionTimeout: Number(process.env.KAFKA_SESSION_TIMEOUT || 30000),
    heartbeatInterval: Number(process.env.KAFKA_HEARTBEAT_INTERVAL || 3000),

    retry: {
      retries: Number(process.env.KAFKA_RETRY_COUNT || 5),
      initialRetryTime: Number(process.env.KAFKA_RETRY_INITIAL_DELAY || 300),
      maxRetryTime: Number(process.env.KAFKA_RETRY_MAX_DELAY || 30000),
    },
  },

  scheduler: {
    parallelEnabled: process.env.PARALLEL_ENABLED === "true",
    maxConcurrency: Number(process.env.MAX_CONCURRENCY || 1),
  },

  externalApi: {
    url: required("EXTERNAL_API_URL"),
    timeout: Number(process.env.EXTERNAL_API_TIMEOUT || 10000),
    rejectUnauthorized: process.env.EXTERNAL_API_REJECT_UNAUTHORIZED !== "false",
  },
  uploadService: {
    url: required("UPLOAD_SERVICE_URL"),
    timeout: Number(process.env.EXTERNAL_API_TIMEOUT || 10000),
    rejectUnauthorized: process.env.EXTERNAL_API_REJECT_UNAUTHORIZED !== "false",

  },
  
  database: {
    user: required("DB_USER"),
    host: required("DB_HOST"),
    name: required("DB_NAME"),
    password: required("DB_PASSWORD"),
    port: Number(process.env.DB_PORT || 5432),
    ssl: process.env.DB_SSL === "true",
  },
  logging: {
  level: process.env.LOG_LEVEL || "INFO",
  output: process.env.LOG_OUTPUT || "console",
  directory: process.env.LOG_DIRECTORY || "./logs",
},
};

export default config;

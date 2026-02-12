import fs from "fs";
import path from "path";
import util from "util";
import config from "../../config/config.js";
import { ensureWritableDirectory } from "./fileAccess.util.js";

let logger;

if (global.__loggerInitialized) {
  logger = global.__loggerInstance;
} else {
  try {
    const LOG_LEVELS = {
      DEBUG: 0,
      INFO: 1,
      WARN: 2,
      ERROR: 3,
    };

    const currentLevelName = config.logging.level || "INFO";
    let currentLogLevel =
      LOG_LEVELS[currentLevelName] ?? LOG_LEVELS.INFO;

    let effectiveLogOutput = config.logging.output || "console";
    const logDirectory = config.logging.directory || "./logs";

    let logFilePath = null;

    /* ------------------------------------
       FILE LOGGING INITIALIZATION
    ------------------------------------- */
    if (effectiveLogOutput === "file" || effectiveLogOutput === "both") {
      const result = ensureWritableDirectory(logDirectory);

      if (!result.success) {
        console.error("⚠ File logging disabled.");
        console.error("Reason:", result.error?.message);
        console.error("Falling back to console logging.");

        effectiveLogOutput = "console";
      } else {
        try {
          const now = new Date();
          const fileName =
            now.toISOString().replace(/:/g, "-").replace(/\..+/, "") +
            ".log";

          logFilePath = path.join(result.directory, fileName);

          // Create empty file to verify write capability
          fs.writeFileSync(logFilePath, "");

          console.log(`📁 Logging to file: ${logFilePath}`);

        } catch (fileError) {
          console.error("⚠ Unable to create log file.");
          console.error("Reason:", fileError.message);
          console.error("Falling back to console logging.");

          effectiveLogOutput = "console";
        }
      }
    }

    /* ------------------------------------
       FORMAT FUNCTION
    ------------------------------------- */
    function formatData(data) {
      if (!data) return "";
      if (typeof data === "string") return data;

      if (data instanceof Error) {
        return `${data.message}\n${data.stack}`;
      }

      return util.inspect(data, {
        depth: 4,
        colors: false,
        maxArrayLength: 10,
        breakLength: 120,
      });
    }

    /* ------------------------------------
       WRITE TO FILE
    ------------------------------------- */
    function writeToFile(message) {
      try {
        fs.appendFileSync(logFilePath, message);
      } catch (err) {
        console.error("⚠ Failed to write log file. Switching to console.");
        console.error("Reason:", err.message);
        effectiveLogOutput = "console";
      }
    }

    /* ------------------------------------
       CORE LOG FUNCTION
    ------------------------------------- */
    function log(level, message, data) {
      if (LOG_LEVELS[level] < currentLogLevel) return;

      const timestamp = new Date().toISOString();
      const formattedData = data ? "\n" + formatData(data) : "";
      const logMessage =
        `[${timestamp}] [${level}] ${message}${formattedData}\n`;

      // File logging
      if (
        effectiveLogOutput === "file" ||
        effectiveLogOutput === "both"
      ) {
        writeToFile(logMessage);
      }

      // Console logging
      if (
        effectiveLogOutput === "console" ||
        effectiveLogOutput === "both"
      ) {
        const consoleMsg =
          `[${timestamp}] ${getColor(level)}[${level}]\x1b[0m ${message}`;

        if (level === "ERROR") {
          console.error(consoleMsg, data ? "\n" + formatData(data) : "");
        } else {
          console.log(consoleMsg, data ? "\n" + formatData(data) : "");
        }
      }
    }

    function getColor(level) {
      switch (level) {
        case "DEBUG": return "\x1b[90m";
        case "INFO": return "\x1b[32m";
        case "WARN": return "\x1b[33m";
        case "ERROR": return "\x1b[31m";
        default: return "\x1b[0m";
      }
    }

    logger = {
      debug: (msg, data) => log("DEBUG", msg, data),
      info: (msg, data) => log("INFO", msg, data),
      warn: (msg, data) => log("WARN", msg, data),
      error: (msg, data) => log("ERROR", msg, data),

      setLevel: (level) => {
        if (LOG_LEVELS[level] === undefined) return;
        currentLogLevel = LOG_LEVELS[level];
      },
    };

    global.__loggerInitialized = true;
    global.__loggerInstance = logger;

  } catch (err) {
    console.error("Logger initialization failed:", err);
  }
}

export default logger;

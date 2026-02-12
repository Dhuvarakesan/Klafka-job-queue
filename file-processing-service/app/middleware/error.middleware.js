import config from "../../config/config.js";
import logger from "../utils/logger.js";

/**
 * Global Error Handler Middleware
 */
function errorMiddleware(err, req, res, next) {
  const statusCode = err.statusCode || 500;

  // Log full error internally
  logger.error("Unhandled Application Error", {
    message: err.message,
    stack: err.stack,
    path: req.originalUrl,
    method: req.method,
  });

  // Response payload
  const response = {
    status: "FAILURE",
    message: err.publicMessage || "Internal Server Error",
  };

  // Only expose stack in development
  if (config.app.env === "development") {
    response.debug = {
      message: err.message,
      stack: err.stack,
    };
  }

  res.status(statusCode).json(response);
}

export default errorMiddleware;

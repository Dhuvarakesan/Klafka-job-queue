import pLimit from "p-limit";
import config from "../../config/config.js";
import logger from "../utils/logger.js";

const parallelEnabled = config.scheduler.parallelEnabled;
const maxConcurrency = config.scheduler.maxConcurrency;

const limit = parallelEnabled ? pLimit(maxConcurrency) : null;

const MAX_HISTORY = 50;

// In-memory state
const state = {
  running: new Map(),   // slot -> jobId
  waiting: [],
  completed: [],
};

/**
 * Get free slot number
 */
function getFreeSlot() {
  for (let i = 1; i <= maxConcurrency; i++) {
    if (!state.running.has(i)) {
      return i;
    }
  }
  return null;
}

/**
 * Execute a job through scheduler
 */
async function run(task, jobId) {
  if (!parallelEnabled) {
    logger.info(`Running sequential job ${jobId}`);
    return execute(task, jobId, 1);
  }

  state.waiting.push(jobId);
  logger.info(`Job queued: ${jobId}`);

  return limit(async () => {
    const slot = getFreeSlot();

    // remove from waiting
    state.waiting = state.waiting.filter(j => j !== jobId);

    state.running.set(slot, jobId);
    logger.info(`Slot ${slot} assigned to job ${jobId}`);

    try {
      const result = await task();

      state.completed.push(jobId);
      trimHistory();

      logger.info(`Job completed: ${jobId}`);
      return result;

    } catch (err) {
      logger.error(`Job failed: ${jobId}`, err);
      throw err;

    } finally {
      state.running.delete(slot);
    }
  });
}

/**
 * Sequential execution helper
 */
async function execute(task, jobId, slot) {
  try {
    state.running.set(slot, jobId);
    const result = await task();
    state.completed.push(jobId);
    trimHistory();
    return result;
  } finally {
    state.running.delete(slot);
  }
}

/**
 * Limit completed history
 */
function trimHistory() {
  if (state.completed.length > MAX_HISTORY) {
    state.completed.shift();
  }
}

/**
 * Monitoring getters
 */
function getState() {
  return {
    concurrency: parallelEnabled ? maxConcurrency : 1,
    running: Array.from(state.running.values()),
    waiting: state.waiting,
    completed: state.completed,
  };
}

export default {
  run,
  getState,
};

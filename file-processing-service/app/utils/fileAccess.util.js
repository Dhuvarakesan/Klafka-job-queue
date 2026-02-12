import fs from "fs";
import path from "path";

/**
 * Ensures directory exists and is writable.
 * Never throws. Always returns structured result.
 *
 * @param {string} directoryPath
 * @returns {{ success: boolean, directory?: string, error?: Error }}
 */
export function ensureWritableDirectory(directoryPath) {
  try {
    const resolvedPath = path.resolve(directoryPath);

    // Step 1: Attempt to create directory if missing
    try {
      if (!fs.existsSync(resolvedPath)) {
        fs.mkdirSync(resolvedPath, { recursive: true });
      }
    } catch (mkdirError) {
      return { success: false, error: mkdirError };
    }

    // Step 2: Check write permission
    try {
      fs.accessSync(resolvedPath, fs.constants.W_OK);
    } catch (accessError) {
      return { success: false, error: accessError };
    }

    return { success: true, directory: resolvedPath };

  } catch (error) {
    return { success: false, error };
  }
}

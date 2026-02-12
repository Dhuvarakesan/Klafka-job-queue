import FormData from "form-data";
import { v4 as uuidv4 } from "uuid";
import config from "../../config/config.js";
import { updateFaissPath } from "../models/upload.model.js";
import { axiosInstance } from "../utils/axios.instance.js";
import logger from "../utils/logger.js";

function normalizeBoolean(value) {
  return value === true || value === "true";
}

async function getFileStreamFromUrl(fileUrl) {
  if (!fileUrl) {
    throw new Error("file_url is required");
  }

  try {
    const response = await axiosInstance.get(fileUrl, {
      responseType: "stream",
    });

    return response.data;
  } catch (error) {
    // Use JSON.stringify to see the actual error body from the API
    const details = error.response?.data
      ? JSON.stringify(error.response.data)
      : error.message;

    throw new Error(`Failed to fetch file from URL: ${details}`);
  }
}


export async function processExternalJob(job) {
  const {
    jobId,
    metadata: { filename, file_path, nodes, isTestcase, process_image },
  } = job;
  const file_url = config.uploadService.url + "/" + file_path;
  const isTest = normalizeBoolean(isTestcase);

  try {
    const fileStream = await getFileStreamFromUrl(file_url);
    let response;

    if (isTest) {
      logger.info(`Job ${jobId} → Calling /get_sheet_list`);

      const form = new FormData();
      form.append("file", fileStream);
      form.append("domain_id", nodes);
      const res = await axiosInstance
        .post(config.externalApi.url)
        .then((res) => {
          console.log("API response received:", res.data);
        })
        .catch((error) => {
          console.log(
            "Error in API call:",
            error.response?.data || error.message,
          );
        });

      console.log("res:", res);
      response = await axiosInstance
        .post(config.externalApi.url + "/get_sheet_list", form, form, {
          headers: form.getHeaders(),
          timeout: 120000, // 2 minutes
        })
        .then(async (res) => {
          if (res) {
            const externalAPIResponse = res.data;
            const neo4jPath = externalAPIResponse?.root_id || "";
            if (!neo4jPath) {
              logger.warn(
                `Job ${jobId} → No root_id in /get_sheet_list response`,
              );
            }

            // Update the neo4j_path in the database using the upload model
            await uploadModel.updateNeo4jPath(jobId, neo4jPath);

            logger.info(`Job ${jobId} → neo4j_path updated successfully`);
          }
        })
        .catch((error) => {
          const apiError =
            error.response?.data ||
            error.message ||
            "Unknown error during /get_sheet_list API call";
          logger.error(`Error in /get_sheet_list API call:`, apiError);
          throw new Error(
            typeof apiError === "string" ? apiError : JSON.stringify(apiError),
          );
        });

      logger.info(`Job ${jobId} → Excel API success`);

      return {
        type: "TESTCASE",
        rootId: response.data?.root_id || null,
        data: response.data,
      };
    } else {
      logger.info(`Job ${jobId} → Calling /supporting_document_processing`);

      const form = new FormData();

      form.append("file", fileStream, {
        filename,
        contentType: "text/plain",
      });

      form.append("domain_id", nodes);
      form.append("process_image", process_image);
      form.append("file_uuid", uuidv4());

      response = await axiosInstance.post(
        config.externalApi.url + "/supporting_document_processing",
        form,
        {
          headers: {
            ...form.getHeaders(),
          },
          maxContentLength: Infinity,
          maxBodyLength: Infinity,
        },
      );

      logger.info("External API response received.");
      logger.info(`Response data: ${JSON.stringify(response.data)}`);

      const faissPaths =
        response?.data?.results
          ?.map((item) => item.FAISS_PATH)
          .filter(Boolean) || [];

      logger.info(`Job ${jobId} → Support document API success`);

      await updateFaissPath(jobId, faissPaths); 
      return {
        type: "SUPPORTING_DOC",
        faissPaths,
        data: response.data,
      };
    }
  } catch (error) {
    console.log("Error caught in processExternalJob:", error);
    const apiError =
      error.response?.data || error.message || "Unknown external API error";

    logger.error(`Job ${jobId} → External API failed`, apiError);

    throw new Error(
      typeof apiError === "string" ? apiError : JSON.stringify(apiError),
    );
  }
}


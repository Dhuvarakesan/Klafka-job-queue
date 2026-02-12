// import axios from "axios";
// import https from "https";
// import config from "../../config/config.js";
// import logger from "./logger.js";

// let httpsAgent;

// if (config.externalApi.rejectUnauthorized === false) {
//   logger.warn("⚠ SSL verification disabled for External API");

//   httpsAgent = new https.Agent({
//     rejectUnauthorized: false,
//   });
// }

// const instance = axios.create({
//   timeout: config.externalApi.timeout,
//   httpsAgent,
// });

// /* ---------------------------
//    REQUEST INTERCEPTOR
// ---------------------------- */
// instance.interceptors.request.use(
//   (req) => {
//     logger.debug("External API Request", {
//       method: req.method,
//       url: req.baseURL + req.url,
//     });
//     return req;
//   },
//   (error) => Promise.reject(error)
// );

// /* ---------------------------
//    RESPONSE INTERCEPTOR
// ---------------------------- */
// instance.interceptors.response.use(
//   (res) => {
//     logger.debug("External API Response", {
//       status: res.status,
//       url: res.config.url,
//     });
//     return res;
//   },
//   (error) => {
//     if (error.code === "ECONNABORTED") {
//       logger.error("External API Timeout", error.message);
//     } else {
//       logger.error("External API Error", {
//         message: error.message,
//         url: error.config?.url,
//         status: error.response?.status,
//       });
//     }

//     return Promise.reject(error);
//   }
// );

// export default instance;

import axios from "axios";
import https from "https";

let axiosInstance;

try {
    console.info('Creating Axios instance with custom HTTPS agent');
    const httpsAgent = new https.Agent({
        rejectUnauthorized: false, // (NOTE: this will disable client verification)
    });

    axiosInstance = axios.create({ httpsAgent });
    console.info('Axios instance created successfully');
} catch (error) {
    console.error('Error creating Axios instance:', (error).message);
    throw error;
}

export { axiosInstance };

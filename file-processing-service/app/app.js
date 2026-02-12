import express from "express";
// import errorMiddleware from "./middleware/error.middleware.js";
// import monitorRoutes from "./routes/monitor.routes.js";
const app = express();


// app.use(errorMiddleware);
// Basic middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Health check
app.get("/health", (req, res) => {
  res.status(200).json({
    status: "OK",
    service: "file-processing-service",
  });
});

// Monitoring routes
// app.use("/monitor", monitorRoutes);



export default app;

import express, { type Request, Response, NextFunction } from "express";
import { registerRoutes } from "./routes";
import { setupVite, serveStatic, log } from "./vite";
import { startDataUpdateService } from "./services/dataUpdateService";
import { startWindDataUpdateService } from "./services/windDataUpdateService";
import { requestLogger } from "./middleware/requestLogger";
import { errorHandler } from "./middleware/errorHandler";
import { performanceMonitor } from "./middleware/performanceMonitor";
import { logger } from "./utils/logger";
import { runMigration } from "./scripts/data/migrations/windGenerationDataMigration";

// Initialize Express app
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// Apply middleware in the correct order
// 1. Request logging (should be early to capture all requests)
app.use(requestLogger);
// 2. Performance monitoring
app.use(performanceMonitor);

// Legacy middleware for backward compatibility
app.use((req, res, next) => {
  const start = Date.now();
  const path = req.path;
  let capturedJsonResponse: Record<string, any> | undefined = undefined;

  const originalResJson = res.json;
  res.json = function (bodyJson, ...args) {
    capturedJsonResponse = bodyJson;
    return originalResJson.apply(res, [bodyJson, ...args]);
  };

  res.on("finish", () => {
    const duration = Date.now() - start;
    if (path.startsWith("/api")) {
      let logLine = `${req.method} ${path} ${res.statusCode} in ${duration}ms`;
      if (capturedJsonResponse) {
        logLine += ` :: ${JSON.stringify(capturedJsonResponse)}`;
      }

      if (logLine.length > 80) {
        logLine = logLine.slice(0, 79) + "…";
      }

      log(logLine);
    }
  });

  next();
});

let dataUpdateServiceStarted = false;
let windDataServiceStarted = false;
let server: any;

const startServer = async () => {
  try {
    server = registerRoutes(app);

    // Add health check endpoint
    app.get('/health', (req, res) => {
      res.status(200).json({ status: 'ok', startTime: new Date().toISOString() });
    });

    // Use our standardized error handler middleware
    app.use(errorHandler);

    if (app.get("env") === "development") {
      await setupVite(app, server);
    } else {
      serveStatic(app);
    }

    const PORT = process.env.PORT || 5000;

    // Start server first, explicitly binding to 0.0.0.0
    await new Promise<void>((resolve) => {
      server.listen(PORT, "0.0.0.0", () => {
        log(`Server started on port ${PORT}`);
        log(`Environment: ${app.get("env")}`);
        resolve();
      });
    });

    // Run wind data migration to ensure the database table exists
    try {
      console.log("Running wind generation data migration...");
      await runMigration();
      console.log("Wind generation data migration completed successfully");
    } catch (error) {
      console.error("Failed to run wind generation data migration:", error);
    }

    // Start the data update service after server is ready
    if (!dataUpdateServiceStarted) {
      try {
        console.log("Initializing data update service...");
        
        // Now properly await the Promise returned by startDataUpdateService
        const updateServiceInterval = await startDataUpdateService();

        // Handle cleanup on server shutdown 
        process.on('SIGTERM', () => {
          console.log('Shutting down data update service...');
          clearInterval(updateServiceInterval);
        });

        dataUpdateServiceStarted = true;
        console.log("Data update service started successfully with interval ID:", updateServiceInterval);
      } catch (error) {
        console.error("Failed to start data update service:", error);
      }
    }
    
    // Start the wind data update service
    if (!windDataServiceStarted) {
      try {
        console.log("Initializing wind data update service...");
        startWindDataUpdateService();
        
        windDataServiceStarted = true;
        console.log("Wind data update service started successfully");
      } catch (error) {
        console.error("Failed to start wind data update service:", error);
      }
    }
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
};

startServer();
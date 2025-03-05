import express, { type Request, Response, NextFunction } from "express";
import { registerRoutes } from "./routes";
import { setupVite, serveStatic, log } from "./vite";
import { startDataUpdateService } from "./services/dataUpdater";

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

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
let server: any;

const startServer = async () => {
  try {
    server = registerRoutes(app);

    // Add health check endpoint
    app.get('/health', (req, res) => {
      res.status(200).json({ status: 'ok', startTime: new Date().toISOString() });
    });

    app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
      const status = err.status || err.statusCode || 500;
      const message = err.message || "Internal Server Error";
      console.error(`Server error: ${message}`, err);
      res.status(status).json({ message });
    });

    if (app.get("env") === "development") {
      await setupVite(app, server);
    } else {
      serveStatic(app);
    }

    const PORT = process.env.PORT || 3000;

    // Start server first, explicitly binding to 0.0.0.0
    await new Promise<void>((resolve) => {
      server.listen(PORT, "0.0.0.0", () => {
        log(`Server started on port ${PORT}`);
        log(`Environment: ${app.get("env")}`);
        resolve();
      });
    });

    // Start the data update service after server is ready
    if (!dataUpdateServiceStarted) {
      try {
        console.log("Initializing data update service...");
        const updateServiceInterval = await startDataUpdateService();

        if (updateServiceInterval) {
          // Handle cleanup on server shutdown
          // Add global uncaught exception handler to prevent termination
          process.on('uncaughtException', (error) => {
            console.error('Uncaught exception:', error);
            // Log but don't exit to prevent crash loops in production
          });

          process.on('unhandledRejection', (reason, promise) => {
            console.error('Unhandled rejection at:', promise, 'reason:', reason);
            // Log but don't exit to prevent crash loops in production
          });

          process.on('SIGTERM', () => {
            console.log('Shutting down data update service...');
            clearInterval(updateServiceInterval);
            process.exit(0);
          });

          process.on('SIGINT', () => {
            console.log('Shutting down data update service...');
            clearInterval(updateServiceInterval);
            process.exit(0);
          });

          dataUpdateServiceStarted = true;
          console.log("Data update service started successfully");
        } else {
          console.warn("Data update service initialization returned no interval - this is unexpected but the service may still be running");
        }
      } catch (error) {
        console.error("Failed to start data update service:", error);
      }
    }
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
};

startServer();
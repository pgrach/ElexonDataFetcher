import express, { type Request, Response, NextFunction } from "express";
import { registerRoutes } from "./routes";
import { setupVite, serveStatic, log } from "./vite";
import { startDataUpdateService } from "./services/dataUpdater";

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// Request logging middleware
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
let updateServiceInterval: NodeJS.Timeout | null = null;

// Function to try binding to a port with enhanced logging and timeout
async function bindPort(server: any, startPort: number, maxAttempts: number = 5, timeout: number = 30000): Promise<number> {
  log(`Attempting to bind server starting from port ${startPort} (max ${maxAttempts} attempts)`);

  for (let port = startPort; port < startPort + maxAttempts; port++) {
    try {
      log(`Attempting to bind to port ${port}...`);
      const boundPort = await new Promise<number | null>((resolve, reject) => {
        const timeoutId = setTimeout(() => {
          server.removeAllListeners();
          log(`Binding attempt to port ${port} timed out after ${timeout}ms`);
          resolve(null);
        }, timeout);

        server.listen(port, "0.0.0.0")
          .once('listening', () => {
            clearTimeout(timeoutId);
            server.removeAllListeners();
            log(`Successfully bound to port ${port}`);
            resolve(port);
          })
          .once('error', (err: NodeJS.ErrnoException) => {
            clearTimeout(timeoutId);
            server.removeAllListeners();
            if (err.code === 'EADDRINUSE') {
              log(`Port ${port} is in use, will try next port`);
              resolve(null);
            } else {
              log(`Error binding to port ${port}: ${err.message}`);
              reject(err);
            }
          });
      });

      if (boundPort !== null) {
        return boundPort;
      }
    } catch (error) {
      log(`Failed to bind to port ${port}: ${error}`);
      if (port === startPort + maxAttempts - 1) {
        throw new Error(`Failed to bind to any port after ${maxAttempts} attempts`);
      }
    }
  }
  throw new Error(`Could not find an available port after ${maxAttempts} attempts`);
}

// Graceful shutdown handler
function setupGracefulShutdown(server: any) {
  const shutdown = () => {
    log('Received shutdown signal');

    // Clear the update service interval if it exists
    if (updateServiceInterval) {
      clearTimeout(updateServiceInterval);
      updateServiceInterval = null;
    }

    server.close(() => {
      log('Server closed');
      process.exit(0);
    });

    // Force exit after 10 seconds
    setTimeout(() => {
      log('Force closing server after timeout');
      process.exit(1);
    }, 10000);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}

(async () => {
  try {
    const server = registerRoutes(app);

    // Start the real-time data update service with error handling
    // Add DISABLE_DATA_SERVICE env var for debugging
    if (!dataUpdateServiceStarted && !process.env.DISABLE_DATA_SERVICE) {
      try {
        log("Initializing data update service...");
        updateServiceInterval = startDataUpdateService();
        dataUpdateServiceStarted = true;
        log("Data update service started successfully");
      } catch (error) {
        log("Failed to start data update service:", error);
        // Continue running the server even if the update service fails
      }
    }

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

    // Try to bind to a port starting from 5000 with enhanced logging
    const boundPort = await bindPort(server, 5000, 5, 30000);
    setupGracefulShutdown(server);

    log(`Server started successfully on port ${boundPort}`);
    log(`Environment: ${app.get("env")}`);
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
})();
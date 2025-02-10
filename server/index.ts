import express, { type Request, Response, NextFunction } from "express";
import { registerRoutes } from "./routes";
import { setupVite, serveStatic, log } from "./vite";
import { startDataUpdateService } from "./services/dataUpdater";
import { createServer } from "net";

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// Function to check if a port is available
function isPortAvailable(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const server = createServer()
      .listen(port, "0.0.0.0", () => {
        server.close(() => resolve(true));
      })
      .on("error", () => resolve(false));
  });
}

// Function to find an available port
async function findAvailablePort(startPort: number): Promise<number> {
  let port = startPort;
  while (!(await isPortAvailable(port))) {
    port++;
    if (port > startPort + 100) {
      throw new Error(`No available ports found between ${startPort} and ${startPort + 100}`);
    }
  }
  return port;
}

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
let currentServer: any = null;

(async () => {
  try {
    // If there's an existing server, close it
    if (currentServer) {
      await new Promise((resolve) => currentServer.close(resolve));
    }

    const DEFAULT_PORT = 5000;
    const port = await findAvailablePort(DEFAULT_PORT);

    const server = registerRoutes(app);
    currentServer = server;

    // Start the real-time data update service with error handling
    if (!dataUpdateServiceStarted) {
      try {
        console.log("Initializing data update service...");
        const updateServiceInterval = startDataUpdateService();

        // Handle cleanup on server shutdown
        process.on('SIGTERM', () => {
          console.log('Shutting down data update service...');
          clearInterval(updateServiceInterval);
          if (currentServer) {
            currentServer.close();
          }
          process.exit(0);
        });

        dataUpdateServiceStarted = true;
        console.log("Data update service started successfully");
      } catch (error) {
        console.error("Failed to start data update service:", error);
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

    server.listen(port, "0.0.0.0", () => {
      log(`Server started on port ${port}`);
      log(`Environment: ${app.get("env")}`);
    });
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
})();
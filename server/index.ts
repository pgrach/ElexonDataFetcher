import express, { type Request, Response, NextFunction } from "express";
import { registerRoutes } from "./routes";
import { setupVite, serveStatic, log } from "./vite";

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// Request logging middleware
app.use((req, res, next) => {
  const start = Date.now();
  log(`${req.method} ${req.path} - Started`);

  res.on("finish", () => {
    const duration = Date.now() - start;
    log(`${req.method} ${req.path} ${res.statusCode} - Completed in ${duration}ms`);
  });

  next();
});

// Global error handler
app.use((err: any, _req: Request, res: Response, _next: NextFunction) => {
  const status = err.status || err.statusCode || 500;
  const message = err.message || "Internal Server Error";
  console.error(`Server error: ${message}`, err);
  res.status(status).json({ message });
});

// Graceful shutdown handler (Retained from original)
function setupGracefulShutdown(server: any) {
  const shutdown = () => {
    log('Received shutdown signal');

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

    log("Setting up server...");

    if (app.get("env") === "development") {
      await setupVite(app, server);
    } else {
      serveStatic(app);
    }

    // Start server
    const port = process.env.PORT || 5000;
    server.listen(port, "0.0.0.0", () => {
      log(`Server started successfully on port ${port}`);
      log(`Environment: ${app.get("env")}`);
      setupGracefulShutdown(server); //Attach graceful shutdown after server starts listening.
    });

  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
})();
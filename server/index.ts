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

// Add robust error handling to prevent termination in production
process.on('uncaughtException', (error) => {
  console.error('Uncaught exception:', error);
  // Log only in production to prevent crash loops
  if (process.env.NODE_ENV !== 'production') {
    process.exit(1);
  }
});

process.on('unhandledRejection', (reason) => {
  console.error('Unhandled rejection:', reason);
  // Log only in production to prevent crash loops
  if (process.env.NODE_ENV !== 'production') {
    process.exit(1);
  }
});

const startServer = async () => {
  try {
    server = registerRoutes(app);

    // Add enhanced health check endpoint
    app.get('/health', (req, res) => {
      res.status(200).json({ 
        status: 'ok', 
        environment: process.env.NODE_ENV || 'development',
        startTime: new Date().toISOString() 
      });
    });
    
    // Add fallback route for AWS connectivity issues in production
    if (process.env.NODE_ENV === 'production') {
      app.get('/api/*', (req, res, next) => {
        try {
          next();
        } catch (error) {
          console.error(`API error for ${req.path}:`, error);
          res.status(503).json({ 
            message: 'Service temporarily unavailable',
            retryAfter: 30
          });
        }
      });
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
      // In production, we need to be more careful with data service startup
      const startDataService = async () => {
        try {
          console.log("Initializing data update service...");
          
          // In production, attempt the data service start but don't let it crash the application
          if (process.env.NODE_ENV === 'production') {
            try {
              const updateServiceInterval = await startDataUpdateService();
              
              if (updateServiceInterval) {
                // Register shutdown handlers
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
                console.warn("Data update service initialization returned no interval - service may be running in another mode");
              }
            } catch (error) {
              console.error("Failed to start data update service in production, continuing anyway:", error);
              // Don't throw in production - allow server to run without data service
            }
          } else {
            // In development, use normal startup
            const updateServiceInterval = await startDataUpdateService();
            
            if (updateServiceInterval) {
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
              console.warn("Data update service initialization returned no interval - this is unexpected");
            }
          }
        } catch (error) {
          console.error("Failed to start data update service:", error);
          if (process.env.NODE_ENV !== 'production') {
            throw error; // Only rethrow in development
          }
        }
      };
      
      // Start the data service
      await startDataService();
    }
  } catch (error) {
    console.error("Failed to start server:", error);
    process.exit(1);
  }
};

startServer();
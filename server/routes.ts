import type { Express } from "express";
import { createServer, type Server } from "http";
import { getDailySummary } from "./controllers/summary";

export function registerRoutes(app: Express): Server {
  // Daily summary endpoint
  app.get("/api/summary/daily/:date", getDailySummary);

  const httpServer = createServer(app);
  return httpServer;
}

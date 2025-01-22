import type { Express } from "express";
import { createServer, type Server } from "http";
import { getDailySummary, getMonthlySummary } from "./controllers/summary";
import { processDailyCurtailment } from "./services/curtailment";
import { getWindFarmPerformance } from "./controllers/windFarms";

export function registerRoutes(app: Express): Server {
  // Daily summary endpoint
  app.get("/api/summary/daily/:date", getDailySummary);

  // Monthly summary endpoint
  app.get("/api/summary/monthly/:yearMonth", getMonthlySummary);

  // Wind farm performance endpoint
  app.get("/api/wind-farms/performance", getWindFarmPerformance);

  // Re-ingest data endpoint
  app.post("/api/ingest/:date", async (req, res) => {
    try {
      const { date } = req.params;
      console.log(`Starting re-ingestion for date: ${date}`);
      await processDailyCurtailment(date);
      res.json({ message: `Successfully re-ingested data for ${date}` });
    } catch (error) {
      console.error('Error during re-ingestion:', error);
      res.status(500).json({ error: 'Failed to re-ingest data' });
    }
  });

  const httpServer = createServer(app);
  return httpServer;
}
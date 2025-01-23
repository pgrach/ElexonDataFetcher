import type { Express } from "express";
import { createServer, type Server } from "http";
import { getDailySummary, getMonthlySummary, getHourlyCurtailment } from "./controllers/summary";
import { processDailyCurtailment } from "./services/curtailment";
import { getAllFarms, getFarmById, initializeFarms } from "./services/farms";

export function registerRoutes(app: Express): Server {
  // Initialize farms data
  initializeFarms().catch(console.error);

  // Farm endpoints
  app.get("/api/farms", async (req, res) => {
    try {
      const farms = await getAllFarms();
      res.json(farms);
    } catch (error) {
      console.error('Error fetching farms:', error);
      res.status(500).json({ error: 'Failed to fetch farms' });
    }
  });

  app.get("/api/farms/:id", async (req, res) => {
    try {
      const farm = await getFarmById(req.params.id);
      if (!farm) {
        return res.status(404).json({ error: 'Farm not found' });
      }
      res.json(farm);
    } catch (error) {
      console.error('Error fetching farm:', error);
      res.status(500).json({ error: 'Failed to fetch farm' });
    }
  });

  // Daily summary endpoint
  app.get("/api/summary/daily/:date", getDailySummary);
  app.get("/api/summary/daily/:date/:farmId", getDailySummary);

  // Monthly summary endpoint
  app.get("/api/summary/monthly/:yearMonth", getMonthlySummary);
  app.get("/api/summary/monthly/:yearMonth/:farmId", getMonthlySummary);

  // Hourly curtailment data endpoint
  app.get("/api/curtailment/hourly/:date", getHourlyCurtailment);
  app.get("/api/curtailment/hourly/:date/:farmId", getHourlyCurtailment);

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
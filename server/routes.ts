import type { Express } from "express";
import { createServer, type Server } from "http";
import { getDailySummary, getMonthlySummary, getHourlyCurtailment, getLeadParties, getCurtailedLeadParties, getYearlySummary } from "./controllers/summary";
import { processDailyCurtailment } from "./services/curtailment";
import curtailmentRoutes from "./routes/curtailmentRoutes";
import miningPotentialRoutes from "./routes/miningPotentialRoutes";

export function registerRoutes(app: Express): Server {
  // Get lead parties endpoint
  app.get("/api/lead-parties", getLeadParties);

  // Get curtailed lead parties for a specific date
  app.get("/api/lead-parties/:date", getCurtailedLeadParties);

  // Daily summary endpoint - uses route parameters for date
  app.get("/api/summary/daily/:date", getDailySummary);

  // Monthly summary endpoint
  app.get("/api/summary/monthly/:yearMonth", getMonthlySummary);

  // Yearly summary endpoint
  app.get("/api/summary/yearly/:year", getYearlySummary);

  // Hourly curtailment data endpoint
  app.get("/api/curtailment/hourly/:date", getHourlyCurtailment);

  // Register Bitcoin mining calculation routes
  app.use('/api/curtailment', curtailmentRoutes);
  
  // Register optimized mining potential routes
  app.use('/api/mining-potential', miningPotentialRoutes);

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
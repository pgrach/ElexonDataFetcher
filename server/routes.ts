import type { Express } from "express";
import { createServer, type Server } from "http";
import { getDailySummary } from "./controllers/summary";
import { processDailyCurtailment } from "./services/curtailment";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { desc, and, gte, lte } from "drizzle-orm";

export function registerRoutes(app: Express): Server {
  // Daily summary endpoint
  app.get("/api/summary/daily/:date", getDailySummary);

  // Get all December 2024 summaries
  app.get("/api/summary/december-2024", async (_req, res) => {
    try {
      const decemberSummaries = await db.query.dailySummaries.findMany({
        where: and(
          gte(dailySummaries.summaryDate, "2024-12-01"),
          lte(dailySummaries.summaryDate, "2024-12-31")
        ),
        orderBy: desc(dailySummaries.summaryDate)
      });

      res.json(decemberSummaries);
    } catch (error) {
      console.error('Error fetching December summaries:', error);
      res.status(500).json({ error: 'Failed to fetch December summaries' });
    }
  });

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
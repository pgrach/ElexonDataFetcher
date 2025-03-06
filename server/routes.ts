import type { Express } from "express";
import { createServer, type Server } from "http";
import { getDailySummary, getMonthlySummary, getHourlyCurtailment, getLeadParties, getCurtailedLeadParties, getYearlySummary } from "./controllers/summary";
import { processDailyCurtailment } from "./services/curtailment";
import curtailmentRoutes from "./routes/curtailmentRoutes";
import optimizedMiningRoutes from "./routes/optimizedMiningRoutes";

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
  
  // Register optimized mining potential routes - uses direct table queries
  app.use('/api/mining-potential', optimizedMiningRoutes);

  // Re-ingest data endpoint
  app.post("/api/ingest/:date", async (req, res) => {
    try {
      const { date } = req.params;
      console.log(`Starting re-ingestion for date: ${date}`);
      
      // Step 1: Reingest curtailment records
      await processDailyCurtailment(date);
      console.log(`Successfully re-ingested curtailment data for ${date}`);
      
      // Step 2: Process Bitcoin calculations for all miner models
      const minerModels = ['S19J_PRO', 'S9', 'M20S'];
      const { processSingleDay } = await import('./services/bitcoinService');
      
      console.log(`Updating Bitcoin calculations for ${date}...`);
      for (const minerModel of minerModels) {
        await processSingleDay(date, minerModel);
        console.log(`- Processed ${minerModel}`);
      }
      
      // Step 3: Verify the update with stats
      const { db } = await import('../db');
      const { curtailmentRecords, dailySummaries } = await import('../db/schema');
      const { eq, sql } = await import('drizzle-orm');
      
      const stats = await db
        .select({
          recordCount: sql<number>`COUNT(*)`,
          periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
          totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
          totalPayment: sql<string>`SUM(payment::numeric)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date));
      
      res.json({ 
        message: `Successfully re-ingested data for ${date}`,
        stats: {
          records: stats[0]?.recordCount || 0,
          periods: stats[0]?.periodCount || 0,
          volume: Number(stats[0]?.totalVolume || 0).toFixed(2),
          payment: Number(stats[0]?.totalPayment || 0).toFixed(2)
        }
      });
    } catch (error) {
      console.error('Error during re-ingestion:', error);
      res.status(500).json({ error: 'Failed to re-ingest data' });
    }
  });

  const httpServer = createServer(app);
  return httpServer;
}
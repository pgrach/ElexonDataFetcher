import type { Express } from "express";
import { createServer, type Server } from "http";
import * as summaryController from "./controllers/summaryController";
import { processDailyCurtailment } from "./services/curtailmentService";
import curtailmentRoutes from "./routes/curtailmentRoutes";
import optimizedMiningRoutes from "./routes/optimizedMiningRoutes";
import productionRoutes from "./routes/productionRoutes";
import farmDataTableRoutes from "./routes/farmDataTableRoutes";
import windGenerationRoutes from "./routes/windGenerationRoutes";

export function registerRoutes(app: Express): Server {
  // Get lead parties endpoint
  app.get("/api/lead-parties", summaryController.getLeadParties);

  // Get curtailed lead parties for a specific date
  app.get("/api/lead-parties/:date", summaryController.getCurtailedLeadParties);
  
  // Get the most recent date with curtailment data
  app.get("/api/latest-date", summaryController.getLatestDate);

  // Daily summary endpoint - uses route parameters for date
  app.get("/api/summary/daily/:date", summaryController.getDailySummary);

  // Monthly summary endpoint
  app.get("/api/summary/monthly/:yearMonth", summaryController.getMonthlySummary);

  // Yearly summary endpoint
  app.get("/api/summary/yearly/:year", summaryController.getYearlySummary);

  // Hourly curtailment data endpoint
  app.get("/api/curtailment/hourly/:date", summaryController.getHourlyCurtailment);
  
  // Hourly comparison data endpoint for the farm opportunity chart
  app.get("/api/curtailment/hourly-comparison/:date", summaryController.getHourlyComparison);
  
  // Monthly comparison data endpoint for the farm opportunity chart
  app.get("/api/curtailment/monthly-comparison/:yearMonth", summaryController.getMonthlyComparison);

  // Register Bitcoin mining calculation routes
  app.use('/api/curtailment', curtailmentRoutes);
  
  // Register optimized mining potential routes - uses direct table queries
  app.use('/api/mining-potential', optimizedMiningRoutes);
  
  // Register production data routes for PN data and curtailment percentages
  app.use('/api/production', productionRoutes);
  
  // Register farm data table routes
  app.use('/api/farm-tables', farmDataTableRoutes);
  
  // Register wind generation data routes
  app.use('/api/wind-generation', windGenerationRoutes);

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
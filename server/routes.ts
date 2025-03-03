import type { Express } from "express";
import { createServer, type Server } from "http";
import { getDailySummary, getMonthlySummary, getHourlyCurtailment, getLeadParties, getCurtailedLeadParties, getYearlySummary } from "./controllers/summary";
import { processDailyCurtailment } from "./services/curtailment";
import curtailmentRoutes from "./routes/curtailmentRoutes";
import fs from "fs";
import path from "path";

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
  
  // BMU Mapping download page
  app.get("/bmu-download", (req, res) => {
    res.sendFile("bmu_download.html", { root: "./public" });
  });
  
  // BMU Mapping data API endpoint
  app.get("/api/bmu-mapping", (req, res) => {
    try {
      const bmuMappingPath = path.resolve("./server/data/bmuMapping.json");
      const bmuMappingData = JSON.parse(fs.readFileSync(bmuMappingPath, "utf8"));
      res.json(bmuMappingData);
    } catch (error) {
      console.error("Error serving BMU mapping data:", error);
      res.status(500).json({ error: "Failed to retrieve BMU mapping data" });
    }
  });
  
  // Get BMU by National Grid ID endpoint
  app.get("/api/bmu-mapping/:id", (req, res) => {
    try {
      const { id } = req.params;
      const bmuMappingPath = path.resolve("./server/data/bmuMapping.json");
      const bmuMappingData = JSON.parse(fs.readFileSync(bmuMappingPath, "utf8"));
      
      const bmu = bmuMappingData.find((item: any) => 
        item.nationalGridBmUnit.toLowerCase() === id.toLowerCase()
      );
      
      if (bmu) {
        res.json(bmu);
      } else {
        res.status(404).json({ error: `BMU with ID ${id} not found` });
      }
    } catch (error) {
      console.error(`Error retrieving BMU with ID ${req.params.id}:`, error);
      res.status(500).json({ error: "Failed to retrieve BMU data" });
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
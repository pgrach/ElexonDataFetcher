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
  
  // BMU summary statistics endpoint
  app.get("/api/bmu-mapping/summary", (req, res) => {
    try {
      const bmuMappingPath = path.resolve("./server/data/bmuMapping.json");
      const bmuMappingData = JSON.parse(fs.readFileSync(bmuMappingPath, "utf8"));
      
      // Create summary by fuel type with proper type definition
      interface FuelTypeSummary {
        count: number;
        totalCapacity: number;
      }
      
      interface Summary {
        totalBmus: number;
        byFuelType: Record<string, FuelTypeSummary>;
      }
      
      const summary: Summary = {
        totalBmus: bmuMappingData.length,
        byFuelType: {}
      };
      
      // Group by fuel type
      bmuMappingData.forEach((bmu: any) => {
        const fuelType = bmu.fuelType || 'Unknown';
        if (!summary.byFuelType[fuelType]) {
          summary.byFuelType[fuelType] = {
            count: 0,
            totalCapacity: 0
          };
        }
        
        summary.byFuelType[fuelType].count++;
        const capacity = parseFloat(bmu.generationCapacity || '0');
        if (!isNaN(capacity)) {
          summary.byFuelType[fuelType].totalCapacity += capacity;
        }
      });
      
      // Format capacities to 2 decimal places
      Object.keys(summary.byFuelType).forEach(fuelType => {
        summary.byFuelType[fuelType].totalCapacity = 
          parseFloat(summary.byFuelType[fuelType].totalCapacity.toFixed(2));
      });
      
      res.json(summary);
    } catch (error) {
      console.error("Error creating BMU summary:", error);
      res.status(500).json({ error: "Failed to create BMU summary" });
    }
  });
  
  // Get all lead party names endpoint
  app.get("/api/bmu-mapping/lead-parties", (req, res) => {
    try {
      const bmuMappingPath = path.resolve("./server/data/bmuMapping.json");
      const bmuMappingData = JSON.parse(fs.readFileSync(bmuMappingPath, "utf8"));
      
      // Extract unique lead party names
      const leadPartyNames = new Set<string>();
      
      bmuMappingData.forEach((bmu: any) => {
        if (bmu.leadPartyName) {
          leadPartyNames.add(bmu.leadPartyName);
        }
      });
      
      // Sort alphabetically
      const sortedLeadParties = Array.from(leadPartyNames).sort();
      
      res.json(sortedLeadParties);
    } catch (error) {
      console.error("Error retrieving lead party names:", error);
      res.status(500).json({ error: "Failed to retrieve lead party names" });
    }
  });
  
  // Search BMUs by lead party name
  app.get("/api/bmu-mapping/search/lead-party", (req, res) => {
    try {
      const { name } = req.query;
      
      if (!name || typeof name !== 'string') {
        return res.status(400).json({ error: "Lead party name is required" });
      }
      
      const bmuMappingPath = path.resolve("./server/data/bmuMapping.json");
      const bmuMappingData = JSON.parse(fs.readFileSync(bmuMappingPath, "utf8"));
      
      const filteredBmus = bmuMappingData.filter((item: any) => 
        item.leadPartyName && item.leadPartyName.toLowerCase().includes(name.toLowerCase())
      );
      
      if (filteredBmus.length > 0) {
        res.json(filteredBmus);
      } else {
        res.status(404).json({ error: `No BMUs found for lead party name containing '${name}'` });
      }
    } catch (error) {
      console.error("Error searching BMUs by lead party:", error);
      res.status(500).json({ error: "Failed to search BMU data" });
    }
  });
  
  // Search BMUs by generation capacity range
  app.get("/api/bmu-mapping/search/capacity", (req, res) => {
    try {
      const minCapacity = req.query.min ? parseFloat(req.query.min as string) : 0;
      const maxCapacity = req.query.max ? parseFloat(req.query.max as string) : Infinity;
      
      if (isNaN(minCapacity) || isNaN(maxCapacity)) {
        return res.status(400).json({ error: "Capacity values must be numbers" });
      }
      
      const bmuMappingPath = path.resolve("./server/data/bmuMapping.json");
      const bmuMappingData = JSON.parse(fs.readFileSync(bmuMappingPath, "utf8"));
      
      const filteredBmus = bmuMappingData.filter((item: any) => {
        const capacity = parseFloat(item.generationCapacity);
        return !isNaN(capacity) && capacity >= minCapacity && capacity <= maxCapacity;
      });
      
      if (filteredBmus.length > 0) {
        res.json(filteredBmus);
      } else {
        res.status(404).json({ 
          error: `No BMUs found with capacity between ${minCapacity} and ${maxCapacity === Infinity ? 'unlimited' : maxCapacity} MW` 
        });
      }
    } catch (error) {
      console.error("Error searching BMUs by capacity:", error);
      res.status(500).json({ error: "Failed to search BMU data by capacity" });
    }
  });
  
  // Search BMUs by fuel type
  app.get("/api/bmu-mapping/search/fuel-type", (req, res) => {
    try {
      const { type } = req.query;
      
      if (!type || typeof type !== 'string') {
        return res.status(400).json({ error: "Fuel type is required" });
      }
      
      const bmuMappingPath = path.resolve("./server/data/bmuMapping.json");
      const bmuMappingData = JSON.parse(fs.readFileSync(bmuMappingPath, "utf8"));
      
      const filteredBmus = bmuMappingData.filter((item: any) => 
        item.fuelType && item.fuelType.toLowerCase() === type.toLowerCase()
      );
      
      if (filteredBmus.length > 0) {
        res.json(filteredBmus);
      } else {
        res.status(404).json({ error: `No BMUs found with fuel type '${type}'` });
      }
    } catch (error) {
      console.error("Error searching BMUs by fuel type:", error);
      res.status(500).json({ error: "Failed to search BMU data by fuel type" });
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
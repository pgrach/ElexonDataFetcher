#!/usr/bin/env tsx
/**
 * Check specific time periods for 2025-03-11
 * 
 * This script examines settlement periods 3, 4, 7, and 8 (1:00 and 3:00)
 * and compares with Elexon API data.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, sql, count, inArray } from "drizzle-orm";
import { fetchBidsOffers } from "./server/services/elexon";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const DATE = "2025-03-11";
const TIME_PERIODS = [3, 4, 7, 8]; // 1:00 and 3:00
const BMU_MAPPING_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), "server", "data", "bmuMapping.json");

async function main() {
  console.log(`\nChecking time periods (1:00 and 3:00) for ${DATE}...\n`);
  
  // Load BMU mapping to identify wind farms
  console.log('Loading wind farm IDs from BMU mapping...');
  const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
  const bmuMapping = JSON.parse(mappingContent);
  
  const windFarmIds = new Set(
    bmuMapping
      .filter((bmu: any) => bmu.fuelType === "WIND")
      .map((bmu: any) => bmu.elexonBmUnit)
  );
  
  console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
  
  // Check current DB data for these periods
  const dbPeriods = await db
    .select({
      period: curtailmentRecords.settlementPeriod,
      recordCount: count(curtailmentRecords.id),
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(
      eq(curtailmentRecords.settlementDate, DATE) &&
      inArray(curtailmentRecords.settlementPeriod, TIME_PERIODS)
    )
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
  
  console.log("=== Current Database Records ===");
  console.log("Period | Records | Volume (MWh) | Payment (£)");
  console.log("-------|---------|-------------|------------");
  
  const dbPeriodMap = new Map();
  
  dbPeriods.forEach(period => {
    console.log(
      `${period.period.toString().padStart(6)} | ` +
      `${period.recordCount.toString().padStart(7)} | ` +
      `${Number(period.totalVolume).toFixed(2).padStart(11)} | ` +
      `${Number(period.totalPayment).toFixed(2).padStart(10)}`
    );
    
    dbPeriodMap.set(period.period, {
      recordCount: period.recordCount,
      volume: Number(period.totalVolume),
      payment: Number(period.totalPayment)
    });
  });
  
  // Now fetch from Elexon API
  console.log("\n=== Fetching from Elexon API ===");
  
  const apiResults = new Map();
  
  for (const period of TIME_PERIODS) {
    console.log(`\nFetching period ${period}...`);
    
    try {
      const apiRecords = await fetchBidsOffers(DATE, period);
      const validRecords = apiRecords.filter(record => 
        record.volume < 0 &&
        (record.soFlag || record.cadlFlag) &&
        windFarmIds.has(record.id)
      );
      
      if (validRecords.length > 0) {
        const totalVolume = validRecords.reduce((sum, record) => sum + Math.abs(record.volume), 0);
        const totalPayment = validRecords.reduce((sum, record) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
        
        apiResults.set(period, {
          recordCount: validRecords.length,
          volume: totalVolume,
          payment: totalPayment
        });
        
        console.log(`Period ${period}: ${validRecords.length} records, ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
      } else {
        console.log(`Period ${period}: No valid records found`);
        apiResults.set(period, {
          recordCount: 0,
          volume: 0,
          payment: 0
        });
      }
    } catch (error) {
      console.error(`Error fetching period ${period}: ${error}`);
    }
  }
  
  // Compare results
  console.log("\n=== Comparison: Database vs API ===");
  console.log("Period | DB Records | API Records | DB Volume | API Volume | Match");
  console.log("-------|------------|-------------|-----------|------------|------");
  
  let missingData = false;
  
  for (const period of TIME_PERIODS) {
    const dbData = dbPeriodMap.get(period) || { recordCount: 0, volume: 0, payment: 0 };
    const apiData = apiResults.get(period) || { recordCount: 0, volume: 0, payment: 0 };
    
    const volumeMatch = Math.abs(dbData.volume - apiData.volume) < 0.1; // Allow small rounding differences
    
    console.log(
      `${period.toString().padStart(6)} | ` +
      `${dbData.recordCount.toString().padStart(10)} | ` +
      `${apiData.recordCount.toString().padStart(11)} | ` +
      `${dbData.volume.toFixed(2).padStart(9)} | ` +
      `${apiData.volume.toFixed(2).padStart(10)} | ` +
      `${volumeMatch ? '✅' : '❌'}`
    );
    
    if (!volumeMatch || dbData.recordCount !== apiData.recordCount) {
      missingData = true;
    }
  }
  
  // Summary
  console.log("\n=== Summary ===");
  if (missingData) {
    console.log("❌ Discrepancies detected between database and Elexon API");
    console.log("Action required: Reingest the highlighted periods");
  } else {
    console.log("✅ All checked periods match Elexon API data");
  }
}

main().catch(console.error);
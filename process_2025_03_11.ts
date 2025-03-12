#!/usr/bin/env tsx
/**
 * Efficient Script to Process Elexon API Data for 2025-03-11
 * 
 * This script processes data in smaller batches to ensure reliable execution
 * and provides comprehensive verification of data integrity.
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { eq, sql, count, and, between } from "drizzle-orm";
import { fetchBidsOffers } from "./server/services/elexon";
import { processSingleDay } from "./server/services/bitcoinService";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

// Configuration
const DATE = "2025-03-11";
const BATCH_SIZE = 6; // Process 6 periods at a time
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const LOG_FILE = `process_${DATE}_${new Date().toISOString().split('T')[0]}.log`;

// Get the directory path
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BMU_MAPPING_PATH = path.join(__dirname, "server", "data", "bmuMapping.json");

// Logging utilities
async function logToFile(message: string): Promise<void> {
  await fs.appendFile(LOG_FILE, `${new Date().toISOString()} - ${message}\n`, 'utf8').catch(console.error);
}

function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
  const colors = {
    info: "\x1b[36m•\x1b[0m",
    success: "\x1b[32m✓\x1b[0m",
    warning: "\x1b[33m⚠\x1b[0m",
    error: "\x1b[31m✗\x1b[0m"
  };

  const logMsg = `${colors[type]} ${message}`;
  console.log(logMsg);
  logToFile(logMsg).catch(() => {});
}

// Utility function to delay execution
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load wind farm BMU IDs
async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    const windFarmIds = new Set(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );
    
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    return windFarmIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

// Process a batch of periods
async function processBatch(
  startPeriod: number,
  endPeriod: number,
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  processedPeriods: number[];
  recordCount: number;
  totalVolume: number;
  totalPayment: number;
}> {
  log(`Processing periods ${startPeriod} to ${endPeriod}`, "info");
  
  const batchResults = {
    processedPeriods: [] as number[],
    recordCount: 0,
    totalVolume: 0,
    totalPayment: 0
  };

  // First, clear existing records for these periods
  await db.delete(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, DATE),
        between(curtailmentRecords.settlementPeriod, startPeriod, endPeriod)
      )
    );
  
  // Process each period in the batch
  for (let period = startPeriod; period <= endPeriod; period++) {
    try {
      const records = await fetchBidsOffers(DATE, period);
      const validRecords = records.filter(record => 
        record.volume < 0 &&
        (record.soFlag || record.cadlFlag) &&
        windFarmIds.has(record.id)
      );
      
      let periodVolume = 0;
      let periodPayment = 0;
      
      if (validRecords.length > 0) {
        log(`[${DATE} P${period}] Processing ${validRecords.length} records`, "info");
        
        for (const record of validRecords) {
          const volume = Math.abs(record.volume);
          const payment = volume * record.originalPrice;
          
          try {
            await db.insert(curtailmentRecords).values({
              settlementDate: DATE,
              settlementPeriod: period,
              farmId: record.id,
              leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
              volume: record.volume.toString(), // Keep the original negative value
              payment: payment.toString(),
              originalPrice: record.originalPrice.toString(),
              finalPrice: record.finalPrice.toString(),
              soFlag: record.soFlag,
              cadlFlag: record.cadlFlag
            });
            
            log(`[${DATE} P${period}] Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`, "success");
            
            periodVolume += volume;
            periodPayment += payment;
            batchResults.recordCount++;
          } catch (error) {
            log(`[${DATE} P${period}] Error inserting record for ${record.id}: ${error}`, "error");
          }
        }
        
        log(`[${DATE} P${period}] Total: ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`, "info");
        batchResults.processedPeriods.push(period);
        batchResults.totalVolume += periodVolume;
        batchResults.totalPayment += periodPayment;
      } else {
        log(`[${DATE} P${period}] No valid records found`, "info");
        batchResults.processedPeriods.push(period);
      }
    } catch (error) {
      log(`Error processing period ${period}: ${error}`, "error");
    }
    
    // Add a small delay between periods to avoid overwhelming the API
    await delay(100);
  }
  
  return batchResults;
}

// Process Bitcoin calculations
async function processBitcoinCalculations(): Promise<void> {
  log(`Updating Bitcoin calculations for ${DATE}...`, "info");
  
  // First, remove existing Bitcoin calculations
  await db.delete(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, DATE));
  
  // Process each miner model
  for (const minerModel of MINER_MODELS) {
    log(`Processing ${minerModel}...`, "info");
    await processSingleDay(DATE, minerModel);
  }
  
  log(`Bitcoin calculations updated for ${DATE}`, "success");
}

// Verify the reconciliation results
async function verifyResults(): Promise<void> {
  // Check curtailment records
  const curtailmentStats = await db
    .select({
      recordCount: count(curtailmentRecords.id),
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE));
  
  // Check Bitcoin calculations
  const bitcoinStats = await Promise.all(
    MINER_MODELS.map(async (model) => {
      const result = await db
        .select({
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`,
          recordCount: count()
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, DATE),
            eq(historicalBitcoinCalculations.minerModel, model)
          )
        );
      
      return { 
        model, 
        bitcoin: result[0]?.totalBitcoin || "0",
        count: result[0]?.recordCount || 0
      };
    })
  );
  
  // Print verification results
  console.log("\n=== Reconciliation Results ===");
  console.log(`Date: ${DATE}`);
  console.log(`Records: ${curtailmentStats[0]?.recordCount || 0}`);
  console.log(`Periods: ${curtailmentStats[0]?.periodCount || 0} / 48`);
  console.log(`Farms: ${curtailmentStats[0]?.farmCount || 0}`);
  console.log(`Volume: ${Number(curtailmentStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
  console.log(`Payment: £${Number(curtailmentStats[0]?.totalPayment || 0).toFixed(2)}`);
  
  console.log("\n=== Bitcoin Calculations ===");
  for (const stat of bitcoinStats) {
    console.log(`${stat.model}: ${Number(stat.bitcoin).toFixed(8)} BTC (${stat.count} records)`);
  }
  
  // Check for missing periods
  const missingPeriods = 48 - (curtailmentStats[0]?.periodCount || 0);
  if (missingPeriods > 0) {
    log(`Warning: Missing data for ${missingPeriods} periods`, "warning");
    
    // Find which periods are missing
    const existingPeriods = await db
      .select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE))
      .groupBy(curtailmentRecords.settlementPeriod);
    
    const existingPeriodNumbers = existingPeriods.map(p => p.period);
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    const missingPeriodList = allPeriods.filter(p => !existingPeriodNumbers.includes(p));
    
    log(`Missing periods: ${missingPeriodList.join(', ')}`, "warning");
  } else {
    log("All 48 settlement periods have data", "success");
  }
}

// Main function
async function main() {
  const startTime = performance.now();
  
  try {
    log(`Starting data reingestion for ${DATE}`, "info");
    
    // Load BMU mappings
    const windFarmIds = await loadWindFarmIds();
    
    // Load BMU-to-LeadParty mapping
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    const bmuLeadPartyMap = new Map(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
    );
    
    // Check initial state
    const initialState = await db
      .select({
        recordCount: count(curtailmentRecords.id),
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE));
    
    log(`Initial state: ${initialState[0]?.recordCount || 0} records, ${Number(initialState[0]?.totalVolume || 0).toFixed(2)} MWh, £${Number(initialState[0]?.totalPayment || 0).toFixed(2)}`, "info");
    
    // Process in batches of BATCH_SIZE periods
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    let processedPeriods: number[] = [];
    
    for (let startPeriod = 1; startPeriod <= 48; startPeriod += BATCH_SIZE) {
      const endPeriod = Math.min(startPeriod + BATCH_SIZE - 1, 48);
      
      const batchResults = await processBatch(
        startPeriod,
        endPeriod,
        windFarmIds,
        bmuLeadPartyMap
      );
      
      totalRecords += batchResults.recordCount;
      totalVolume += batchResults.totalVolume;
      totalPayment += batchResults.totalPayment;
      processedPeriods = [...processedPeriods, ...batchResults.processedPeriods];
      
      log(`Batch ${startPeriod}-${endPeriod} completed: ${batchResults.recordCount} records, ${batchResults.totalVolume.toFixed(2)} MWh`, "success");
      
      // Small delay between batches to avoid overwhelming the API
      await delay(500);
    }
    
    log(`Reingestion complete: ${totalRecords} records, ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`, "success");
    
    // Process Bitcoin calculations
    await processBitcoinCalculations();
    
    // Verify results
    await verifyResults();
    
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(1);
    
    log(`Processing completed in ${duration}s`, "success");
  } catch (error) {
    log(`Error during processing: ${error}`, "error");
    process.exit(1);
  }
}

// Run the script
main();
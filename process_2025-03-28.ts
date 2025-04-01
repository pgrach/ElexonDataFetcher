/**
 * Process Missing Data for 2025-03-28
 * 
 * This script will:
 * 1. Check current data for 2025-03-28
 * 2. Process any missing or incomplete data from Elexon API
 * 3. Update all Bitcoin calculations to ensure completeness
 */

import { db } from './db';
import { and, between, eq } from 'drizzle-orm';
import { curtailmentRecords, historicalBitcoinCalculations, dailySummaries } from './db/schema';
import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { sql } from 'drizzle-orm';

// Handle ESM path resolution
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const LOG_FILE = `process_2025-03-28_${new Date().toISOString().split('T')[0]}.log`;
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds
const BATCH_SIZE = 5; // Number of periods to process in a batch

// Target date to process
const date = '2025-03-28';
const START_PERIOD = 1;
const END_PERIOD = 48;

// Logging utilities
async function logToFile(message: string): Promise<void> {
  try {
    await fs.appendFile(LOG_FILE, `${new Date().toISOString()} - ${message}\n`);
  } catch (error) {
    console.error('Error writing to log file:', error);
  }
}

function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
  const timestamp = new Date().toISOString().split('T')[1].slice(0, 8);
  let formattedMessage = '';
  
  switch(type) {
    case "success":
      formattedMessage = `\x1b[32m[${timestamp}] ${message}\x1b[0m`; // Green
      break;
    case "warning":
      formattedMessage = `\x1b[33m[${timestamp}] ${message}\x1b[0m`; // Yellow
      break;
    case "error":
      formattedMessage = `\x1b[31m[${timestamp}] ${message}\x1b[0m`; // Red
      break;
    default:
      formattedMessage = `\x1b[36m[${timestamp}] ${message}\x1b[0m`; // Cyan
  }
  
  console.log(formattedMessage);
  logToFile(message).catch(() => {}); // Log to file but don't block on errors
}

// Utility function to delay execution
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mappings once
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`, "info");
  
  try {
    const data = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(data);
    
    const windFarmIds = new Set<string>();
    const bmuLeadPartyMap = new Map<string, string>();
    
    for (const bmu of bmuMapping) {
      if (bmu.fuelType === "WIND" && bmu.elexonBmUnit) {
        windFarmIds.add(bmu.elexonBmUnit);
        bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown');
      }
    }
    
    log(`Found ${windFarmIds.size} wind farm BMUs`, "success");
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    log(`Error loading BMU mapping: ${error}`, "error");
    throw error;
  }
}

// Process a single settlement period
async function processPeriod(
  period: number,
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>,
  attempt: number = 1
): Promise<{
  success: boolean;
  records: number;
  volume: number;
  payment: number;
}> {
  log(`Processing period ${period} (attempt ${attempt})`, "info");
  
  try {
    // Import the fetchBidsOffers function from the existing service
    const { fetchBidsOffers } = await import('./server/services/elexon');
    const validRecords = await fetchBidsOffers(date, period);
    
    const totalVolume = validRecords.reduce((sum: number, record: any) => sum + Math.abs(record.volume), 0);
    const totalPayment = validRecords.reduce((sum: number, record: any) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
    
    log(`Period ${period}: Found ${validRecords.length} valid records (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`, "success");
    
    if (validRecords.length === 0) {
      return { success: true, records: 0, volume: 0, payment: 0 };
    }
    
    // Clear all existing records for this period - simpler approach that guarantees no duplicates
    const deleteResult = await db.delete(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    // Insert new records
    const insertData = validRecords.map(record => ({
      settlementDate: date,
      settlementPeriod: period,
      farmId: record.id,
      leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
      volume: record.volume.toString(), // Convert to string as required by schema
      payment: (record.volume * record.originalPrice).toString(), // Convert to string
      originalPrice: record.originalPrice.toString(),
      finalPrice: record.finalPrice.toString(),
      soFlag: record.soFlag,
      cadlFlag: record.cadlFlag
    }));
    
    await db.insert(curtailmentRecords).values(insertData);
    
    // Log results for verification
    for (const record of validRecords) {
      log(`[${date} P${period}] Added record for ${record.id}: ${Math.abs(record.volume)} MWh, £${record.volume * record.originalPrice}`, "info");
    }
    
    log(`[${date} P${period}] Total: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`, "info");
    
    return {
      success: true,
      records: validRecords.length,
      volume: totalVolume,
      payment: totalPayment
    };
  } catch (error) {
    log(`Error processing period ${period}: ${error}`, "error");
    
    if (attempt < MAX_RETRIES) {
      log(`Retrying period ${period} (attempt ${attempt + 1})...`, "warning");
      await delay(RETRY_DELAY);
      return processPeriod(period, windFarmIds, bmuLeadPartyMap, attempt + 1);
    }
    
    return {
      success: false,
      records: 0,
      volume: 0,
      payment: 0
    };
  }
}

// Process batch of periods
async function processBatch(
  periods: number[],
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  success: boolean;
  records: number;
  volume: number;
  payment: number;
  failedPeriods: number[];
}> {
  const results = await Promise.all(
    periods.map(period => processPeriod(period, windFarmIds, bmuLeadPartyMap))
  );
  
  const summaryResult = {
    success: results.every(r => r.success),
    records: results.reduce((sum, r) => sum + r.records, 0),
    volume: results.reduce((sum, r) => sum + r.volume, 0),
    payment: results.reduce((sum, r) => sum + r.payment, 0),
    failedPeriods: periods.filter((_, i) => !results[i].success)
  };
  
  return summaryResult;
}

// Process all periods for the date
async function processDate(): Promise<void> {
  log(`Starting processing for ${date}`, "info");
  
  try {
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== Processing ${date} ===\n`);
    
    // Load BMU mappings once
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Process periods in batches
    let totalProcessed = 0;
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    let failedPeriods: number[] = [];
    
    log(`Starting batch processing for periods ${START_PERIOD}-${END_PERIOD}`, "info");
    
    for (let batchStart = START_PERIOD; batchStart <= END_PERIOD; batchStart += BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, END_PERIOD);
      const periodsToProcess = Array.from({ length: batchEnd - batchStart + 1 }, (_, i) => batchStart + i);
      
      log(`Processing batch: periods ${batchStart}-${batchEnd}`, "info");
      
      const batchResult = await processBatch(periodsToProcess, windFarmIds, bmuLeadPartyMap);
      
      totalProcessed += periodsToProcess.length;
      totalRecords += batchResult.records;
      totalVolume += batchResult.volume;
      totalPayment += batchResult.payment;
      failedPeriods.push(...batchResult.failedPeriods);
      
      log(`Processed ${totalProcessed}/${END_PERIOD - START_PERIOD + 1} periods (${(totalProcessed / (END_PERIOD - START_PERIOD + 1) * 100).toFixed(1)}%)`, "info");
      
      // Add a short delay between batches to avoid rate limiting
      if (batchEnd < END_PERIOD) {
        await delay(1000);
      }
    }
    
    log(`Processing complete for ${date}`, "success");
    log(`Stats: ${totalRecords} records, ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`, "success");
    
    if (failedPeriods.length > 0) {
      log(`Failed periods: ${failedPeriods.join(', ')}`, "warning");
    }
    
    // Verify the result with the database
    const dbStats = await db
      .select({
        records: sql<number>`COUNT(*)`,
        periods: sql<number>`COUNT(DISTINCT settlement_period)`,
        volume: sql<string>`SUM(ABS(volume::numeric))`,
        payment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    log(`Database verification: ${dbStats[0].records} records, ${dbStats[0].periods} periods, ${Number(dbStats[0].volume).toFixed(2)} MWh, £${Number(dbStats[0].payment).toFixed(2)}`, "info");
    
    // Update daily summary - use the existing service function instead of custom implementation
    try {
      log('Using processDailyCurtailment to regenerate summaries', "info");
      const { processDailyCurtailment } = await import('./server/services/curtailment');
      // The function only takes the date parameter and always updates summaries
      await processDailyCurtailment(date);
      log('Daily summary updated successfully', "success");
    } catch (error) {
      log(`Error updating daily summary: ${error}`, "error");
      // Continue even if summary update fails - it's not critical
    }
    
  } catch (error) {
    log(`Error processing date: ${error}`, "error");
    throw error;
  }
}

// Find missing calculations
async function findMissingCalculations(): Promise<number[]> {
  try {
    log(`Finding missing Bitcoin calculations for ${date}`, "info");
    
    // Get all periods with curtailment records
    const curtailmentPeriods = await db
      .select({
        period: curtailmentRecords.settlementPeriod
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.settlementPeriod);
    
    const periods = curtailmentPeriods.map(p => p.period);
    
    // Get periods with Bitcoin calculations (for a sample miner model)
    const calculatedPeriods = await db
      .select({
        period: historicalBitcoinCalculations.settlementPeriod
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          eq(historicalBitcoinCalculations.minerModel, 'S19J_PRO')
        )
      )
      .groupBy(historicalBitcoinCalculations.settlementPeriod);
    
    const calculatedSet = new Set(calculatedPeriods.map(p => p.period));
    
    // Find the difference
    const missingPeriods = periods.filter(p => !calculatedSet.has(p));
    
    log(`Found ${missingPeriods.length} periods with missing Bitcoin calculations`, "info");
    
    return missingPeriods;
  } catch (error) {
    log(`Error finding missing calculations: ${error}`, "error");
    return [];
  }
}

// Update Bitcoin calculations
async function updateBitcoinCalculations(): Promise<void> {
  try {
    log(`Updating Bitcoin calculations for ${date}`, "info");
    
    // Clear existing calculations to ensure consistency
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, date));
    
    log(`Cleared existing Bitcoin calculations for ${date}`, "info");
    
    // Import bitcoin service
    const { processSingleDay, calculateMonthlyBitcoinSummary, manualUpdateYearlyBitcoinSummary } = await import('./server/services/bitcoinService');
    
    // Process for each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      log(`Processing ${date} with ${minerModel}`, "info");
      await processSingleDay(date, minerModel);
    }
    
    log(`Bitcoin calculations updated for models: ${minerModels.join(', ')}`, "success");
    
    // Update monthly summary
    const month = date.substring(0, 7); // e.g., 2025-03
    log(`Updating monthly Bitcoin summary for ${month}...`, "info");
    
    for (const minerModel of minerModels) {
      log(`Calculating monthly Bitcoin summary for ${month} with ${minerModel}`, "info");
      await calculateMonthlyBitcoinSummary(month, minerModel);
    }
    
    log(`Monthly Bitcoin summaries updated for ${month}`, "success");
    
    // Update yearly summary
    const year = date.substring(0, 4); // e.g., 2025
    log(`Updating yearly Bitcoin summary for ${year}...`, "info");
    await manualUpdateYearlyBitcoinSummary(year);
    
    log(`Yearly Bitcoin summaries updated for ${year}`, "success");
    
    // Verify updates
    const verificationResult = await db
      .select({
        records: sql<string>`COUNT(*)`,
        periods: sql<string>`COUNT(DISTINCT settlement_period)`,
        volume: sql<string>`SUM(ABS(volume))`,
        payment: sql<string>`SUM(payment)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    log(`Verification Check for ${date}: ${JSON.stringify(verificationResult[0])}`, "info");
    
  } catch (error) {
    log(`Error updating Bitcoin calculations: ${error}`, "error");
    throw error;
  }
}

// Main function
async function main() {  
  log(`Starting processing script for ${date}`, "info");
  
  try {
    // Get current state for comparison
    const initialState = await db
      .select({
        records: sql<string>`COUNT(*)`,
        volume: sql<string>`SUM(ABS(volume::numeric))`,
        payment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    log(`Initial state: ${initialState[0].records} records, ${Number(initialState[0].volume || 0).toFixed(2)} MWh, £${Number(initialState[0].payment || 0).toFixed(2)}`, "info");
    
    // Process all periods
    await processDate();
    
    // Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Get final state for comparison
    const finalState = await db
      .select({
        records: sql<string>`COUNT(*)`,
        volume: sql<string>`SUM(ABS(volume::numeric))`,
        payment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    log(`Final state: ${finalState[0].records} records, ${Number(finalState[0].volume || 0).toFixed(2)} MWh, £${Number(finalState[0].payment || 0).toFixed(2)}`, "success");
    
    const changeInVolume = Number(finalState[0].volume || 0) - Number(initialState[0].volume || 0);
    const changeInPayment = Number(finalState[0].payment || 0) - Number(initialState[0].payment || 0);
    
    log(`Changes: ${changeInVolume.toFixed(2)} MWh, £${changeInPayment.toFixed(2)}`, "success");
    
    log(`Update successful at ${new Date().toISOString()}`, "success");
    log(`=== Update Summary ===`);
    log(`Duration: ${((Date.now() - new Date().getTime()) / 1000).toFixed(1)}s`);
    
  } catch (error) {
    log(`Fatal error: ${error}`, "error");
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
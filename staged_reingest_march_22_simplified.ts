/**
 * Staged Reingest for March 22, 2025
 * 
 * This script allows for reingesting settlement periods in smaller batches.
 * Set START_PERIOD and END_PERIOD to control which range to process.
 * 
 * The current data shows 46 of 48 settlement periods with a payment of £63,809.23.
 * This script will ensure all 48 periods are properly processed.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import { fetchBidsOffers } from "./server/services/elexon";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TARGET_DATE = '2025-03-22';
const START_PERIOD = 47; // Start with the missing periods (47-48)
const END_PERIOD = 48;  // Recommended batch size: 6 periods per batch
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");
const API_THROTTLE_MS = 500; // Delay between API calls to avoid rate limits

// Utility function to delay between API calls
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mapping to get valid wind farm IDs
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  try {
    console.log('Loading BMU mapping from: ' + BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    const windFarmIds = new Set<string>(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );
    
    const bmuLeadPartyMap = new Map<string, string>();
    for (const bmu of bmuMapping.filter((bmu: any) => bmu.fuelType === "WIND")) {
      bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown');
    }
    
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    console.error('Error loading BMU mapping: ' + error);
    throw error;
  }
}

// Clear the existing data for specific periods only
async function clearExistingPeriodsData(): Promise<void> {
  console.log(`Clearing data for ${TARGET_DATE} periods ${START_PERIOD} to ${END_PERIOD}...`);
  
  try {
    // Delete from historical_bitcoin_calculations first
    const deleteBitcoinResult = await db.query(
      `DELETE FROM historical_bitcoin_calculations 
       WHERE settlement_date = $1 
       AND settlement_period BETWEEN $2 AND $3`,
      [TARGET_DATE, START_PERIOD, END_PERIOD]
    );
    console.log(`Deleted ${deleteBitcoinResult.rowCount} Bitcoin calculation records`);
    
    // Delete specified periods from curtailment_records
    const deleteResult = await db.delete(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          sql`${curtailmentRecords.settlementPeriod} >= ${START_PERIOD}`,
          sql`${curtailmentRecords.settlementPeriod} <= ${END_PERIOD}`
        )
      );
    
    console.log(`Deleted curtailment records for periods ${START_PERIOD}-${END_PERIOD}`);
  } catch (error) {
    console.error(`Error clearing existing data: ${error}`);
    throw error;
  }
  
  return;
}

// Process a single settlement period
async function processPeriod(
  period: number, 
  windFarmIds: Set<string>, 
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  volume: number;
  payment: number;
  recordCount: number;
}> {
  console.log(`Processing period ${period} for ${TARGET_DATE}...`);
  
  try {
    // Get data from Elexon API
    const records = await fetchBidsOffers(TARGET_DATE, period);
    const validRecords = records.filter(record =>
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      windFarmIds.has(record.id)
    );
    
    console.log(`[${TARGET_DATE} P${period}] Records: ${validRecords.length} (${validRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0).toFixed(2)} MWh)`);
    
    if (validRecords.length === 0) {
      console.warn(`[${TARGET_DATE} P${period}] No valid curtailment records found`);
      return { volume: 0, payment: 0, recordCount: 0 };
    }
    
    // Insert records in a batch
    const insertValues = validRecords.map(record => {
      const volume = record.volume; // Keep the original negative value
      const payment = volume * record.originalPrice; // Payment will be negative
      
      return {
        settlementDate: TARGET_DATE,
        settlementPeriod: period,
        farmId: record.id,
        leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
        volume: volume.toString(),
        payment: payment.toString(),
        originalPrice: record.originalPrice.toString(),
        finalPrice: record.finalPrice.toString(),
        soFlag: record.soFlag,
        cadlFlag: record.cadlFlag
      };
    });
    
    // Insert all records at once
    if (insertValues.length > 0) {
      await db.insert(curtailmentRecords).values(insertValues);
    }
    
    // Calculate period totals
    const periodTotal = validRecords.reduce(
      (acc, record) => ({
        volume: acc.volume + Math.abs(record.volume),
        payment: acc.payment + (Math.abs(record.volume) * record.originalPrice)
      }),
      { volume: 0, payment: 0 }
    );
    
    console.log(`[${TARGET_DATE} P${period}] Total: ${periodTotal.volume.toFixed(2)} MWh, £${periodTotal.payment.toFixed(2)}`);
    
    return { 
      volume: periodTotal.volume, 
      payment: periodTotal.payment,
      recordCount: validRecords.length
    };
  } catch (error) {
    console.error(`Error processing period ${period}: ${error}`);
    return { volume: 0, payment: 0, recordCount: 0 };
  }
}

// Get current status of processed periods
async function getCompletedPeriods(): Promise<Set<number>> {
  const existingPeriods = await db
    .select({ period: curtailmentRecords.settlementPeriod })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .groupBy(curtailmentRecords.settlementPeriod);
    
  return new Set(existingPeriods.map(r => r.period));
}

// Process all periods in the specified range
async function main(): Promise<void> {
  console.log(`=== Staged Reingest for March 22, 2025 ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  console.log(`Target date: ${TARGET_DATE}`);
  console.log(`Processing periods: ${START_PERIOD} to ${END_PERIOD}`);
  
  try {
    // Step 1: Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Step 2: Clear existing data for the specified periods
    await clearExistingPeriodsData();
    
    // Step 3: Process periods in the specified range
    const periods = Array.from(
      { length: END_PERIOD - START_PERIOD + 1 }, 
      (_, i) => START_PERIOD + i
    );
    
    let totalVolume = 0;
    let totalPayment = 0;
    let totalRecords = 0;
    
    for (const period of periods) {
      const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
      totalVolume += result.volume;
      totalPayment += result.payment;
      totalRecords += result.recordCount;
      
      console.log(`Running total: ${totalRecords} records, ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
      
      // Add delay between periods to avoid API rate limits
      if (period < END_PERIOD) {
        await delay(API_THROTTLE_MS);
      }
    }
    
    // Step 4: Verify the final state for this batch
    const completedPeriods = await getCompletedPeriods();
    
    console.log(`\nStaged Reingest Summary for ${TARGET_DATE}:`);
    console.log(`- Processed periods ${START_PERIOD} to ${END_PERIOD}`);
    console.log(`- Total records: ${totalRecords}`);
    console.log(`- Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`- Total payment: £${totalPayment.toFixed(2)}`);
    console.log(`- Overall completion: ${completedPeriods.size}/48 periods`);
    
    // List the completed and missing periods
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    const missingPeriods = allPeriods.filter(p => !completedPeriods.has(p));
    
    if (missingPeriods.length > 0) {
      console.warn(`Missing periods (${missingPeriods.length}): ${missingPeriods.join(', ')}`);
    } else {
      console.log(`All 48 settlement periods are now complete!`);
    }
    
    console.log(`\nReingest batch completed successfully at ${new Date().toISOString()}`);
    console.log(`\nTo update summaries after all batches are complete, run:`);
    console.log(`npx tsx update_march_22_summaries.ts`);
  } catch (error) {
    console.error('Error during reingest process: ' + error);
    process.exit(1);
  }
}

// Run the script
main()
  .then(() => {
    console.log('Periods processed successfully.');
    process.exit(0);
  })
  .catch(error => {
    console.error('Unhandled error:', error);
    process.exit(1);
  });
/**
 * Reprocess Curtailment Records for March 5, 2025
 * 
 * This script is specifically designed to fix missing records in the curtailment_records table
 * for March 5, 2025. It fetches data from the Elexon API, compares it with existing records,
 * and inserts any missing records without creating duplicates.
 */

import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { fetchBidsOffers, delay } from './server/services/elexon';
import { eq, and, sql } from 'drizzle-orm';
import { ElexonBidOffer } from './server/types/elexon';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// Constants
const TARGET_DATE = '2025-03-05';
const MAX_RETRIES = 3;
const DELAY_BETWEEN_PERIODS = 1000; // 1 second delay between period requests
const MAX_CONCURRENT_PERIODS = 4;
const BMU_MAPPING_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), 'server/data/bmuMapping.json');

let windFarmBmuIds: Set<string> | null = null;
let bmuLeadPartyMap: Map<string, string> | null = null;

// Utility functions
async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    if (windFarmBmuIds === null || bmuLeadPartyMap === null) {
      console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
      const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
      const bmuMapping = JSON.parse(mappingContent);
      console.log(`Loaded ${bmuMapping.length} wind farm BMU IDs`);

      windFarmBmuIds = new Set(
        bmuMapping
          .filter((bmu: any) => bmu.fuelType === "WIND")
          .map((bmu: any) => bmu.elexonBmUnit)
      );

      bmuLeadPartyMap = new Map(
        bmuMapping
          .filter((bmu: any) => bmu.fuelType === "WIND")
          .map((bmu: any) => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
      );

      console.log(`Found ${windFarmBmuIds.size} wind farm BMUs`);
    }

    if (!windFarmBmuIds || !bmuLeadPartyMap) {
      throw new Error('Failed to initialize BMU mappings');
    }

    return windFarmBmuIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

function formatDateTime(date: string, period: number): string {
  // Convert settlement period (1-48) to hour and minute
  const hour = Math.floor((period - 1) / 2);
  const minute = (period - 1) % 2 === 0 ? '00' : '30';
  return `${date}T${hour.toString().padStart(2, '0')}:${minute}:00.000Z`;
}

async function processPeriod(period: number, retryCount = 0): Promise<{
  processed: number;
  added: number;
  skipped: number;
  errors: number;
}> {
  try {
    console.log(`Processing period ${period} for date ${TARGET_DATE}`);
    
    // Make sure wind farm IDs are loaded
    const validWindFarmIds = await loadWindFarmIds();
    
    // Fetch data from Elexon API
    const bidsOffers = await fetchBidsOffers(TARGET_DATE, period);
    
    if (!bidsOffers || bidsOffers.length === 0) {
      console.log(`No data returned for period ${period}`);
      return { processed: 0, added: 0, skipped: 0, errors: 0 };
    }
    
    console.log(`Retrieved ${bidsOffers.length} records for period ${period}`);
    
    // Filter to keep only curtailment records for wind farms (where volume is negative)
    const curtailments = bidsOffers.filter(offer => 
      offer.volume < 0 && 
      validWindFarmIds.has(offer.id)
    );
    
    console.log(`Found ${curtailments.length} wind farm curtailment records for period ${period}`);
    
    let added = 0;
    let skipped = 0;
    let errors = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each curtailment record
    for (const curtailment of curtailments) {
      try {
        // Check if record already exists to avoid duplicates
        const existingRecords = await db.select({ count: sql<number>`count(*)` })
          .from(curtailmentRecords)
          .where(
            and(
              eq(curtailmentRecords.settlementDate, TARGET_DATE),
              eq(curtailmentRecords.settlementPeriod, period),
              eq(curtailmentRecords.farmId, curtailment.id)
            )
          );

        if (existingRecords[0].count > 0) {
          // Record already exists, skip it
          skipped++;
          continue;
        }
        
        const volume = curtailment.volume;
        const payment = Math.abs(volume) * curtailment.originalPrice;
        totalVolume += Math.abs(volume);
        totalPayment += payment;
        
        // Insert new record
        await db.insert(curtailmentRecords).values({
          settlementDate: TARGET_DATE,
          settlementPeriod: period,
          farmId: curtailment.id,
          leadPartyName: bmuLeadPartyMap?.get(curtailment.id) || 'Unknown',
          volume: volume.toString(),
          payment: payment.toString(),
          originalPrice: curtailment.originalPrice.toString(),
          finalPrice: curtailment.finalPrice.toString(),
          soFlag: curtailment.soFlag,
          cadlFlag: curtailment.cadlFlag === null ? false : curtailment.cadlFlag
        });
        
        added++;
      } catch (err) {
        console.error(`Error processing record for period ${period}:`, err);
        errors++;
      }
    }
    
    if (added > 0) {
      console.log(`[${TARGET_DATE} P${period}] Added ${added} records: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    }
    
    return { processed: curtailments.length, added, skipped, errors };
  } catch (err) {
    console.error(`Error fetching data for period ${period}:`, err);
    
    // Retry logic
    if (retryCount < MAX_RETRIES) {
      console.log(`Retrying period ${period} (attempt ${retryCount + 1}/${MAX_RETRIES})...`);
      await delay(2000 * (retryCount + 1)); // Exponential backoff
      return processPeriod(period, retryCount + 1);
    }
    
    return { processed: 0, added: 0, skipped: 0, errors: 1 };
  }
}

async function validateResults(): Promise<{
  isValid: boolean;
  dbRecordCount: number;
  dbTotalVolume: number;
  apiRecordCount: number;
  apiTotalVolume: number;
}> {
  // Get the database statistics after processing
  const dbStats = await db.select({
    recordCount: sql<number>`count(*)`,
    totalVolume: sql<number>`sum(volume)`
  })
  .from(curtailmentRecords)
  .where(eq(curtailmentRecords.settlementDate, new Date(TARGET_DATE)));
  
  // Fetch all data from API to get the total volume
  let apiRecordCount = 0;
  let apiTotalVolume = 0;
  
  // Process each period (1-48) to get full day data
  for (let period = 1; period <= 48; period++) {
    const records = await fetchBidsOffers(TARGET_DATE, period);
    const curtailments = records.filter(offer => offer.volume < 0);
    
    apiRecordCount += curtailments.length;
    apiTotalVolume += curtailments.reduce((sum, record) => sum + record.volume, 0);
    
    // Add a small delay to not overwhelm the API
    await delay(500);
  }
  
  const dbRecordCount = Number(dbStats[0].recordCount);
  const dbTotalVolume = Number(dbStats[0].totalVolume);
  
  // Consider results valid if counts match and volumes are within 0.01% of each other
  const countMatch = dbRecordCount === apiRecordCount;
  const volumeMatch = Math.abs((dbTotalVolume - apiTotalVolume) / apiTotalVolume) < 0.0001;
  
  return {
    isValid: countMatch && volumeMatch,
    dbRecordCount,
    dbTotalVolume,
    apiRecordCount,
    apiTotalVolume
  };
}

async function main() {
  console.log(`🔄 Starting reprocessing of curtailment records for ${TARGET_DATE}`);
  
  // Get initial database stats
  const initialStats = await db.select({
    recordCount: sql<number>`count(*)`,
    totalVolume: sql<number>`sum(volume)`
  })
  .from(curtailmentRecords)
  .where(eq(curtailmentRecords.settlementDate, new Date(TARGET_DATE)));
  
  console.log(`Initial database stats: ${initialStats[0].recordCount} records with total volume ${initialStats[0].totalVolume}`);
  
  // Process all 48 settlement periods for the day
  const results = {
    totalProcessed: 0,
    totalAdded: 0,
    totalSkipped: 0,
    totalErrors: 0
  };
  
  // Process in batches to limit concurrent API calls
  for (let startPeriod = 1; startPeriod <= 48; startPeriod += MAX_CONCURRENT_PERIODS) {
    const endPeriod = Math.min(startPeriod + MAX_CONCURRENT_PERIODS - 1, 48);
    console.log(`Processing periods ${startPeriod}-${endPeriod}...`);
    
    const periodPromises = [];
    for (let period = startPeriod; period <= endPeriod; period++) {
      // Stagger requests to avoid overwhelming the API
      await delay(DELAY_BETWEEN_PERIODS);
      periodPromises.push(processPeriod(period));
    }
    
    const periodResults = await Promise.all(periodPromises);
    
    // Aggregate results
    for (const result of periodResults) {
      results.totalProcessed += result.processed;
      results.totalAdded += result.added;
      results.totalSkipped += result.skipped;
      results.totalErrors += result.errors;
    }
    
    console.log(`Completed periods ${startPeriod}-${endPeriod}`);
  }
  
  console.log("\n===== Processing Summary =====");
  console.log(`Total records processed: ${results.totalProcessed}`);
  console.log(`Total records added: ${results.totalAdded}`);
  console.log(`Total records skipped (already exist): ${results.totalSkipped}`);
  console.log(`Total errors: ${results.totalErrors}`);
  
  // Validate the results
  console.log("\n===== Validation =====");
  const validation = await validateResults();
  
  console.log(`Database records: ${validation.dbRecordCount}`);
  console.log(`API records: ${validation.apiRecordCount}`);
  console.log(`Database total volume: ${validation.dbTotalVolume}`);
  console.log(`API total volume: ${validation.apiTotalVolume}`);
  console.log(`Validation result: ${validation.isValid ? '✅ PASSED' : '❌ FAILED'}`);
  
  console.log("\n===== Final Status =====");
  if (validation.isValid) {
    console.log("✅ SUCCESS: Data integrity verified. Database records match the Elexon API data.");
  } else {
    console.log("❌ WARNING: Data integrity check failed. Database records do not match the Elexon API data.");
    console.log("Please review the logs and consider running the script again.");
  }
  
  // After data is fixed, we should also run the reconciliation
  console.log("\nNext steps:");
  console.log("1. Run the reconciliation for 2025-03-05 to ensure Bitcoin calculations are updated");
  console.log("   npx tsx server/services/historicalReconciliation.ts date 2025-03-05");
}

// Run the main function
main()
  .then(() => {
    console.log("Reprocessing complete");
    process.exit(0);
  })
  .catch((err) => {
    console.error("Error during reprocessing:", err);
    process.exit(1);
  });
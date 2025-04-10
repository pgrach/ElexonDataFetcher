/**
 * Data Reprocessing Script for Any Date
 * 
 * This script performs a complete reprocessing of data for a specified date:
 * 1. Deletes existing records from the curtailment_records table
 * 2. Removes associated Bitcoin calculations 
 * 3. Fetches fresh data from Elexon API for all 48 settlement periods
 * 4. Recalculates all summaries and Bitcoin mining potential
 * 
 * Usage: npx tsx scripts/reprocessAnyDate.ts YYYY-MM-DD
 */

import { db } from "../db";
import { curtailmentRecords, historicalBitcoinCalculations } from "../db/schema";
import { fetchBidsOffers } from "../server/services/elexon";
import { processDailyCurtailment } from "../server/services/curtailment_enhanced";
import { processSingleDay } from "../server/services/bitcoinService";
import { eq, and } from "drizzle-orm";
import { minerModels } from "../server/types/bitcoin";
import { logger } from "../server/utils/logger";

// Get target date from command line arguments
const TARGET_DATE = process.argv[2];

// Validate date format
if (!TARGET_DATE || !/^\d{4}-\d{2}-\d{2}$/.test(TARGET_DATE)) {
  console.error('Please provide a valid date in YYYY-MM-DD format');
  console.error('Usage: npx tsx scripts/reprocessAnyDate.ts YYYY-MM-DD');
  process.exit(1);
}

// Delay between API requests to stay within rate limits
const API_REQUEST_DELAY = 250; // ms

/**
 * Utility function to pause execution
 */
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Delete existing records for the target date
 */
async function deleteExistingRecords(): Promise<{ curtailmentCount: number, bitcoinCalculationsCount: number }> {
  logger.info(`Deleting existing records for ${TARGET_DATE}...`);
  
  // 1. Delete Bitcoin calculations first (foreign key dependency)
  const bitcoinDeleteResult = await db.delete(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
    .returning();
  
  const bitcoinCalculationsCount = bitcoinDeleteResult.length;
  logger.info(`Deleted ${bitcoinCalculationsCount} Bitcoin calculations for ${TARGET_DATE}`);
  
  // 2. Delete curtailment records
  const curtailmentDeleteResult = await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .returning();
  
  const curtailmentCount = curtailmentDeleteResult.length;
  logger.info(`Deleted ${curtailmentCount} curtailment records for ${TARGET_DATE}`);
  
  return { curtailmentCount, bitcoinCalculationsCount };
}

/**
 * Fetch fresh data from Elexon API for all 48 periods
 */
async function fetchFreshData(): Promise<{ totalRecords: number, totalVolume: number, totalPayment: number }> {
  logger.info(`Fetching fresh data from Elexon API for ${TARGET_DATE}...`);
  
  let stats = {
    totalRecords: 0,
    totalVolume: 0,
    totalPayment: 0
  };
  
  // Process all 48 settlement periods
  for (let period = 1; period <= 48; period++) {
    logger.info(`Fetching data for ${TARGET_DATE} settlement period ${period}...`);
    
    try {
      // Fetch data from Elexon API
      const records = await fetchBidsOffers(TARGET_DATE, period);
      
      if (records && Array.isArray(records)) {
        // We only want curtailment records (negative volume and SO or CADL flag is true)
        const validRecords = records.filter(record => 
          record.volume < 0 && (record.soFlag || record.cadlFlag)
        );
        
        if (validRecords.length > 0) {
          // Insert the valid records into the database
          const insertValues = validRecords.map(record => ({
            settlementDate: TARGET_DATE,
            settlementPeriod: period,
            bmUnit: record.bmUnit || record.id,
            farmId: record.id,
            leadPartyName: record.leadPartyName || '',
            volume: record.volume.toString(),
            originalPrice: record.originalPrice.toString(),
            finalPrice: record.finalPrice.toString(),
            // payment = volume * originalPrice (negative values)
            payment: (record.volume * record.originalPrice).toString(),
            soFlag: record.soFlag,
            cadlFlag: !!record.cadlFlag
          }));
          
          const insertResult = await db.insert(curtailmentRecords).values(insertValues);
          
          logger.info(`Inserted ${validRecords.length} records for period ${period}`);
          
          // Update stats
          stats.totalRecords += validRecords.length;
          stats.totalVolume += validRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
          stats.totalPayment += validRecords.reduce((sum, r) => sum + Math.abs(r.volume * r.originalPrice), 0);
        } else {
          logger.info(`No valid curtailment records found for period ${period}`);
        }
      }
    } catch (error) {
      logger.error(`Error processing period ${period}:`, error);
    }
    
    // Add delay to respect API rate limits
    await delay(API_REQUEST_DELAY);
  }
  
  logger.info(`Completed fetching data for ${TARGET_DATE}:`);
  logger.info(`- Total records: ${stats.totalRecords}`);
  logger.info(`- Total volume: ${stats.totalVolume.toFixed(2)} MWh`);
  logger.info(`- Total payment: £${stats.totalPayment.toFixed(2)}`);
  
  return stats;
}

/**
 * Update daily summaries for the target date
 */
async function updateDailySummaries(): Promise<void> {
  logger.info(`Updating daily summaries for ${TARGET_DATE}...`);
  
  try {
    await processDailyCurtailment(TARGET_DATE);
    logger.info(`Successfully updated daily summaries for ${TARGET_DATE}`);
  } catch (error) {
    logger.error(`Error updating daily summaries:`, error);
    throw error;
  }
}

/**
 * Update Bitcoin calculations for the target date
 */
async function updateBitcoinCalculations(): Promise<void> {
  logger.info(`Updating Bitcoin calculations for ${TARGET_DATE}...`);
  
  // Process each miner model
  const minerModelList = Object.keys(minerModels);
  
  for (const minerModel of minerModelList) {
    logger.info(`Processing Bitcoin calculations for ${minerModel}...`);
    
    try {
      await processSingleDay(TARGET_DATE, minerModel);
      logger.info(`Successfully processed Bitcoin calculations for ${minerModel}`);
    } catch (error) {
      logger.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
    }
  }
}

/**
 * Main function to run the reprocessing
 */
async function main() {
  logger.info(`Starting reprocessing of data for ${TARGET_DATE}...`);
  
  try {
    // Step 1: Delete existing records
    const deleteStats = await deleteExistingRecords();
    
    // Step 2: Fetch fresh data from API
    const fetchStats = await fetchFreshData();
    
    // Step 3: Update daily summaries which will cascade to monthly and yearly
    await updateDailySummaries();
    
    // Step 4: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    logger.info(`===== Reprocessing completed successfully =====`);
    logger.info(`Summary of changes:`);
    logger.info(`- Deleted: ${deleteStats.curtailmentCount} curtailment records, ${deleteStats.bitcoinCalculationsCount} Bitcoin calculations`);
    logger.info(`- Added: ${fetchStats.totalRecords} curtailment records, ${fetchStats.totalVolume.toFixed(2)} MWh, £${fetchStats.totalPayment.toFixed(2)}`);
    logger.info(`- Update completed: Daily, monthly, and yearly summaries updated`);
    logger.info(`- Update completed: Bitcoin calculations for all miner models updated`);
    
  } catch (error) {
    logger.error(`Reprocessing failed:`, error);
  }
}

// Execute the script
main().catch(console.error);
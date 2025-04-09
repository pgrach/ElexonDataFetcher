/**
 * Update Records for 2025-04-01
 * 
 * This script ingests curtailment data from Elexon API for 2025-04-01 
 * and cascades updates to all dependent tables (summaries and Bitcoin calculations).
 */

import { db } from "@db";
import { reprocessDay } from "../services/historicalReconciliation";
import { processSingleDay } from "../services/bitcoinService";
import { processDailyCurtailment } from "../services/curtailment_enhanced";
import { format } from "date-fns";
import { calculateMonthlyBitcoinSummary, manualUpdateYearlyBitcoinSummary } from "../services/bitcoinService";
import { logger } from "../utils/logger";

// Constants
const TARGET_DATE = "2025-04-01";
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

async function updateCurtailmentRecords(): Promise<void> {
  try {
    logger.info(`\n=== Starting data update for ${TARGET_DATE} ===\n`);
    
    // Step 1: Process curtailment data using the enhanced service
    logger.info(`Processing curtailment data from Elexon API...`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Step 2: Update Bitcoin calculations for all miner models
    for (const minerModel of MINER_MODELS) {
      logger.info(`Processing Bitcoin calculations for ${minerModel}...`);
      await processSingleDay(TARGET_DATE, minerModel);
    }

    // Step 3: Calculate monthly Bitcoin summaries
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM format
    for (const minerModel of MINER_MODELS) {
      logger.info(`Updating monthly Bitcoin summary for ${yearMonth} with ${minerModel}...`);
      await calculateMonthlyBitcoinSummary(yearMonth, minerModel);
    }

    // Step 4: Update yearly Bitcoin summaries
    const year = TARGET_DATE.substring(0, 4); // YYYY format
    logger.info(`Updating yearly Bitcoin summary for ${year}...`);
    await manualUpdateYearlyBitcoinSummary(year);
    
    logger.info(`\n=== Data update for ${TARGET_DATE} completed successfully ===\n`);
  } catch (error) {
    logger.error(`Error updating data for ${TARGET_DATE}:`, error);
    throw error;
  }
}

// Alternative method using reconciliation service
async function updateRecordsUsingReconciliation(): Promise<void> {
  try {
    logger.info(`\n=== Starting data reprocessing for ${TARGET_DATE} using reconciliation service ===\n`);
    
    // Reprocess the day using the reconciliation service
    // This handles curtailment processing, Bitcoin calculations and summary updates
    await reprocessDay(TARGET_DATE);
    
    logger.info(`\n=== Data reprocessing for ${TARGET_DATE} completed successfully ===\n`);
  } catch (error) {
    logger.error(`Error reprocessing data for ${TARGET_DATE}:`, error);
    throw error;
  }
}

async function main(): Promise<void> {
  try {
    const startTime = new Date();
    logger.info(`Starting update process at ${format(startTime, 'yyyy-MM-dd HH:mm:ss')}`);
    
    // Choose which method to use:
    // 1. Detailed step-by-step approach:
    // await updateCurtailmentRecords();
    
    // 2. Using reconciliation service (recommended):
    await updateRecordsUsingReconciliation();
    
    const endTime = new Date();
    const processingTime = (endTime.getTime() - startTime.getTime()) / 1000;
    logger.info(`Update process completed at ${format(endTime, 'yyyy-MM-dd HH:mm:ss')}`);
    logger.info(`Total processing time: ${processingTime.toFixed(2)} seconds`);
    
    process.exit(0);
  } catch (error) {
    logger.error("Update process failed:", error);
    process.exit(1);
  }
}

// Execute the main function
main();
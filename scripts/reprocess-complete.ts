/**
 * Complete Data Reprocessing Script
 * 
 * This script provides a flexible way to reprocess data for specific dates.
 * It handles all aspects of the data pipeline:
 * 1. Fetching and processing curtailment data from Elexon
 * 2. Fetching and processing wind generation data
 * 3. Calculating Bitcoin mining potential
 * 4. Updating all summary tables
 * 
 * Usage:
 *   npx tsx scripts/reprocess-complete.ts --date 2025-05-08
 *   npx tsx scripts/reprocess-complete.ts --start 2025-05-01 --end 2025-05-08
 *   npx tsx scripts/reprocess-complete.ts --date 2025-05-08 --force
 *   npx tsx scripts/reprocess-complete.ts --start 2025-05-01 --end 2025-05-08 --skip-wind
 * 
 * Options:
 *   --date: Single date to process (YYYY-MM-DD)
 *   --start: Start date for range processing (YYYY-MM-DD)
 *   --end: End date for range processing (YYYY-MM-DD)
 *   --force: Force reprocessing even if data exists and appears correct
 *   --skip-wind: Skip wind data processing
 *   --skip-bitcoin: Skip Bitcoin calculations
 *   --miners: Comma-separated list of miner models to process (default: all)
 */

import { db } from "../db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations,
  dailySummaries,
  windGenerationData
} from "../db/schema";
import { processDailyCurtailment } from "../server/services/curtailment_enhanced";
import { processWindDataForDate } from "../server/services/windDataUpdater";
import { processSingleDay } from "../server/services/bitcoinService";
import { reconcileDay } from "../server/services/historicalReconciliation";
import { and, eq, between, sql } from "drizzle-orm";
import { format, parse, addDays, isBefore, isValid, parseISO } from "date-fns";
import pLimit from "p-limit";

// Constants
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
const MAX_CONCURRENT_DATES = 3; // Process max 3 dates in parallel to avoid rate limits
const DATE_FORMAT = "yyyy-MM-dd";

// Command line arguments
const args = process.argv.slice(2);
let singleDate: string | null = null;
let startDate: string | null = null;
let endDate: string | null = null;
let forceReprocess = false;
let skipWind = false;
let skipBitcoin = false;
let minerModels = MINER_MODELS;

// Parse command line arguments
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--date" && i + 1 < args.length) {
    singleDate = args[i + 1];
    i++;
  } else if (args[i] === "--start" && i + 1 < args.length) {
    startDate = args[i + 1];
    i++;
  } else if (args[i] === "--end" && i + 1 < args.length) {
    endDate = args[i + 1];
    i++;
  } else if (args[i] === "--force") {
    forceReprocess = true;
  } else if (args[i] === "--skip-wind") {
    skipWind = true;
  } else if (args[i] === "--skip-bitcoin") {
    skipBitcoin = true;
  } else if (args[i] === "--miners" && i + 1 < args.length) {
    minerModels = args[i + 1].split(",");
    i++;
  }
}

/**
 * Validate a date string is in YYYY-MM-DD format
 */
function isValidDateFormat(dateStr: string): boolean {
  if (!dateStr) return false;
  
  // Check basic format with regex
  if (!dateStr.match(/^\d{4}-\d{2}-\d{2}$/)) {
    return false;
  }
  
  // Validate using date-fns
  const parsed = parseISO(dateStr);
  return isValid(parsed);
}

/**
 * Get all dates in a range as formatted strings
 */
function getDatesInRange(start: string, end: string): string[] {
  const dates: string[] = [];
  
  const startDate = parseISO(start);
  const endDate = parseISO(end);
  
  let currentDate = startDate;
  
  while (isBefore(currentDate, endDate) || format(currentDate, DATE_FORMAT) === end) {
    dates.push(format(currentDate, DATE_FORMAT));
    currentDate = addDays(currentDate, 1);
  }
  
  return dates;
}

/**
 * Check if a date already has curtailment data
 */
async function hasCurtailmentData(date: string): Promise<boolean> {
  const result = await db
    .select({
      count: sql<number>`COUNT(*)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  return result[0].count > 0;
}

/**
 * Check if a date already has wind generation data
 */
async function hasWindData(date: string): Promise<boolean> {
  const result = await db
    .select({
      count: sql<number>`COUNT(*)`
    })
    .from(windGenerationData)
    .where(eq(windGenerationData.settlementDate, date));
  
  return result[0].count > 0;
}

/**
 * Check if Bitcoin calculations exist for a date and miner model
 */
async function hasBitcoinCalculations(date: string, minerModel: string): Promise<boolean> {
  const result = await db
    .select({
      count: sql<number>`COUNT(*)`
    })
    .from(historicalBitcoinCalculations)
    .where(and(
      eq(historicalBitcoinCalculations.settlementDate, date),
      eq(historicalBitcoinCalculations.minerModel, minerModel)
    ));
  
  return result[0].count > 0;
}

/**
 * Process a single date's worth of data
 */
async function processDate(date: string): Promise<void> {
  console.log(`\n=== Processing data for ${date} ===`);
  
  try {
    // Step 1: Process curtailment data
    let skipCurtailment = false;
    if (!forceReprocess) {
      const hasData = await hasCurtailmentData(date);
      skipCurtailment = hasData;
    }
    
    if (skipCurtailment) {
      console.log(`[${date}] Curtailment data exists - skipping (use --force to override)`);
    } else {
      console.log(`[${date}] Processing curtailment data...`);
      
      // First delete existing records
      await db.delete(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date));
      
      // Process curtailment data for this date
      await processDailyCurtailment(date);
      
      // Verify the data was processed
      const curtailmentCount = await db
        .select({
          count: sql<number>`COUNT(*)`,
          periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
          totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
          totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date));
      
      console.log(`[${date}] Curtailment processing complete:`, {
        records: curtailmentCount[0].count,
        periods: curtailmentCount[0].periodCount,
        volume: Number(curtailmentCount[0].totalVolume || 0).toFixed(2),
        payment: Number(curtailmentCount[0].totalPayment || 0).toFixed(2)
      });
    }
    
    // Step 2: Process wind generation data (if not skipped)
    if (skipWind) {
      console.log(`[${date}] Skipping wind data processing (--skip-wind)`);
    } else {
      let skipWindProcessing = false;
      if (!forceReprocess) {
        const hasData = await hasWindData(date);
        skipWindProcessing = hasData;
      }
      
      if (skipWindProcessing) {
        console.log(`[${date}] Wind generation data exists - skipping (use --force to override)`);
      } else {
        console.log(`[${date}] Processing wind generation data...`);
        const windDataProcessed = await processWindDataForDate(date);
        
        if (windDataProcessed) {
          console.log(`[${date}] Wind generation data processing complete`);
        } else {
          console.log(`[${date}] No wind generation data found or processing failed`);
        }
      }
    }
    
    // Step 3: Process Bitcoin calculations (if not skipped)
    if (skipBitcoin) {
      console.log(`[${date}] Skipping Bitcoin calculations (--skip-bitcoin)`);
    } else {
      console.log(`[${date}] Processing Bitcoin calculations for ${minerModels.length} miner models...`);
      
      // Process each miner model
      for (const minerModel of minerModels) {
        let skipBitcoinProcessing = false;
        if (!forceReprocess) {
          const hasData = await hasBitcoinCalculations(date, minerModel);
          skipBitcoinProcessing = hasData;
        }
        
        if (skipBitcoinProcessing) {
          console.log(`[${date}] Bitcoin calculations for ${minerModel} exist - skipping (use --force to override)`);
        } else {
          console.log(`[${date}] Processing Bitcoin calculations for ${minerModel}...`);
          
          // First delete existing records for this date and model
          await db.delete(historicalBitcoinCalculations)
            .where(and(
              eq(historicalBitcoinCalculations.settlementDate, date),
              eq(historicalBitcoinCalculations.minerModel, minerModel)
            ));
          
          // Process Bitcoin calculations for this date and model
          await processSingleDay(date, minerModel);
          
          // Verify the data was processed
          const bitcoinCount = await db
            .select({
              count: sql<number>`COUNT(*)`,
              totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
            })
            .from(historicalBitcoinCalculations)
            .where(and(
              eq(historicalBitcoinCalculations.settlementDate, date),
              eq(historicalBitcoinCalculations.minerModel, minerModel)
            ));
          
          console.log(`[${date}] Bitcoin processing complete for ${minerModel}:`, {
            records: bitcoinCount[0].count,
            bitcoinMined: Number(bitcoinCount[0].totalBitcoin || 0).toFixed(8)
          });
        }
      }
    }
    
    // Step 4: Verify the daily summary was updated
    const dailySummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });
    
    if (dailySummary) {
      console.log(`[${date}] Daily summary:`, {
        energy: Number(dailySummary.totalCurtailedEnergy || 0).toFixed(2),
        payment: Number(dailySummary.totalPayment || 0).toFixed(2),
        windGeneration: Number(dailySummary.totalWindGeneration || 0).toFixed(2)
      });
    } else {
      console.log(`[${date}] No daily summary found`);
    }
    
    console.log(`\n=== Completed processing for ${date} ===`);
  } catch (error) {
    console.error(`[${date}] Error during processing:`, error);
    throw error;
  }
}

/**
 * Trigger a reconciliation for a date using the existing reconciliation system
 */
async function triggerReconciliation(date: string): Promise<void> {
  console.log(`\n=== Triggering reconciliation for ${date} ===`);
  
  try {
    await reconcileDay(date);
    console.log(`=== Reconciliation for ${date} complete ===`);
  } catch (error) {
    console.error(`Error during reconciliation for ${date}:`, error);
    throw error;
  }
}

/**
 * Main function to run the reprocessing
 */
async function runReprocessing() {
  console.log("Bitcoin Mining Analytics - Data Reprocessing Tool");
  console.log("================================================\n");
  
  try {
    // Validate inputs
    if (singleDate && (!isValidDateFormat(singleDate))) {
      console.error("Invalid date format. Use YYYY-MM-DD format for --date parameter.");
      process.exit(1);
    }
    
    if ((startDate || endDate) && (!isValidDateFormat(startDate!) || !isValidDateFormat(endDate!))) {
      console.error("Invalid date format. Use YYYY-MM-DD format for --start and --end parameters.");
      process.exit(1);
    }
    
    if (!singleDate && (!startDate || !endDate)) {
      console.error("Please provide either a single date with --date or a date range with --start and --end.");
      console.error("Example: npx tsx scripts/reprocess-complete.ts --date 2025-05-08");
      console.error("Example: npx tsx scripts/reprocess-complete.ts --start 2025-05-01 --end 2025-05-08");
      process.exit(1);
    }
    
    // Set up concurrency limit to avoid overwhelming the APIs
    const limit = pLimit(MAX_CONCURRENT_DATES);
    const processingPromises = [];
    
    // Process a single date
    if (singleDate) {
      console.log(`Processing single date: ${singleDate}`);
      console.log(`Force reprocess: ${forceReprocess ? 'Yes' : 'No'}`);
      console.log(`Skip wind processing: ${skipWind ? 'Yes' : 'No'}`);
      console.log(`Skip Bitcoin processing: ${skipBitcoin ? 'Yes' : 'No'}`);
      console.log(`Miner models: ${minerModels.join(', ')}`);
      
      if (forceReprocess) {
        // Use manual processing for forced reprocessing
        processingPromises.push(limit(() => processDate(singleDate!)));
      } else {
        // Use the reconciliation system for normal processing
        processingPromises.push(limit(() => triggerReconciliation(singleDate!)));
      }
    } 
    // Process a date range
    else if (startDate && endDate) {
      console.log(`Processing date range: ${startDate} to ${endDate}`);
      console.log(`Force reprocess: ${forceReprocess ? 'Yes' : 'No'}`);
      console.log(`Skip wind processing: ${skipWind ? 'Yes' : 'No'}`);
      console.log(`Skip Bitcoin processing: ${skipBitcoin ? 'Yes' : 'No'}`);
      console.log(`Miner models: ${minerModels.join(', ')}`);
      
      const dates = getDatesInRange(startDate, endDate);
      console.log(`Total dates to process: ${dates.length}`);
      
      for (const date of dates) {
        if (forceReprocess) {
          // Use manual processing for forced reprocessing
          processingPromises.push(limit(() => processDate(date)));
        } else {
          // Use the reconciliation system for normal processing
          processingPromises.push(limit(() => triggerReconciliation(date)));
        }
      }
    }
    
    console.log("\nStarting data processing...");
    console.log("This may take some time depending on the number of dates and API rate limits.");
    
    const startTime = Date.now();
    await Promise.all(processingPromises);
    const endTime = Date.now();
    
    const duration = (endTime - startTime) / 1000; // Convert to seconds
    
    console.log("\n=== Reprocessing Complete ===");
    console.log(`Total execution time: ${duration.toFixed(1)} seconds`);
    console.log("All data has been processed successfully.");
    
  } catch (error) {
    console.error("Error during reprocessing:", error);
    process.exit(1);
  }
}

// Run the reprocessing
runReprocessing();
/**
 * Reprocess Data for Date Range
 * 
 * This script allows reprocessing data for a range of dates.
 * It processes curtailment data, wind generation data, and
 * Bitcoin calculations for all dates in the specified range.
 * 
 * Usage:
 *   npx tsx scripts/reprocess-date-range.ts --start 2025-05-01 --end 2025-05-08
 *   npx tsx scripts/reprocess-date-range.ts --start 2025-05-01 --end 2025-05-08 --skip-wind
 *   npx tsx scripts/reprocess-date-range.ts --start 2025-05-01 --end 2025-05-08 --miners S19J_PRO
 */

import { db } from "../db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations,
  dailySummaries
} from "../db/schema";
import { processDailyCurtailment } from "../server/services/curtailment_enhanced";
import { processWindDataForDate } from "../server/services/windDataUpdater";
import { processSingleDay } from "../server/services/bitcoinService";
import { eq, and, sql } from "drizzle-orm";
import { format, parse, addDays, isBefore, isValid, parseISO } from "date-fns";
import pLimit from "p-limit";

// Constants
const DEFAULT_MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
const MAX_CONCURRENT_DATES = 3; // Process max 3 dates in parallel to avoid rate limits
const DATE_FORMAT = "yyyy-MM-dd";

// Command line arguments
const args = process.argv.slice(2);
let startDate: string | null = null;
let endDate: string | null = null;
let skipWind = false;
let minerModels = DEFAULT_MINER_MODELS;

// Parse command line arguments
for (let i = 0; i < args.length; i++) {
  if (args[i] === "--start" && i + 1 < args.length) {
    startDate = args[i + 1];
    i++;
  } else if (args[i] === "--end" && i + 1 < args.length) {
    endDate = args[i + 1];
    i++;
  } else if (args[i] === "--skip-wind") {
    skipWind = true;
  } else if (args[i] === "--miners" && i + 1 < args.length) {
    minerModels = args[i + 1].split(",");
    i++;
  }
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
 * Process a single date's worth of data
 */
async function processDate(date: string): Promise<void> {
  console.log(`\n=== Processing data for ${date} ===`);
  
  try {
    // Step 1: Clear existing data for the target date
    console.log(`[${date}] Clearing existing curtailment records...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    console.log(`[${date}] Clearing existing Bitcoin calculations...`);
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, date));
    
    // Step 2: Reprocess curtailment data
    console.log(`[${date}] Reprocessing curtailment data...`);
    try {
      await processDailyCurtailment(date);
      
      // Verify curtailment data was processed
      const curtailmentStats = await db
        .select({
          count: sql<number>`COUNT(*)`,
          periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
          totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
          totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date));
      
      console.log(`[${date}] Curtailment data processed:`, {
        records: curtailmentStats[0].count,
        periods: curtailmentStats[0].periodCount,
        volume: Number(curtailmentStats[0].totalVolume || 0).toFixed(2) + ' MWh',
        payment: '£' + Number(curtailmentStats[0].totalPayment || 0).toFixed(2)
      });
    } catch (error) {
      console.error(`[${date}] Error processing curtailment data:`, error);
      throw error;
    }
    
    // Step 3: Process wind generation data (if not skipped)
    if (skipWind) {
      console.log(`[${date}] Skipping wind data processing (--skip-wind)`);
    } else {
      console.log(`[${date}] Processing wind generation data...`);
      try {
        const windDataProcessed = await processWindDataForDate(date);
        if (windDataProcessed) {
          console.log(`[${date}] Wind generation data processed successfully`);
        } else {
          console.log(`[${date}] No wind generation data found`);
        }
      } catch (error) {
        console.error(`[${date}] Error processing wind generation data:`, error);
        // Continue even if wind data processing fails
        console.log(`[${date}] Continuing with Bitcoin calculations despite wind data error`);
      }
    }
    
    // Step 4: Process Bitcoin calculations for each miner model
    console.log(`[${date}] Processing Bitcoin calculations...`);
    for (const minerModel of minerModels) {
      try {
        console.log(`[${date}] Processing Bitcoin calculations for ${minerModel}...`);
        await processSingleDay(date, minerModel);
        
        // Verify Bitcoin calculations were processed
        const bitcoinStats = await db
          .select({
            count: sql<number>`COUNT(*)`,
            totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
          })
          .from(historicalBitcoinCalculations)
          .where(and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          ));
        
        console.log(`[${date}] Bitcoin calculations for ${minerModel}:`, {
          records: bitcoinStats[0].count,
          bitcoinMined: Number(bitcoinStats[0].totalBitcoin || 0).toFixed(8) + ' BTC'
        });
      } catch (error) {
        console.error(`[${date}] Error processing Bitcoin calculations for ${minerModel}:`, error);
        // Continue with other miner models even if one fails
      }
    }
    
    // Step 5: Verify daily summary was updated
    const dailySummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });
    
    if (dailySummary) {
      console.log(`[${date}] Daily summary updated:`, {
        energy: Number(dailySummary.totalCurtailedEnergy || 0).toFixed(2) + ' MWh',
        payment: '£' + Number(dailySummary.totalPayment || 0).toFixed(2),
        windGeneration: Number(dailySummary.totalWindGeneration || 0).toFixed(2) + ' MWh'
      });
    } else {
      console.log(`[${date}] Warning: No daily summary found`);
    }
    
    console.log(`=== Completed processing for ${date} ===`);
  } catch (error) {
    console.error(`[${date}] Error during processing:`, error);
    throw error;
  }
}

/**
 * Main function to run the reprocessing
 */
async function runDateRangeReprocessing() {
  console.log("Bitcoin Mining Analytics - Date Range Reprocessing Tool");
  console.log("====================================================\n");
  
  try {
    // Validate inputs
    if (!startDate || !endDate) {
      console.error("Please provide both start and end dates.");
      console.error("Example: npx tsx scripts/reprocess-date-range.ts --start 2025-05-01 --end 2025-05-08");
      process.exit(1);
    }
    
    // Validate date formats
    const startDateObj = parseISO(startDate);
    const endDateObj = parseISO(endDate);
    
    if (!isValid(startDateObj) || !isValid(endDateObj)) {
      console.error("Invalid date format. Use YYYY-MM-DD format for --start and --end parameters.");
      process.exit(1);
    }
    
    if (startDateObj > endDateObj) {
      console.error("Start date must be before or equal to end date.");
      process.exit(1);
    }
    
    // Get all dates in the range
    const dates = getDatesInRange(startDate, endDate);
    
    console.log(`Processing date range: ${startDate} to ${endDate}`);
    console.log(`Total dates to process: ${dates.length}`);
    console.log(`Skip wind processing: ${skipWind ? 'Yes' : 'No'}`);
    console.log(`Miner models: ${minerModels.join(', ')}`);
    console.log(`Max concurrent dates: ${MAX_CONCURRENT_DATES}`);
    
    // Set up concurrency limit to avoid overwhelming the APIs
    const limit = pLimit(MAX_CONCURRENT_DATES);
    const processingPromises = dates.map(date => 
      limit(() => processDate(date))
    );
    
    console.log("\nStarting data processing...");
    console.log("This may take some time depending on the number of dates and API rate limits.");
    
    const startTime = Date.now();
    await Promise.all(processingPromises);
    const endTime = Date.now();
    
    const duration = (endTime - startTime) / 1000 / 60; // Convert to minutes
    
    console.log("\n=== Reprocessing Complete ===");
    console.log(`Processed ${dates.length} dates from ${startDate} to ${endDate}`);
    console.log(`Total execution time: ${duration.toFixed(1)} minutes`);
    console.log("All data has been processed successfully.");
    
  } catch (error) {
    console.error("Error during reprocessing:", error);
    process.exit(1);
  }
}

// Run the reprocessing
runDateRangeReprocessing();
/**
 * This script identifies and fixes all missing or incomplete Bitcoin calculations.
 * It analyzes the entire dataset, prioritizes missing calculations, and processes them in batches.
 */

import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { sql, and, eq, between, desc } from "drizzle-orm";
import { format, parseISO, eachMonthOfInterval, addDays, subDays, isBefore } from "date-fns";
import pLimit from "p-limit";
import fs from "fs/promises";
import path from "path";
import { processSingleDay } from "../server/services/bitcoinService";
import { minerModels } from "../server/types/bitcoin";

// Configuration
const MINER_MODEL_LIST = Object.keys(minerModels);
const DEFAULT_MINER_MODEL = "S19J_PRO";
const CONCURRENCY_LIMIT = 2; // Limit concurrent processing to avoid resource exhaustion
const BATCH_SIZE = 5; // Number of dates to process in each batch
const DELAY_BETWEEN_BATCHES = 5000; // 5 seconds delay between batches
const LOG_FILE = "bitcoin_calculation_fix_log.json";

// Define interfaces for our data structures
interface MonthSummary {
  yearMonth: string;
  curtailmentCount: number;
  bitcoinCount: number | null;
  status: 'Missing' | 'Incomplete' | 'Complete';
  priority: number;
}

interface DateSummary {
  date: string;
  curtailmentCount: number;
  bitcoinCount: number | null;
  status: 'Missing' | 'Incomplete' | 'Complete';
  priority: number;
}

interface FixResult {
  date: string;
  status: 'Success' | 'Failure';
  message: string;
  durationMs: number;
}

interface ProgressLog {
  startTime: string;
  lastUpdated: string;
  processedDates: string[];
  successfulDates: string[];
  failedDates: Record<string, string>;
  currentBatch: string[];
}

/**
 * Get a summary of all months with their calculation status
 */
async function getMonthSummary(): Promise<MonthSummary[]> {
  const curtailmentMonths = await db.execute(sql`
    SELECT 
      TO_CHAR(settlement_date, 'YYYY-MM') as year_month,
      COUNT(*) as curtailment_count
    FROM curtailment_records
    GROUP BY TO_CHAR(settlement_date, 'YYYY-MM')
    ORDER BY year_month
  `);

  const bitcoinMonths = await db.execute(sql`
    SELECT 
      TO_CHAR(settlement_date, 'YYYY-MM') as year_month,
      COUNT(*) as bitcoin_count
    FROM historical_bitcoin_calculations
    WHERE miner_model = ${DEFAULT_MINER_MODEL}
    GROUP BY TO_CHAR(settlement_date, 'YYYY-MM')
    ORDER BY year_month
  `);

  // Convert DB results to a map for easier lookup
  const bitcoinMap = new Map<string, number>();
  for (const row of bitcoinMonths.rows) {
    bitcoinMap.set(row.year_month, parseInt(row.bitcoin_count));
  }

  // Create the combined summary with status
  const summary: MonthSummary[] = [];
  for (const row of curtailmentMonths.rows) {
    const yearMonth = row.year_month;
    const curtailmentCount = parseInt(row.curtailment_count);
    const bitcoinCount = bitcoinMap.get(yearMonth) || null;
    
    let status: 'Missing' | 'Incomplete' | 'Complete';
    let priority: number;
    
    if (bitcoinCount === null) {
      status = 'Missing';
      priority = 1; // Highest priority
    } else if (bitcoinCount < curtailmentCount) {
      status = 'Incomplete';
      priority = 2;
    } else {
      status = 'Complete';
      priority = 3; // Lowest priority
    }
    
    summary.push({
      yearMonth,
      curtailmentCount,
      bitcoinCount,
      status,
      priority
    });
  }
  
  // Sort by priority (highest first) and then by year month
  return summary.sort((a, b) => 
    a.priority === b.priority 
      ? a.yearMonth.localeCompare(b.yearMonth)
      : a.priority - b.priority
  );
}

/**
 * Get a summary of specific dates that need to be fixed
 */
async function getDatesThatNeedFixing(limit: number = 1000): Promise<DateSummary[]> {
  const result = await db.execute(sql`
    WITH curtailment_dates AS (
      SELECT 
        settlement_date,
        COUNT(*) as curtailment_count
      FROM curtailment_records
      GROUP BY settlement_date
    ),
    bitcoin_dates AS (
      SELECT 
        settlement_date,
        COUNT(*) as bitcoin_count
      FROM historical_bitcoin_calculations
      WHERE miner_model = ${DEFAULT_MINER_MODEL}
      GROUP BY settlement_date
    )
    SELECT 
      c.settlement_date::text as date,
      c.curtailment_count,
      b.bitcoin_count,
      CASE
        WHEN b.bitcoin_count IS NULL THEN 'Missing'
        WHEN c.curtailment_count > b.bitcoin_count THEN 'Incomplete'
        ELSE 'Complete'
      END as status
    FROM curtailment_dates c
    LEFT JOIN bitcoin_dates b ON c.settlement_date = b.settlement_date
    WHERE b.bitcoin_count IS NULL OR c.curtailment_count > b.bitcoin_count
    ORDER BY 
      CASE
        WHEN b.bitcoin_count IS NULL THEN 1
        WHEN c.curtailment_count > b.bitcoin_count THEN 2
        ELSE 3
      END,
      c.settlement_date
    LIMIT ${limit}
  `);

  return result.rows.map(row => ({
    date: row.date,
    curtailmentCount: parseInt(row.curtailment_count),
    bitcoinCount: row.bitcoin_count ? parseInt(row.bitcoin_count) : null,
    status: row.status as 'Missing' | 'Incomplete' | 'Complete',
    priority: row.status === 'Missing' ? 1 : 2
  }));
}

/**
 * Process a single date to fix its Bitcoin calculations
 */
async function fixDateCalculations(date: string): Promise<FixResult> {
  const startTime = Date.now();
  try {
    console.log(`Processing Bitcoin calculations for ${date}...`);
    
    // Get curtailment records count for this date to verify later
    const curtailmentCount = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    const expectedCount = curtailmentCount[0]?.count || 0;
    
    // Process all miner models concurrently
    console.log(`Processing ${MINER_MODEL_LIST.length} miner models for ${date}`);
    
    const limit = pLimit(3); // Process up to 3 miner models at once
    const modelResults = await Promise.all(
      MINER_MODEL_LIST.map(model => 
        limit(async () => {
          try {
            await processSingleDay(date, model);
            return { model, success: true };
          } catch (error) {
            console.error(`Failed to process ${model} for ${date}:`, error);
            return { model, success: false, error };
          }
        })
      )
    );
    
    // Check results after processing
    const successfulModels = modelResults.filter(r => r.success).map(r => r.model);
    const failedModels = modelResults.filter(r => !r.success).map(r => r.model);
    
    // Verify counts to make sure everything was processed
    const verifyResult = await verifyCalculations(date);
    
    const duration = Date.now() - startTime;
    
    if (failedModels.length > 0) {
      return {
        date,
        status: 'Failure',
        message: `Processed ${successfulModels.join(', ')} but failed for ${failedModels.join(', ')}. Verification: ${verifyResult.complete ? 'Complete' : 'Incomplete'}`,
        durationMs: duration
      };
    }
    
    return {
      date,
      status: 'Success',
      message: `Successfully processed all models. Verification: ${verifyResult.message}`,
      durationMs: duration
    };
    
  } catch (error) {
    const duration = Date.now() - startTime;
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(`Error processing ${date}:`, error);
    
    return {
      date,
      status: 'Failure',
      message: `Error: ${errorMessage}`,
      durationMs: duration
    };
  }
}

/**
 * Verify that calculations were successfully completed for all miner models
 */
async function verifyCalculations(date: string): Promise<{ complete: boolean; message: string }> {
  // Get curtailment records count
  const curtailmentCount = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  const expectedCount = curtailmentCount[0]?.count || 0;
  
  // Check each miner model
  const verificationResults = [];
  for (const model of MINER_MODEL_LIST) {
    const bitcoinCount = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        eq(historicalBitcoinCalculations.minerModel, model)
      ));
    
    const actualCount = bitcoinCount[0]?.count || 0;
    const percentComplete = Math.round((actualCount / expectedCount) * 100);
    
    verificationResults.push({
      model,
      expectedCount,
      actualCount,
      percentComplete,
      complete: actualCount >= expectedCount
    });
  }
  
  const allComplete = verificationResults.every(r => r.complete);
  const message = verificationResults
    .map(r => `${r.model}: ${r.actualCount}/${r.expectedCount} (${r.percentComplete}%)`)
    .join(', ');
  
  return {
    complete: allComplete,
    message
  };
}

/**
 * Load existing progress log if it exists
 */
async function loadProgressLog(): Promise<ProgressLog | null> {
  try {
    const logContent = await fs.readFile(LOG_FILE, 'utf-8');
    return JSON.parse(logContent) as ProgressLog;
  } catch (error) {
    return null;
  }
}

/**
 * Save current progress to the log file
 */
async function saveProgressLog(progress: ProgressLog): Promise<void> {
  progress.lastUpdated = new Date().toISOString();
  await fs.writeFile(LOG_FILE, JSON.stringify(progress, null, 2), 'utf-8');
}

/**
 * Main function to fix all missing Bitcoin calculations
 */
async function fixAllMissingCalculations() {
  console.log("=== Starting Comprehensive Bitcoin Calculation Fix ===");
  const startTime = new Date().toISOString();
  
  // Initialize or load progress log
  let progress = await loadProgressLog();
  if (!progress) {
    progress = {
      startTime,
      lastUpdated: startTime,
      processedDates: [],
      successfulDates: [],
      failedDates: {},
      currentBatch: []
    };
  }
  
  console.log("Progress loaded:", {
    processed: progress.processedDates.length,
    successful: progress.successfulDates.length,
    failed: Object.keys(progress.failedDates).length
  });
  
  // Get a summary of all months to understand the scope of the issue
  console.log("\nAnalyzing monthly data...");
  const monthSummary = await getMonthSummary();
  
  // Count the different statuses
  const missingMonths = monthSummary.filter(m => m.status === 'Missing').length;
  const incompleteMonths = monthSummary.filter(m => m.status === 'Incomplete').length;
  const completeMonths = monthSummary.filter(m => m.status === 'Complete').length;
  
  console.log(`Found ${monthSummary.length} months of data:`);
  console.log(`- ${missingMonths} months with missing calculations`);
  console.log(`- ${incompleteMonths} months with incomplete calculations`);
  console.log(`- ${completeMonths} months with complete calculations`);
  
  // Get a list of all dates that need fixing
  console.log("\nIdentifying individual dates that need processing...");
  let datesToFix = await getDatesThatNeedFixing();
  
  // Filter out already processed dates
  datesToFix = datesToFix.filter(d => !progress.processedDates.includes(d.date));
  
  console.log(`Found ${datesToFix.length} dates that need processing`);
  console.log("Sample dates:", datesToFix.slice(0, 5).map(d => d.date).join(", "));
  
  if (datesToFix.length === 0) {
    console.log("No dates need fixing. All calculations are complete!");
    return;
  }
  
  // Process in batches with limited concurrency
  const totalBatches = Math.ceil(datesToFix.length / BATCH_SIZE);
  console.log(`\nProcessing in ${totalBatches} batches of ${BATCH_SIZE} dates with ${CONCURRENCY_LIMIT} concurrent operations per batch`);
  
  for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
    const batchStart = batchIndex * BATCH_SIZE;
    const batchEnd = Math.min(batchStart + BATCH_SIZE, datesToFix.length);
    const currentBatch = datesToFix.slice(batchStart, batchEnd);
    
    console.log(`\n=== Processing Batch ${batchIndex + 1}/${totalBatches} ===`);
    console.log(`Dates in this batch: ${currentBatch.map(d => d.date).join(", ")}`);
    
    // Update progress log with current batch
    progress.currentBatch = currentBatch.map(d => d.date);
    await saveProgressLog(progress);
    
    // Process the batch with limited concurrency
    const limit = pLimit(CONCURRENCY_LIMIT);
    const batchResults = await Promise.all(
      currentBatch.map(dateInfo => 
        limit(async () => {
          const result = await fixDateCalculations(dateInfo.date);
          
          // Update progress immediately after each date
          progress.processedDates.push(dateInfo.date);
          if (result.status === 'Success') {
            progress.successfulDates.push(dateInfo.date);
          } else {
            progress.failedDates[dateInfo.date] = result.message;
          }
          await saveProgressLog(progress);
          
          return result;
        })
      )
    );
    
    // Print batch results
    console.log(`\nBatch ${batchIndex + 1} Results:`);
    for (const result of batchResults) {
      const duration = (result.durationMs / 1000).toFixed(1);
      console.log(`${result.date}: ${result.status} (${duration}s) - ${result.message}`);
    }
    
    // Delay between batches to avoid overwhelming the system
    if (batchIndex < totalBatches - 1) {
      console.log(`Waiting ${DELAY_BETWEEN_BATCHES / 1000} seconds before next batch...`);
      await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_BATCHES));
    }
  }
  
  // Final summary
  const totalDuration = new Date().getTime() - new Date(startTime).getTime();
  const durationMinutes = (totalDuration / (1000 * 60)).toFixed(1);
  
  console.log(`\n=== Final Summary ===`);
  console.log(`Total processing time: ${durationMinutes} minutes`);
  console.log(`Processed dates: ${progress.processedDates.length}`);
  console.log(`Successful dates: ${progress.successfulDates.length}`);
  console.log(`Failed dates: ${Object.keys(progress.failedDates).length}`);
  
  if (Object.keys(progress.failedDates).length > 0) {
    console.log("\nFailed dates:");
    for (const [date, message] of Object.entries(progress.failedDates)) {
      console.log(`- ${date}: ${message}`);
    }
  }
  
  // Re-verify overall progress
  console.log("\nPerforming final verification...");
  const finalMonthSummary = await getMonthSummary();
  const finalMissingMonths = finalMonthSummary.filter(m => m.status === 'Missing').length;
  const finalIncompleteMonths = finalMonthSummary.filter(m => m.status === 'Incomplete').length;
  const finalCompleteMonths = finalMonthSummary.filter(m => m.status === 'Complete').length;
  
  console.log(`Final status:`);
  console.log(`- ${finalMissingMonths} months with missing calculations`);
  console.log(`- ${finalIncompleteMonths} months with incomplete calculations`);
  console.log(`- ${finalCompleteMonths} months with complete calculations`);
  
  console.log("\n=== Bitcoin Calculation Fix Complete ===");
}

/**
 * Main entry point
 */
async function main() {
  try {
    await fixAllMissingCalculations();
    process.exit(0);
  } catch (error) {
    console.error("Fatal error:", error);
    process.exit(1);
  }
}

// Run the script if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}

export { fixAllMissingCalculations, getMonthSummary, getDatesThatNeedFixing };
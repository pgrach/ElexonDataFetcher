/**
 * This script ensures 100% reconciliation between curtailment_records and historical_bitcoin_calculations.
 * It identifies and processes all missing or incomplete Bitcoin calculations across all time periods.
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { sql, and, eq, between, desc, isNull } from "drizzle-orm";
import { format, parseISO, addDays, subDays } from "date-fns";
import pLimit from "p-limit";
import fs from "fs/promises";
import { processSingleDay } from "./server/services/bitcoinService";
import { minerModels } from "./server/types/bitcoin";
import { getDifficultyData } from "./server/services/dynamodbService";

// Configuration
const MINER_MODEL_LIST = Object.keys(minerModels);
const CONCURRENCY_LIMIT = 2; // Limit concurrent processing to avoid resource exhaustion
const BATCH_SIZE = 5; // Number of dates to process in each batch
const DELAY_BETWEEN_BATCHES = 5000; // 5 seconds delay between batches
const LOG_FILE = "bitcoin_calculation_fix_log.json";
const VERIFICATION_INTERVAL = 10; // Verify progress every N batches

// Define interfaces for our data structures
interface ReconciliationSummary {
  totalYearMonths: number;
  missingYearMonths: number;
  incompleteYearMonths: number;
  completeYearMonths: number;
  completionPercentage: number;
  totalCurtailmentRecords: number;
  totalBitcoinCalculations: number;
  missingCalculations: number;
}

interface YearMonthSummary {
  yearMonth: string;
  curtailmentCount: number;
  modelCounts: {[model: string]: number};
  status: 'Missing' | 'Incomplete' | 'Complete';
  completionPercentage: number;
  priority: number;
}

interface DateToProcess {
  date: string;
  curtailmentCount: number;
  bitcoinCounts: {[model: string]: number};
  missingModels: string[];
  incompleteModels: string[];
  status: 'Missing' | 'Incomplete' | 'Complete';
  completionPercentage: number;
  priority: number;
}

interface FixResult {
  date: string;
  status: 'Success' | 'Failure';
  message: string;
  durationMs: number;
  modelResults: {
    model: string;
    success: boolean;
    errorMessage?: string;
    recordsAdded?: number;
  }[];
}

interface ProgressLog {
  startTime: string;
  lastUpdated: string;
  reconciliationSummary: ReconciliationSummary;
  processedDates: string[];
  successfulDates: string[];
  failedDates: Record<string, string>;
  currentBatch: string[];
}

/**
 * Get a comprehensive summary of reconciliation status across all time periods
 */
async function getReconciliationSummary(): Promise<ReconciliationSummary> {
  // Get total curtailment records
  const totalCurtailmentResult = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(curtailmentRecords);
  
  const totalCurtailmentRecords = totalCurtailmentResult[0]?.count || 0;
  
  // Get total bitcoin calculations for the default model
  const totalBitcoinResult = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.minerModel, MINER_MODEL_LIST[0]));
  
  const totalBitcoinCalculations = totalBitcoinResult[0]?.count || 0;
  
  // Get year-month summary
  const yearMonthSummary = await getYearMonthSummary();
  
  // Calculate summary statistics
  const missingYearMonths = yearMonthSummary.filter(m => m.status === 'Missing').length;
  const incompleteYearMonths = yearMonthSummary.filter(m => m.status === 'Incomplete').length;
  const completeYearMonths = yearMonthSummary.filter(m => m.status === 'Complete').length;
  
  return {
    totalYearMonths: yearMonthSummary.length,
    missingYearMonths,
    incompleteYearMonths,
    completeYearMonths,
    completionPercentage: Math.round((completeYearMonths / yearMonthSummary.length) * 100),
    totalCurtailmentRecords,
    totalBitcoinCalculations,
    missingCalculations: totalCurtailmentRecords - totalBitcoinCalculations
  };
}

/**
 * Get a summary of all year-months with their calculation status for all miner models
 */
async function getYearMonthSummary(): Promise<YearMonthSummary[]> {
  const result = await db.execute(sql`
    WITH curtailment_detail AS (
      SELECT 
        TO_CHAR(settlement_date, 'YYYY-MM') as year_month,
        COUNT(*) as curtailment_count
      FROM curtailment_records
      GROUP BY TO_CHAR(settlement_date, 'YYYY-MM')
    ),
    bitcoin_model_counts AS (
      SELECT 
        TO_CHAR(settlement_date, 'YYYY-MM') as year_month,
        miner_model,
        COUNT(*) as bitcoin_count
      FROM historical_bitcoin_calculations
      GROUP BY TO_CHAR(settlement_date, 'YYYY-MM'), miner_model
    )
    SELECT 
      c.year_month,
      c.curtailment_count,
      jsonb_object_agg(b.miner_model, b.bitcoin_count) FILTER (WHERE b.miner_model IS NOT NULL) as model_counts,
      CASE
        WHEN MIN(b.bitcoin_count) = c.curtailment_count THEN 'Complete' 
        WHEN MIN(b.bitcoin_count) IS NULL THEN 'Missing'
        ELSE 'Incomplete'
      END as status,
      CASE 
        WHEN MIN(b.bitcoin_count) IS NULL THEN 0
        ELSE ROUND(100.0 * MIN(b.bitcoin_count) / c.curtailment_count)
      END as completion_percentage
    FROM curtailment_detail c
    LEFT JOIN bitcoin_model_counts b ON c.year_month = b.year_month
    GROUP BY c.year_month, c.curtailment_count
    ORDER BY 
      CASE
        WHEN MIN(b.bitcoin_count) IS NULL THEN 1
        WHEN MIN(b.bitcoin_count) < c.curtailment_count THEN 2
        ELSE 3
      END,
      c.year_month
  `);

  return result.rows.map(row => {
    // Parse the model counts from JSON
    const modelCounts = row.model_counts ? JSON.parse(row.model_counts) : {};
    
    // Determine priority: Missing > Incomplete > Complete
    let priority = 3;
    if (row.status === 'Missing') priority = 1;
    else if (row.status === 'Incomplete') priority = 2;
    
    return {
      yearMonth: row.year_month,
      curtailmentCount: parseInt(row.curtailment_count),
      modelCounts,
      status: row.status as 'Missing' | 'Incomplete' | 'Complete',
      completionPercentage: parseInt(row.completion_percentage),
      priority
    };
  });
}

/**
 * Get a list of all dates that need Bitcoin calculation reconciliation
 */
async function getDatesToProcess(): Promise<DateToProcess[]> {
  const result = await db.execute(sql`
    WITH curtailment_dates AS (
      SELECT 
        settlement_date::text as date,
        COUNT(*) as curtailment_count
      FROM curtailment_records
      GROUP BY settlement_date
    ),
    bitcoin_model_counts AS (
      SELECT 
        settlement_date::text as date,
        miner_model,
        COUNT(*) as bitcoin_count
      FROM historical_bitcoin_calculations
      GROUP BY settlement_date, miner_model
    ),
    date_model_summary AS (
      SELECT 
        c.date,
        c.curtailment_count,
        b.miner_model,
        b.bitcoin_count,
        CASE
          WHEN b.bitcoin_count IS NULL THEN 'Missing'
          WHEN b.bitcoin_count < c.curtailment_count THEN 'Incomplete'
          WHEN b.bitcoin_count >= c.curtailment_count THEN 'Complete'
        END as model_status,
        CASE
          WHEN b.bitcoin_count IS NULL THEN 0
          ELSE ROUND(100.0 * b.bitcoin_count / c.curtailment_count)
        END as model_completion
      FROM curtailment_dates c
      LEFT JOIN bitcoin_model_counts b ON c.date = b.date
    )
    SELECT 
      date,
      curtailment_count,
      jsonb_object_agg(miner_model, bitcoin_count) FILTER (WHERE miner_model IS NOT NULL) as model_counts,
      jsonb_agg(miner_model) FILTER (WHERE model_status = 'Missing') as missing_models,
      jsonb_agg(miner_model) FILTER (WHERE model_status = 'Incomplete') as incomplete_models,
      CASE
        WHEN COUNT(CASE WHEN model_status = 'Complete' THEN 1 END) = ${MINER_MODEL_LIST.length} THEN 'Complete'
        WHEN COUNT(CASE WHEN model_status = 'Missing' THEN 1 END) = ${MINER_MODEL_LIST.length} THEN 'Missing'
        ELSE 'Incomplete'
      END as date_status,
      MIN(model_completion) as min_completion
    FROM date_model_summary
    GROUP BY date, curtailment_count
    HAVING CASE
        WHEN COUNT(CASE WHEN model_status = 'Complete' THEN 1 END) = ${MINER_MODEL_LIST.length} THEN 'Complete'
        WHEN COUNT(CASE WHEN model_status = 'Missing' THEN 1 END) = ${MINER_MODEL_LIST.length} THEN 'Missing'
        ELSE 'Incomplete'
      END != 'Complete'
    ORDER BY 
      CASE
        WHEN COUNT(CASE WHEN model_status = 'Missing' THEN 1 END) = ${MINER_MODEL_LIST.length} THEN 1
        ELSE 2
      END,
      date
  `);

  return result.rows.map(row => {
    // Parse fields from JSON
    const modelCounts = row.model_counts ? JSON.parse(row.model_counts) : {};
    const missingModels = row.missing_models ? JSON.parse(row.missing_models) : [];
    const incompleteModels = row.incomplete_models ? JSON.parse(row.incomplete_models) : [];
    
    // Determine priority: Missing everything > Missing some models > Incomplete > Complete
    let priority = 4;
    if (row.date_status === 'Missing') priority = 1;
    else if (missingModels.length > 0) priority = 2; 
    else if (row.date_status === 'Incomplete') priority = 3;
    
    return {
      date: row.date,
      curtailmentCount: parseInt(row.curtailment_count),
      bitcoinCounts: modelCounts,
      missingModels,
      incompleteModels,
      status: row.date_status as 'Missing' | 'Incomplete' | 'Complete',
      completionPercentage: parseInt(row.min_completion || '0'),
      priority
    };
  });
}

/**
 * Process a single date to ensure complete Bitcoin calculations for all models
 */
async function fixDateCalculations(date: string): Promise<FixResult> {
  const startTime = Date.now();
  
  try {
    console.log(`Processing Bitcoin calculations for ${date}...`);
    
    // Get the specific date info
    const dateInfo = await getSingleDateInfo(date);
    
    if (!dateInfo) {
      return {
        date,
        status: 'Failure',
        message: `No curtailment records found for ${date}`,
        durationMs: Date.now() - startTime,
        modelResults: []
      };
    }
    
    console.log(`Found ${dateInfo.curtailmentCount} curtailment records for ${date}`);
    
    // Get difficulty for this date (required for processing)
    try {
      const difficulty = await getDifficultyData(date);
      console.log(`Using difficulty: ${difficulty.toLocaleString()} for ${date}`);
    } catch (error) {
      console.warn(`Warning: Could not get difficulty for ${date}. Will use default difficulty.`);
    }
    
    // Process each miner model that needs attention
    const modelsToProcess = [
      ...dateInfo.missingModels, 
      ...dateInfo.incompleteModels
    ];
    
    if (modelsToProcess.length === 0) {
      return {
        date,
        status: 'Success',
        message: `All models are already complete for ${date}`,
        durationMs: Date.now() - startTime,
        modelResults: []
      };
    }
    
    console.log(`Processing ${modelsToProcess.length} models for ${date}: ${modelsToProcess.join(', ')}`);
    
    // Process each model
    const limit = pLimit(3); // Process up to 3 miner models concurrently
    const modelResults = await Promise.all(
      modelsToProcess.map(model => 
        limit(async () => {
          try {
            // Get pre-processing count
            const preCount = await getModelRecordCount(date, model);
            
            // Process the model
            await processSingleDay(date, model);
            
            // Get post-processing count to determine records added
            const postCount = await getModelRecordCount(date, model);
            const recordsAdded = postCount - preCount;
            
            return { 
              model, 
              success: true, 
              recordsAdded 
            };
          } catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            console.error(`Failed to process ${model} for ${date}:`, error);
            return { 
              model, 
              success: false, 
              errorMessage 
            };
          }
        })
      )
    );
    
    // Verify results
    const verificationResult = await verifyDateCalculations(date);
    
    const successfulModels = modelResults.filter(r => r.success).map(r => r.model);
    const failedModels = modelResults.filter(r => !r.success).map(r => r.model);
    
    const status = failedModels.length > 0 ? 'Failure' : 'Success';
    const message = `Processed ${successfulModels.length} models (${successfulModels.join(', ')}). ` +
      (failedModels.length > 0 ? `Failed: ${failedModels.join(', ')}. ` : '') +
      `Verification: ${verificationResult.message}`;
    
    return {
      date,
      status,
      message,
      durationMs: Date.now() - startTime,
      modelResults
    };
    
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(`Error processing ${date}:`, error);
    
    return {
      date,
      status: 'Failure',
      message: `Error: ${errorMessage}`,
      durationMs: Date.now() - startTime,
      modelResults: []
    };
  }
}

/**
 * Get detailed information about a specific date
 */
async function getSingleDateInfo(date: string): Promise<DateToProcess | null> {
  const result = await db.execute(sql`
    WITH curtailment_count AS (
      SELECT COUNT(*) as count
      FROM curtailment_records
      WHERE settlement_date = ${date}::date
    ),
    bitcoin_counts AS (
      SELECT 
        miner_model,
        COUNT(*) as count
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${date}::date
      GROUP BY miner_model
    )
    SELECT 
      c.count as curtailment_count,
      jsonb_object_agg(b.miner_model, b.count) FILTER (WHERE b.miner_model IS NOT NULL) as model_counts
    FROM curtailment_count c
    LEFT JOIN bitcoin_counts b ON true
    GROUP BY c.count
  `);

  if (result.rows.length === 0 || parseInt(result.rows[0].curtailment_count) === 0) {
    return null;
  }

  const row = result.rows[0];
  const curtailmentCount = parseInt(row.curtailment_count);
  const modelCounts = row.model_counts ? JSON.parse(row.model_counts) : {};
  
  // Check each model's status
  const missingModels: string[] = [];
  const incompleteModels: string[] = [];
  const completeModels: string[] = [];
  
  for (const model of MINER_MODEL_LIST) {
    const modelCount = modelCounts[model] || 0;
    if (modelCount === 0) {
      missingModels.push(model);
    } else if (modelCount < curtailmentCount) {
      incompleteModels.push(model);
    } else {
      completeModels.push(model);
    }
  }
  
  // Determine overall status
  let status: 'Missing' | 'Incomplete' | 'Complete';
  if (missingModels.length === MINER_MODEL_LIST.length) {
    status = 'Missing';
  } else if (completeModels.length === MINER_MODEL_LIST.length) {
    status = 'Complete';
  } else {
    status = 'Incomplete';
  }
  
  // Calculate minimum completion percentage
  const minCompletion = MINER_MODEL_LIST.reduce((min, model) => {
    const modelCount = modelCounts[model] || 0;
    const percentage = curtailmentCount > 0 ? Math.round((modelCount / curtailmentCount) * 100) : 0;
    return Math.min(min, percentage);
  }, 100);
  
  // Determine priority
  let priority = 4;
  if (status === 'Missing') priority = 1;
  else if (missingModels.length > 0) priority = 2;
  else if (status === 'Incomplete') priority = 3;
  
  return {
    date,
    curtailmentCount,
    bitcoinCounts: modelCounts,
    missingModels,
    incompleteModels,
    status,
    completionPercentage: minCompletion,
    priority
  };
}

/**
 * Get the count of Bitcoin calculations for a specific date and model
 */
async function getModelRecordCount(date: string, model: string): Promise<number> {
  const result = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(historicalBitcoinCalculations)
    .where(and(
      eq(historicalBitcoinCalculations.settlementDate, date),
      eq(historicalBitcoinCalculations.minerModel, model)
    ));
  
  return result[0]?.count || 0;
}

/**
 * Verify that a date has complete Bitcoin calculations for all models
 */
async function verifyDateCalculations(date: string): Promise<{ 
  complete: boolean; 
  message: string;
  modelResults: {
    model: string;
    expected: number;
    actual: number;
    percentage: number;
    complete: boolean;
  }[];
}> {
  // Get curtailment record count
  const curtailmentResult = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  const expectedCount = curtailmentResult[0]?.count || 0;
  
  if (expectedCount === 0) {
    return {
      complete: true,
      message: 'No curtailment records to process',
      modelResults: []
    };
  }
  
  // Check each model
  const modelResults = [];
  
  for (const model of MINER_MODEL_LIST) {
    const bitcoinResult = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        eq(historicalBitcoinCalculations.minerModel, model)
      ));
    
    const actualCount = bitcoinResult[0]?.count || 0;
    const percentage = Math.round((actualCount / expectedCount) * 100);
    const complete = actualCount >= expectedCount;
    
    modelResults.push({
      model,
      expected: expectedCount,
      actual: actualCount,
      percentage,
      complete
    });
  }
  
  const allComplete = modelResults.every(r => r.complete);
  const message = modelResults
    .map(r => `${r.model}: ${r.actual}/${r.expected} (${r.percentage}%)`)
    .join(', ');
  
  return {
    complete: allComplete,
    message,
    modelResults
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
 * Main function to ensure 100% reconciliation between tables
 */
async function ensureCompleteBitcoinCalculations() {
  console.log("=== Starting Complete Bitcoin Calculation Reconciliation ===");
  const startTime = new Date().toISOString();
  
  // Get initial reconciliation summary
  console.log("Generating initial reconciliation summary...");
  const initialSummary = await getReconciliationSummary();
  
  console.log(`\nInitial Status:`);
  console.log(`- Total curtailment records: ${initialSummary.totalCurtailmentRecords.toLocaleString()}`);
  console.log(`- Total Bitcoin calculations per model: ${initialSummary.totalBitcoinCalculations.toLocaleString()}`);
  console.log(`- Missing calculations per model: ${initialSummary.missingCalculations.toLocaleString()}`);
  console.log(`- Total months of data: ${initialSummary.totalYearMonths}`);
  console.log(`  - ${initialSummary.completeYearMonths} complete months (${initialSummary.completionPercentage}%)`);
  console.log(`  - ${initialSummary.incompleteYearMonths} incomplete months`);
  console.log(`  - ${initialSummary.missingYearMonths} missing months`);
  
  // Initialize or load progress log
  let progress = await loadProgressLog();
  if (!progress) {
    progress = {
      startTime,
      lastUpdated: startTime,
      reconciliationSummary: initialSummary,
      processedDates: [],
      successfulDates: [],
      failedDates: {},
      currentBatch: []
    };
  } else {
    console.log("\nProgress loaded from previous run:");
    console.log(`- Processed dates: ${progress.processedDates.length}`);
    console.log(`- Successful dates: ${progress.successfulDates.length}`);
    console.log(`- Failed dates: ${Object.keys(progress.failedDates).length}`);
  }
  await saveProgressLog(progress);
  
  // Get all dates that need processing
  console.log("\nIdentifying dates that need processing...");
  let datesToProcess = await getDatesToProcess();
  
  // Filter out already processed dates
  datesToProcess = datesToProcess.filter(d => !progress.processedDates.includes(d.date));
  
  // Sort by priority
  datesToProcess.sort((a, b) => 
    a.priority === b.priority 
      ? a.date.localeCompare(b.date) 
      : a.priority - b.priority
  );
  
  if (datesToProcess.length === 0) {
    console.log("All dates are fully reconciled! No processing needed.");
    return;
  }
  
  console.log(`Found ${datesToProcess.length} dates to process.`);
  console.log(`- ${datesToProcess.filter(d => d.status === 'Missing').length} completely missing dates`);
  console.log(`- ${datesToProcess.filter(d => d.status === 'Incomplete').length} incomplete dates`);
  
  if (datesToProcess.length > 0) {
    console.log("\nSample dates to process:");
    for (let i = 0; i < Math.min(5, datesToProcess.length); i++) {
      const date = datesToProcess[i];
      const missingModels = date.missingModels.length > 0 
        ? `, missing models: ${date.missingModels.join(', ')}` 
        : '';
      const incompleteModels = date.incompleteModels.length > 0 
        ? `, incomplete models: ${date.incompleteModels.join(', ')}` 
        : '';
      
      console.log(`- ${date.date}: ${date.status}, ${date.completionPercentage}% complete${missingModels}${incompleteModels}`);
    }
  }
  
  // Process in batches with limited concurrency
  const totalBatches = Math.ceil(datesToProcess.length / BATCH_SIZE);
  console.log(`\nProcessing in ${totalBatches} batches of ${BATCH_SIZE} dates with ${CONCURRENCY_LIMIT} concurrent operations per batch`);
  
  for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
    const batchStart = batchIndex * BATCH_SIZE;
    const batchEnd = Math.min(batchStart + BATCH_SIZE, datesToProcess.length);
    const currentBatch = datesToProcess.slice(batchStart, batchEnd);
    
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
    
    // Periodically verify progress
    if (batchIndex % VERIFICATION_INTERVAL === 0 || batchIndex === totalBatches - 1) {
      console.log("\nVerifying current reconciliation status...");
      progress.reconciliationSummary = await getReconciliationSummary();
      await saveProgressLog(progress);
      
      console.log(`Current Status:`);
      console.log(`- Completion: ${progress.reconciliationSummary.completionPercentage}%`);
      console.log(`- ${progress.reconciliationSummary.completeYearMonths}/${progress.reconciliationSummary.totalYearMonths} months complete`);
      console.log(`- ${progress.reconciliationSummary.totalBitcoinCalculations.toLocaleString()}/${progress.reconciliationSummary.totalCurtailmentRecords.toLocaleString()} records processed per model`);
    }
    
    // Delay between batches to avoid overwhelming the system
    if (batchIndex < totalBatches - 1) {
      console.log(`Waiting ${DELAY_BETWEEN_BATCHES / 1000} seconds before next batch...`);
      await new Promise(resolve => setTimeout(resolve, DELAY_BETWEEN_BATCHES));
    }
  }
  
  // Final verification
  console.log("\n=== Performing Final Verification ===");
  const finalSummary = await getReconciliationSummary();
  
  console.log(`\nFinal Status:`);
  console.log(`- Total curtailment records: ${finalSummary.totalCurtailmentRecords.toLocaleString()}`);
  console.log(`- Total Bitcoin calculations per model: ${finalSummary.totalBitcoinCalculations.toLocaleString()}`);
  console.log(`- Missing calculations per model: ${finalSummary.missingCalculations.toLocaleString()}`);
  console.log(`- Months of data: ${finalSummary.totalYearMonths}`);
  console.log(`  - ${finalSummary.completeYearMonths} complete months (${finalSummary.completionPercentage}%)`);
  console.log(`  - ${finalSummary.incompleteYearMonths} incomplete months`);
  console.log(`  - ${finalSummary.missingYearMonths} missing months`);
  
  const totalDuration = new Date().getTime() - new Date(startTime).getTime();
  const durationMinutes = (totalDuration / (1000 * 60)).toFixed(1);
  
  console.log(`\nTotal processing time: ${durationMinutes} minutes`);
  console.log(`Processed dates: ${progress.processedDates.length}`);
  console.log(`Successful dates: ${progress.successfulDates.length}`);
  console.log(`Failed dates: ${Object.keys(progress.failedDates).length}`);
  
  if (Object.keys(progress.failedDates).length > 0) {
    console.log("\nFailed dates:");
    for (const [date, message] of Object.entries(progress.failedDates)) {
      console.log(`- ${date}: ${message}`);
    }
    console.log("\nConsider rerunning the script to process failed dates.");
  }
  
  if (finalSummary.completionPercentage === 100) {
    console.log("\n🎉 Success! All Bitcoin calculations are now 100% reconciled with curtailment records! 🎉");
  } else {
    console.log(`\n⚠️ Warning: Reconciliation is at ${finalSummary.completionPercentage}%, not 100% complete.`);
    console.log("Run this script again to process any remaining or failed dates.");
  }
  
  console.log("\n=== Bitcoin Calculation Reconciliation Complete ===");
}

/**
 * Main entry point
 */
async function main() {
  try {
    await ensureCompleteBitcoinCalculations();
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

export { ensureCompleteBitcoinCalculations, getReconciliationSummary, getDatesToProcess, fixDateCalculations };
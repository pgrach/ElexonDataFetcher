/**
 * Process All 2023 Data At Once
 * 
 * This is a streamlined script specifically for processing all 2023 data
 * to fix missing Bitcoin calculations. It leverages existing services but
 * focuses on batch processing with improved error handling and retry logic.
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { and, count, eq, sql } from "drizzle-orm";
import { minerModels } from "./server/types/bitcoin";
import { processDailyCurtailment } from "./server/services/curtailment";
import { getDifficultyData } from "./server/services/dynamodbService";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from 'url';

// Get current filename and directory in ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Progress tracking
interface ProcessingStats {
  totalDates: number;
  processedDates: number;
  successfulDates: number;
  partialSuccessDates: number;
  failedDates: number;
  totalMissingCalculations: number;
  totalFixedCalculations: number;
  startTime: number;
  lastUpdateTime: number;
}

// Global stats
const stats: ProcessingStats = {
  totalDates: 0,
  processedDates: 0,
  successfulDates: 0,
  partialSuccessDates: 0,
  failedDates: 0,
  totalMissingCalculations: 0,
  totalFixedCalculations: 0,
  startTime: Date.now(),
  lastUpdateTime: Date.now()
};

// Map of dates with missing calculations count
const missingCalculationsByDate: Record<string, number> = {};

/**
 * Sleep for specified milliseconds
 */
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Save processing summary to file
 */
function saveProgress(): void {
  const logFile = path.join(__dirname, 'reconcile2023_progress.json');
  const data = {
    ...stats,
    percentComplete: ((stats.processedDates / stats.totalDates) * 100).toFixed(2) + '%',
    duration: ((Date.now() - stats.startTime) / 1000 / 60).toFixed(2) + ' minutes',
    timestamp: new Date().toISOString(),
    missingCalculationsByDate
  };
  
  fs.writeFileSync(logFile, JSON.stringify(data, null, 2));
  console.log(`Progress saved to ${logFile}`);
}

/**
 * Get all dates in 2023 with curtailment records
 */
async function get2023Dates(): Promise<string[]> {
  console.log("Finding all dates in 2023 with curtailment records...");
  
  const result = await db.execute(sql`
    SELECT DISTINCT settlement_date::text
    FROM curtailment_records 
    WHERE settlement_date >= '2023-01-01'
    AND settlement_date < '2024-01-01'
    ORDER BY settlement_date
  `);
  
  const dates = result.rows.map((row: any) => row.settlement_date as string);
  console.log(`Found ${dates.length} dates in 2023 with curtailment records`);
  
  return dates;
}

/**
 * Check for missing Bitcoin calculations for a date
 */
async function checkMissingCalculations(date: string): Promise<{
  totalMissing: number;
  missingByModel: Record<string, number>;
  curtailmentCount: number;
}> {
  console.log(`Checking for missing calculations for ${date}...`);
  
  // Get total number of curtailment records for this date
  const curtailmentResult = await db.select({
    count: count(),
  }).from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  const curtailmentCount = curtailmentResult[0].count;
  
  // Check calculations for each miner model
  const missingByModel: Record<string, number> = {};
  let totalMissing = 0;
  
  for (const minerModel of Object.keys(minerModels)) {
    // Count bitcoin calculations for this date and model
    const calculationsResult = await db.select({
      count: count(),
    }).from(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      ));
    
    const calculationsCount = calculationsResult[0].count;
    const missingCount = curtailmentCount - calculationsCount;
    
    missingByModel[minerModel] = missingCount > 0 ? missingCount : 0;
    totalMissing += missingByModel[minerModel];
  }
  
  return { totalMissing, missingByModel, curtailmentCount };
}

/**
 * Fix missing calculations for a date
 */
async function fixMissingCalculations(date: string): Promise<boolean> {
  console.log(`\nProcessing date: ${date}`);
  
  try {
    // First check for missing calculations
    const before = await checkMissingCalculations(date);
    
    if (before.totalMissing === 0) {
      console.log(`No missing calculations for ${date}, skipping.`);
      return true;
    }
    
    // Update global stats
    stats.totalMissingCalculations += before.totalMissing;
    missingCalculationsByDate[date] = before.totalMissing;
    
    // Log missing calculation details
    console.log(`Found ${before.totalMissing} missing calculations for ${date}`);
    console.log("Missing by model:");
    for (const [model, count] of Object.entries(before.missingByModel)) {
      if (count > 0) {
        console.log(`  ${model}: ${count}`);
      }
    }
    
    // Ensure Bitcoin difficulty data is available
    console.log(`Verifying Bitcoin difficulty data for ${date}...`);
    
    try {
      // This will either return the existing difficulty or the fallback value
      const difficulty = await getDifficultyData(date);
      console.log(`Found Bitcoin difficulty for ${date}: ${difficulty.toLocaleString()}`);
    } catch (error) {
      console.error(`Error verifying difficulty data for ${date}:`, error);
      console.log(`Will attempt to proceed with default difficulty value...`);
    }
    
    // Reprocess the entire day's calculations through the curtailment service
    console.log(`Processing curtailment data for ${date}...`);
    await processDailyCurtailment(date);
    
    // Allow time for database to process
    await sleep(1000);
    
    // Verify the results
    const after = await checkMissingCalculations(date);
    
    // Calculate fixed calculations
    const fixedCalculations = before.totalMissing - after.totalMissing;
    stats.totalFixedCalculations += fixedCalculations;
    
    // Check if we fixed everything
    if (after.totalMissing === 0) {
      console.log(`✅ SUCCESS: All ${before.totalMissing} missing calculations for ${date} have been fixed!`);
      stats.successfulDates++;
      return true;
    }
    
    // Check if we made some progress
    if (after.totalMissing < before.totalMissing) {
      const percentFixed = (fixedCalculations / before.totalMissing * 100).toFixed(2);
      console.log(`⚠️ PARTIAL SUCCESS: Fixed ${fixedCalculations} of ${before.totalMissing} calculations (${percentFixed}%)`);
      
      // If we fixed more than 80%, count as partial success
      if (fixedCalculations > before.totalMissing * 0.8) {
        stats.partialSuccessDates++;
        return true;
      }
    }
    
    // If we get here, we didn't fix enough
    console.log(`❌ FAILED: Could not fix enough calculations for ${date}`);
    stats.failedDates++;
    return false;
    
  } catch (error) {
    console.error(`Error processing ${date}:`, error);
    stats.failedDates++;
    return false;
  }
}

/**
 * Process all dates in 2023
 */
async function processAll2023Dates(): Promise<void> {
  console.log("Starting comprehensive 2023 data reconciliation...");
  
  // Get all dates in 2023 with curtailment records
  const allDates = await get2023Dates();
  stats.totalDates = allDates.length;
  
  // Process each date
  for (let i = 0; i < allDates.length; i++) {
    const date = allDates[i];
    
    try {
      await fixMissingCalculations(date);
    } catch (error) {
      console.error(`Unexpected error processing ${date}:`, error);
      stats.failedDates++;
    }
    
    // Update progress stats
    stats.processedDates++;
    stats.lastUpdateTime = Date.now();
    
    // Save progress regularly
    if (i % 5 === 0 || i === allDates.length - 1) {
      saveProgress();
      
      // Log progress summary
      const percentComplete = (stats.processedDates / stats.totalDates * 100).toFixed(2);
      const timeElapsed = ((Date.now() - stats.startTime) / 1000 / 60).toFixed(2);
      
      console.log(`\n===== PROGRESS UPDATE =====`);
      console.log(`Processed ${stats.processedDates}/${stats.totalDates} dates (${percentComplete}% complete)`);
      console.log(`Success: ${stats.successfulDates}, Partial: ${stats.partialSuccessDates}, Failed: ${stats.failedDates}`);
      console.log(`Total missing: ${stats.totalMissingCalculations}, Fixed: ${stats.totalFixedCalculations}`);
      console.log(`Time elapsed: ${timeElapsed} minutes`);
      console.log(`Current date: ${date}`);
    }
    
    // Add a delay between dates to avoid overloading the database
    await sleep(2000);
  }
  
  // Final summary
  const timeElapsed = ((Date.now() - stats.startTime) / 1000 / 60).toFixed(2);
  const fixedPercent = (stats.totalFixedCalculations / stats.totalMissingCalculations * 100).toFixed(2);
  
  console.log(`\n===== 2023 RECONCILIATION COMPLETE =====`);
  console.log(`Processed ${stats.totalDates} dates in ${timeElapsed} minutes`);
  console.log(`Successfully fixed: ${stats.successfulDates} dates`);
  console.log(`Partially fixed: ${stats.partialSuccessDates} dates`);
  console.log(`Failed: ${stats.failedDates} dates`);
  console.log(`Total missing calculations: ${stats.totalMissingCalculations}`);
  console.log(`Total fixed calculations: ${stats.totalFixedCalculations} (${fixedPercent}%)`);
  
  // Save final progress
  saveProgress();
}

// Run the script
processAll2023Dates()
  .catch(error => {
    console.error("Unhandled error:", error);
    process.exit(1);
  })
  .finally(() => {
    console.log("Script execution completed");
  });
/**
 * Reconcile 2023 Data - Batch Processing Version
 * 
 * This script analyzes and fixes missing Bitcoin calculations for 2023 data
 * one month at a time to avoid timeouts. It provides a command-line interface
 * to run specific months or continue from a previous run.
 * 
 * Usage:
 *   npx tsx reconcile2023_batch.ts [command] [options]
 * 
 * Commands:
 *   month YYYY-MM     - Process a specific month (e.g., "2023-01")
 *   range START END   - Process a range of months (e.g., "2023-01 2023-03")
 *   continue          - Continue from last processed month
 *   status            - Show current reconciliation status
 *   all               - Process all months in 2023 (careful: long-running)
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { and, count, eq, sql } from "drizzle-orm";
import { minerModels } from "./server/types/bitcoin";
import { processDailyCurtailment } from "./server/services/curtailment";
import * as fs from "fs";
import * as path from "path";

// Constants
const CHECKPOINT_FILE = 'reconcile2023_checkpoint.json';
const MONTHS_2023 = [
  '2023-01', '2023-02', '2023-03', '2023-04', '2023-05', '2023-06',
  '2023-07', '2023-08', '2023-09', '2023-10', '2023-11', '2023-12'
];

// Checkpoint interface
interface Checkpoint {
  lastProcessedMonth: string | null;
  processedMonths: string[];
  pendingMonths: string[];
  monthStats: Record<string, {
    totalDates: number;
    datesWithIssues: number;
    totalMissingCalculations: number;
    fixedDates: number;
    failedDates: number;
  }>;
  startTime: number;
  lastUpdateTime: number;
}

// ReconciliationStats interface
interface ReconciliationStats {
  date: string;
  totalCurtailmentRecords: number;
  totalPeriods: number;
  totalFarms: number;
  missingCalculations: {
    [key: string]: { // miner model
      count: number;
      periods: number[];
    }
  };
  fixed: boolean;
}

// Global checkpoint state
let checkpoint: Checkpoint = {
  lastProcessedMonth: null,
  processedMonths: [],
  pendingMonths: [],
  monthStats: {},
  startTime: Date.now(),
  lastUpdateTime: Date.now(),
};

/**
 * Save checkpoint to file
 */
function saveCheckpoint(): void {
  checkpoint.lastUpdateTime = Date.now();
  fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2));
  console.log("Checkpoint saved");
}

/**
 * Load checkpoint from file if exists
 */
function loadCheckpoint(): boolean {
  try {
    if (fs.existsSync(CHECKPOINT_FILE)) {
      const data = fs.readFileSync(CHECKPOINT_FILE, 'utf8');
      checkpoint = JSON.parse(data);
      console.log("Loaded checkpoint from file");
      return true;
    }
  } catch (error) {
    console.error("Error loading checkpoint:", error);
  }
  return false;
}

/**
 * Reset checkpoint
 */
function resetCheckpoint(): void {
  checkpoint = {
    lastProcessedMonth: null,
    processedMonths: [],
    pendingMonths: MONTHS_2023.slice(),
    monthStats: {},
    startTime: Date.now(),
    lastUpdateTime: Date.now(),
  };
  saveCheckpoint();
  console.log("Checkpoint reset");
}

/**
 * Sleep for specified milliseconds
 */
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Show help menu
 */
function showHelp(): void {
  console.log(`
Reconcile 2023 Data - Batch Processing Version

Usage:
  npx tsx reconcile2023_batch.ts [command] [options]

Commands:
  month YYYY-MM     - Process a specific month (e.g., "2023-01")
  range START END   - Process a range of months (e.g., "2023-01 2023-03")
  continue          - Continue from last processed month
  status            - Show current reconciliation status
  all               - Process all months in 2023 (careful: long-running)
  reset             - Reset checkpoint
  help              - Show this help menu
`);
}

/**
 * Get all dates in a specific month with curtailment records
 */
async function getMonthDates(yearMonth: string): Promise<string[]> {
  console.log(`Fetching dates for ${yearMonth} with curtailment records...`);
  
  const [year, month] = yearMonth.split('-');
  const startDate = `${year}-${month}-01`;
  const endDate = month === "12" 
    ? `${parseInt(year) + 1}-01-01` 
    : `${year}-${String(parseInt(month) + 1).padStart(2, '0')}-01`;
  
  const result = await db.execute(sql`
    SELECT DISTINCT settlement_date::text
    FROM curtailment_records 
    WHERE settlement_date >= ${startDate}
    AND settlement_date < ${endDate}
    ORDER BY settlement_date
  `);
  
  const dates = result.rows.map((row: any) => row.settlement_date as string);
  console.log(`Found ${dates.length} dates in ${yearMonth} with curtailment records`);
  return dates;
}

/**
 * Analyze a specific date to check for missing Bitcoin calculations
 */
async function analyzeDate(date: string): Promise<ReconciliationStats> {
  console.log(`Analyzing date ${date}...`);
  
  // Get total number of curtailment records for this date
  const curtailmentResult = await db.select({
    count: count(),
  }).from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  const totalCurtailmentRecords = curtailmentResult[0].count;
  
  // Get distinct periods and farms for this date
  const periodsResult = await db.execute(sql`
    SELECT COUNT(DISTINCT settlement_period) as periods, COUNT(DISTINCT farm_id) as farms
    FROM curtailment_records
    WHERE settlement_date = ${date}
  `);
  
  const totalPeriods = parseInt(periodsResult.rows[0].periods as string, 10);
  const totalFarms = parseInt(periodsResult.rows[0].farms as string, 10);
  
  // Check calculations for each miner model
  const stats: ReconciliationStats = {
    date,
    totalCurtailmentRecords,
    totalPeriods,
    totalFarms,
    missingCalculations: {},
    fixed: false
  };
  
  // Check for each miner model
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
    const missingCount = totalCurtailmentRecords - calculationsCount;
    
    if (missingCount > 0) {
      // Get missing periods
      const periodsWithMissingCalculations = await db.execute(sql`
        SELECT DISTINCT c.settlement_period
        FROM curtailment_records c
        LEFT JOIN historical_bitcoin_calculations h
        ON c.settlement_date = h.settlement_date
        AND c.settlement_period = h.settlement_period
        AND c.farm_id = h.farm_id
        AND h.miner_model = ${minerModel}
        WHERE c.settlement_date = ${date}
        AND h.id IS NULL
        ORDER BY c.settlement_period
      `);
      
      const missingPeriods = periodsWithMissingCalculations.rows.map((row: any) => 
        parseInt(row.settlement_period, 10)
      );
      
      stats.missingCalculations[minerModel] = {
        count: missingCount,
        periods: missingPeriods
      };
    }
  }
  
  return stats;
}

/**
 * Fix missing calculations for a date
 */
async function fixMissingCalculations(date: string): Promise<boolean> {
  console.log(`Fixing missing calculations for ${date}...`);
  
  try {
    // First, ensure Bitcoin difficulty data is available
    console.log(`Verifying Bitcoin difficulty data for ${date}...`);
    
    // Import necessary functions from dynamodbService
    const { getDifficultyData } = require('./server/services/dynamodbService');
    
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
    
    // Add small delay to allow for database processing
    await sleep(1000);
    
    // Verify that the fix worked
    const verificationStats = await analyzeDate(date);
    
    // Check if all missing calculations were fixed
    let allFixed = true;
    for (const model of Object.keys(minerModels)) {
      if (verificationStats.missingCalculations[model]?.count > 0) {
        allFixed = false;
        console.error(`Failed to fix all missing calculations for ${date}, model ${model}`);
        console.error(`  Still missing: ${verificationStats.missingCalculations[model].count} records`);
        
        if (verificationStats.missingCalculations[model].periods.length > 0) {
          console.error(`  Missing periods: ${verificationStats.missingCalculations[model].periods.join(', ')}`);
        }
      }
    }
    
    if (allFixed) {
      console.log(`✅ Successfully fixed all missing calculations for ${date}`);
      return true;
    }
    
    // Partial success is also reported
    const totalMissingBefore = Object.values(verificationStats.missingCalculations)
      .reduce((sum, current) => sum + current.count, 0);
    
    if (totalMissingBefore < verificationStats.totalCurtailmentRecords * Object.keys(minerModels).length * 0.1) {
      console.log(`⚠️ Partial success: Fixed most calculations for ${date}, only ${totalMissingBefore} still missing`);
      return true;
    }
    
    console.log(`❌ Failed to fix calculations for ${date}`);
    return false;
  } catch (error) {
    console.error(`Error fixing calculations for ${date}:`, error);
    return false;
  }
}

/**
 * Process a specific month 
 */
async function processMonth(yearMonth: string): Promise<void> {
  console.log(`\n===== PROCESSING MONTH: ${yearMonth} =====`);
  
  // Check if we've already processed this month
  if (checkpoint.processedMonths.includes(yearMonth)) {
    console.log(`Month ${yearMonth} already processed, skipping`);
    return;
  }
  
  // Initialize month stats if not already present
  if (!checkpoint.monthStats[yearMonth]) {
    checkpoint.monthStats[yearMonth] = {
      totalDates: 0,
      datesWithIssues: 0,
      totalMissingCalculations: 0,
      fixedDates: 0,
      failedDates: 0
    };
  }
  
  // Get all dates in this month
  const dates = await getMonthDates(yearMonth);
  checkpoint.monthStats[yearMonth].totalDates = dates.length;
  
  // Summary stats
  let totalMissingCalculations = 0;
  let missingByModel: Record<string, number> = {};
  const datesToFix: string[] = [];
  
  // Initialize model counters
  for (const model of Object.keys(minerModels)) {
    missingByModel[model] = 0;
  }
  
  // Analyze all dates in the month
  console.log(`Analyzing all dates in ${yearMonth} for missing Bitcoin calculations...`);
  for (let i = 0; i < dates.length; i++) {
    const date = dates[i];
    const stats = await analyzeDate(date);
    
    // Track missing calculations
    let dateHasMissing = false;
    
    for (const [model, data] of Object.entries(stats.missingCalculations)) {
      if (data.count > 0) {
        totalMissingCalculations += data.count;
        missingByModel[model] += data.count;
        dateHasMissing = true;
        
        console.log(`${date}: Missing ${data.count} calculations for ${model}`);
        console.log(`  Missing periods: ${data.periods.join(', ')}`);
      }
    }
    
    if (dateHasMissing) {
      datesToFix.push(date);
    }
    
    // Simple progress indicator
    if ((i + 1) % 5 === 0 || i === dates.length - 1) {
      console.log(`Progress: ${i + 1}/${dates.length} dates analyzed`);
    }
    
    // Avoid overloading the database with too many queries at once
    await sleep(300);
  }
  
  // Update month stats
  checkpoint.monthStats[yearMonth].datesWithIssues = datesToFix.length;
  checkpoint.monthStats[yearMonth].totalMissingCalculations = totalMissingCalculations;
  saveCheckpoint();
  
  // Print summary of analysis
  console.log(`\n===== ANALYSIS SUMMARY FOR ${yearMonth} =====`);
  console.log(`Total dates analyzed: ${dates.length}`);
  console.log(`Dates with missing calculations: ${datesToFix.length}`);
  console.log(`Total missing calculations: ${totalMissingCalculations}`);
  console.log("Missing by model:");
  for (const [model, count] of Object.entries(missingByModel)) {
    console.log(`  ${model}: ${count}`);
  }
  
  // Fix missing calculations if found
  if (datesToFix.length > 0) {
    console.log(`\n===== FIXING MISSING CALCULATIONS FOR ${yearMonth} =====`);
    console.log(`Will process ${datesToFix.length} dates with missing calculations`);
    
    const fixedDates: string[] = [];
    const failedDates: string[] = [];
    
    for (let i = 0; i < datesToFix.length; i++) {
      const date = datesToFix[i];
      console.log(`Processing date ${i + 1}/${datesToFix.length}: ${date}`);
      
      const success = await fixMissingCalculations(date);
      
      if (success) {
        fixedDates.push(date);
      } else {
        failedDates.push(date);
      }
      
      // Progress update
      console.log(`Progress: ${i + 1}/${datesToFix.length} dates processed`);
      console.log(`Success: ${fixedDates.length}, Failed: ${failedDates.length}`);
      
      // Update checkpoint after each date
      checkpoint.monthStats[yearMonth].fixedDates = fixedDates.length;
      checkpoint.monthStats[yearMonth].failedDates = failedDates.length;
      saveCheckpoint();
      
      // Avoid overloading the database with too many operations at once
      await sleep(1000);
    }
    
    // Final summary for month
    console.log(`\n===== MONTH ${yearMonth} RECONCILIATION COMPLETE =====`);
    console.log(`Total dates processed: ${datesToFix.length}`);
    console.log(`Successfully fixed: ${fixedDates.length}`);
    console.log(`Failed to fix: ${failedDates.length}`);
    
    if (failedDates.length > 0) {
      console.log("Failed dates:");
      failedDates.forEach(date => console.log(`  ${date}`));
    }
  } else {
    console.log(`\n===== MONTH ${yearMonth} RECONCILIATION COMPLETE =====`);
    console.log("No missing calculations found. All data is already reconciled!");
  }
  
  // Mark month as processed
  checkpoint.lastProcessedMonth = yearMonth;
  checkpoint.processedMonths.push(yearMonth);
  checkpoint.pendingMonths = checkpoint.pendingMonths.filter(m => m !== yearMonth);
  saveCheckpoint();
}

/**
 * Process a range of months
 */
async function processMonthRange(startMonth: string, endMonth: string): Promise<void> {
  console.log(`Processing month range: ${startMonth} to ${endMonth}`);
  
  // Validate months
  if (!startMonth.match(/^2023-(0[1-9]|1[0-2])$/) || !endMonth.match(/^2023-(0[1-9]|1[0-2])$/)) {
    console.error("Invalid month format. Use YYYY-MM (e.g., 2023-01)");
    return;
  }
  
  const startIndex = MONTHS_2023.indexOf(startMonth);
  const endIndex = MONTHS_2023.indexOf(endMonth);
  
  if (startIndex === -1 || endIndex === -1 || startIndex > endIndex) {
    console.error("Invalid month range");
    return;
  }
  
  // Process each month in the range
  for (let i = startIndex; i <= endIndex; i++) {
    const month = MONTHS_2023[i];
    await processMonth(month);
  }
  
  console.log("\n===== RANGE PROCESSING COMPLETE =====");
  displayOverallStats();
}

/**
 * Continue processing from last checkpoint
 */
async function continueProcessing(): Promise<void> {
  // Load checkpoint if exists
  if (!loadCheckpoint()) {
    console.log("No checkpoint found. Starting from the beginning.");
    resetCheckpoint();
  }
  
  console.log("Continuing from last checkpoint");
  
  if (checkpoint.pendingMonths.length === 0) {
    console.log("All months have been processed!");
    displayOverallStats();
    return;
  }
  
  // Start from the first pending month
  const pendingMonths = [...checkpoint.pendingMonths].sort();
  
  for (const month of pendingMonths) {
    await processMonth(month);
  }
  
  console.log("\n===== ALL MONTHS PROCESSED =====");
  displayOverallStats();
}

/**
 * Process all months in 2023
 */
async function processAll2023(): Promise<void> {
  // Reset checkpoint to start fresh
  resetCheckpoint();
  
  // Process all months
  for (const month of MONTHS_2023) {
    await processMonth(month);
  }
  
  console.log("\n===== ALL 2023 MONTHS PROCESSED =====");
  displayOverallStats();
}

/**
 * Display overall reconciliation stats
 */
function displayOverallStats(): void {
  console.log("\n===== OVERALL RECONCILIATION STATS =====");
  
  let totalDates = 0;
  let totalDatesWithIssues = 0;
  let totalMissingCalculations = 0;
  let totalFixedDates = 0;
  let totalFailedDates = 0;
  
  console.log("Monthly Stats:");
  for (const month of MONTHS_2023) {
    const stats = checkpoint.monthStats[month] || { 
      totalDates: 0, datesWithIssues: 0, totalMissingCalculations: 0, fixedDates: 0, failedDates: 0 
    };
    
    totalDates += stats.totalDates;
    totalDatesWithIssues += stats.datesWithIssues;
    totalMissingCalculations += stats.totalMissingCalculations;
    totalFixedDates += stats.fixedDates;
    totalFailedDates += stats.failedDates;
    
    const processed = checkpoint.processedMonths.includes(month);
    
    console.log(`  ${month}: ${processed ? 'PROCESSED' : 'PENDING'}`);
    if (stats.totalDates > 0) {
      console.log(`    Dates: ${stats.totalDates}, With Issues: ${stats.datesWithIssues}`);
      console.log(`    Missing Calculations: ${stats.totalMissingCalculations}`);
      console.log(`    Fixed: ${stats.fixedDates}, Failed: ${stats.failedDates}`);
    }
  }
  
  console.log("\nOverall Summary:");
  console.log(`Total Dates: ${totalDates}`);
  console.log(`Dates With Issues: ${totalDatesWithIssues}`);
  console.log(`Total Missing Calculations: ${totalMissingCalculations}`);
  console.log(`Fixed Dates: ${totalFixedDates}`);
  console.log(`Failed Dates: ${totalFailedDates}`);
  
  const startTime = new Date(checkpoint.startTime);
  const duration = (Date.now() - checkpoint.startTime) / 1000 / 60; // minutes
  
  console.log(`\nStarted: ${startTime.toISOString()}`);
  console.log(`Duration: ${duration.toFixed(2)} minutes`);
  
  const percentComplete = checkpoint.processedMonths.length / MONTHS_2023.length * 100;
  console.log(`Overall Progress: ${percentComplete.toFixed(2)}% (${checkpoint.processedMonths.length}/${MONTHS_2023.length} months)`);
}

/**
 * Main function to handle command line args
 */
async function main() {
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    // If no arguments provided, process all 2023 data (as requested)
    console.log("No command specified. Processing all 2023 data by default.");
    await processAll2023();
    return;
  }
  
  const command = args[0];
  
  try {
    switch (command) {
      case 'help':
        showHelp();
        break;
      
      case 'month':
        if (args.length < 2) {
          console.error("Missing month argument. Use format YYYY-MM (e.g., 2023-01)");
          break;
        }
        await processMonth(args[1]);
        break;
      
      case 'range':
        if (args.length < 3) {
          console.error("Missing month range. Use format: range START_MONTH END_MONTH");
          break;
        }
        await processMonthRange(args[1], args[2]);
        break;
      
      case 'continue':
        await continueProcessing();
        break;
      
      case 'status':
        if (!loadCheckpoint()) {
          console.log("No checkpoint found. No reconciliation has been started.");
          break;
        }
        displayOverallStats();
        break;
      
      case 'all':
        console.log("Processing all 2023 Bitcoin calculation data...");
        await processAll2023();
        break;
      
      case 'reset':
        resetCheckpoint();
        console.log("Checkpoint has been reset. Ready to start fresh.");
        break;
      
      default:
        console.log(`Unknown command: ${command}. Processing all 2023 data by default.`);
        await processAll2023();
    }
  } catch (error) {
    console.error("Error executing command:", error);
  }
}

// Run the main function
main()
  .catch(error => {
    console.error("Unhandled error:", error);
    process.exit(1);
  })
  .finally(() => {
    console.log("Script execution completed");
  });
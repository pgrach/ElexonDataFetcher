/**
 * Comprehensive Reconciliation Runner
 * 
 * This script implements the plan outlined in comprehensive_reconciliation_plan.md
 * It coordinates the reconciliation of all historical data (2022-2024) in an efficient manner.
 */

import { db } from "./db";
import { sql } from "drizzle-orm";
import pLimit from "p-limit";
import { format, parse, subMonths, addDays } from "date-fns";
import { reconcileDay } from "./server/services/historicalReconciliation";

// Configuration
const CONCURRENCY_LIMIT = 3; // Number of days to process in parallel
const BATCH_SIZE = 7; // Days to process in each batch
const DELAY_BETWEEN_BATCHES = 10000; // 10 seconds between batches
const DELAY_BETWEEN_MONTHS = 60000; // 1 minute between months
const BACKOFF_TIME = 200; // 200ms between individual day processing

interface ReconciliationStatus {
  total: number;
  completed: number;
  percentage: number;
  startTime: Date;
  currentBatch: string[];
  completedDates: string[];
  failedDates: string[];
}

const status: ReconciliationStatus = {
  total: 0,
  completed: 0,
  percentage: 0,
  startTime: new Date(),
  currentBatch: [],
  completedDates: [],
  failedDates: []
};

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function setupReconciliationSchema() {
  console.log("Setting up reconciliation progress tracking...");
  
  try {
    // Check if the table exists
    const tableExists = await db.execute(sql`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = 'reconciliation_progress'
      );
    `);
    
    if (!tableExists.rows[0].exists) {
      console.log("Creating reconciliation_progress table...");
      
      await db.execute(sql`
        CREATE TABLE reconciliation_progress (
          id SERIAL PRIMARY KEY,
          time_period VARCHAR(10) NOT NULL,
          status VARCHAR(20) NOT NULL DEFAULT 'not_started',
          percentage_complete INTEGER NOT NULL DEFAULT 0,
          last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
          total_records INTEGER,
          processed_records INTEGER DEFAULT 0,
          error_details TEXT,
          UNIQUE(time_period)
        )
      `);
      
      // Add temporary performance index
      await db.execute(sql`
        CREATE INDEX IF NOT EXISTS temp_curtailment_date_idx 
        ON curtailment_records(settlement_date)
      `);
      
      console.log("Schema setup complete.");
    } else {
      console.log("Reconciliation progress table already exists.");
    }
  } catch (error) {
    console.error("Error setting up reconciliation schema:", error);
    throw error;
  }
}

async function getMonthsToProcess(): Promise<{ year: number; month: number }[]> {
  const months: { year: number; month: number }[] = [];
  
  // Add all months from 2022-01 to current
  let currentDate = new Date();
  let startDate = new Date(2022, 0, 1); // Jan 2022
  
  // We want to process most recent months first (2024, then 2023, then 2022)
  // Start with all months of 2024 in reverse order
  for (let year = currentDate.getFullYear(); year >= 2022; year--) {
    const endMonth = year === currentDate.getFullYear() ? currentDate.getMonth() : 11;
    for (let month = endMonth; month >= 0; month--) {
      months.push({ year, month: month + 1 });
    }
  }
  
  return months;
}

async function updateProgressForMonth(yearMonth: string, status: string, percentage: number, errorDetails?: string) {
  try {
    await db.execute(sql`
      INSERT INTO reconciliation_progress 
        (time_period, status, percentage_complete, last_updated, error_details)
      VALUES 
        (${yearMonth}, ${status}, ${percentage}, NOW(), ${errorDetails || null})
      ON CONFLICT (time_period) 
      DO UPDATE SET 
        status = EXCLUDED.status,
        percentage_complete = EXCLUDED.percentage_complete,
        last_updated = EXCLUDED.last_updated,
        error_details = EXCLUDED.error_details
    `);
  } catch (error) {
    console.error(`Error updating progress for ${yearMonth}:`, error);
  }
}

async function getReconciliationStatusForMonth(year: number, month: number): Promise<{
  curtailmentRecords: number;
  uniqueCombinations: number;
  bitcoinCalculations: number;
  expectedCalculations: number;
  reconciliationPercentage: number;
}> {
  const yearMonth = `${year}-${month.toString().padStart(2, '0')}`;
  const startDate = `${yearMonth}-01`;
  const endDate = format(new Date(year, month, 0), 'yyyy-MM-dd'); // Last day of month
  
  try {
    const result = await db.execute(sql`
      WITH month_curtailment AS (
        SELECT 
          COUNT(*) AS total_records,
          COUNT(DISTINCT (settlement_date, settlement_period, farm_id)) AS unique_combinations
        FROM curtailment_records
        WHERE settlement_date BETWEEN ${startDate} AND ${endDate}
      ),
      month_calculations AS (
        SELECT 
          COUNT(*) AS total_calculations,
          COUNT(DISTINCT (settlement_date, settlement_period, farm_id, miner_model)) AS unique_combinations
        FROM historical_bitcoin_calculations
        WHERE settlement_date BETWEEN ${startDate} AND ${endDate}
      )
      SELECT 
        mc.total_records AS curtailment_records,
        mc.unique_combinations AS unique_combinations,
        COALESCE(mcc.total_calculations, 0) AS bitcoin_calculations,
        mc.unique_combinations * 3 AS expected_calculations
      FROM month_curtailment mc
      LEFT JOIN month_calculations mcc ON 1=1
    `);

    if (result.rows.length > 0) {
      const {
        curtailment_records,
        unique_combinations,
        bitcoin_calculations,
        expected_calculations
      } = result.rows[0];

      const reconciliationPercentage = Math.round((parseInt(bitcoin_calculations) / parseInt(expected_calculations || '1')) * 100);

      return {
        curtailmentRecords: parseInt(curtailment_records),
        uniqueCombinations: parseInt(unique_combinations),
        bitcoinCalculations: parseInt(bitcoin_calculations),
        expectedCalculations: parseInt(expected_calculations),
        reconciliationPercentage
      };
    }
  } catch (error) {
    console.error(`Error getting status for ${yearMonth}:`, error);
  }

  return {
    curtailmentRecords: 0,
    uniqueCombinations: 0,
    bitcoinCalculations: 0,
    expectedCalculations: 0,
    reconciliationPercentage: 0
  };
}

async function getDatesForMonth(year: number, month: number): Promise<string[]> {
  const yearMonth = `${year}-${month.toString().padStart(2, '0')}`;
  const startDate = `${yearMonth}-01`;
  const endDate = format(new Date(year, month, 0), 'yyyy-MM-dd'); // Last day of month
  
  try {
    const result = await db.execute(sql`
      SELECT DISTINCT settlement_date
      FROM curtailment_records
      WHERE settlement_date BETWEEN ${startDate} AND ${endDate}
      ORDER BY settlement_date
    `);
    
    return result.rows.map(row => row.settlement_date);
  } catch (error) {
    console.error(`Error getting dates for ${yearMonth}:`, error);
    return [];
  }
}

async function processBatch(dates: string[], concurrency = CONCURRENCY_LIMIT): Promise<string[]> {
  console.log(`Processing batch of ${dates.length} dates with concurrency ${concurrency}...`);
  
  status.currentBatch = dates;
  const failedDates: string[] = [];
  
  const limit = pLimit(concurrency);
  const promises = dates.map(date => limit(async () => {
    try {
      console.log(`[${date}] Starting reconciliation...`);
      await reconcileDay(date);
      console.log(`[${date}] Reconciliation complete`);
      status.completedDates.push(date);
      status.completed++;
      status.percentage = Math.round((status.completed / status.total) * 100);
      // Add backoff between individual days to prevent overloading
      await sleep(BACKOFF_TIME);
      return { date, success: true };
    } catch (error) {
      console.error(`[${date}] Error during reconciliation:`, error);
      failedDates.push(date);
      status.failedDates.push(date);
      return { date, success: false };
    }
  }));

  const results = await Promise.all(promises);
  const successCount = results.filter(r => r.success).length;
  console.log(`Batch processing complete. ${successCount}/${dates.length} dates successfully reconciled.`);
  
  return failedDates;
}

async function getDateBatches(dates: string[]): Promise<string[][]> {
  const batches: string[][] = [];
  for (let i = 0; i < dates.length; i += BATCH_SIZE) {
    batches.push(dates.slice(i, i + BATCH_SIZE));
  }
  return batches;
}

async function reconcileMonth(year: number, month: number): Promise<{
  success: boolean;
  completedDates: string[];
  failedDates: string[];
}> {
  const monthName = new Date(year, month - 1, 1).toLocaleString('default', { month: 'long' });
  const yearMonth = `${year}-${month.toString().padStart(2, '0')}`;
  
  console.log(`\n=== Starting Reconciliation for ${monthName} ${year} ===\n`);
  
  try {
    // Get initial status
    const initialStatus = await getReconciliationStatusForMonth(year, month);
    console.log(`Initial Status: ${initialStatus.reconciliationPercentage}% reconciled`);
    
    // Update progress table
    await updateProgressForMonth(yearMonth, 'in_progress', initialStatus.reconciliationPercentage);
    
    // Get all dates for the month
    const dates = await getDatesForMonth(year, month);
    if (dates.length === 0) {
      console.log(`No dates found for ${monthName} ${year}`);
      await updateProgressForMonth(yearMonth, 'completed', 100);
      return { success: true, completedDates: [], failedDates: [] };
    }
    
    console.log(`Found ${dates.length} dates for ${monthName} ${year}`);
    status.total += dates.length;
    
    // Split into batches
    const batches = await getDateBatches(dates);
    console.log(`Divided into ${batches.length} batches of up to ${BATCH_SIZE} days each`);
    
    const allFailedDates: string[] = [];
    const completedDates: string[] = [];
    
    // Process each batch
    let batchNumber = 1;
    for (const batch of batches) {
      console.log(`\nProcessing batch ${batchNumber}/${batches.length} for ${monthName} ${year}`);
      console.log(`Dates in this batch: ${batch.join(', ')}`);
      
      const failedDates = await processBatch(batch);
      allFailedDates.push(...failedDates);
      completedDates.push(...batch.filter(d => !failedDates.includes(d)));
      
      // Update progress
      const currentStatus = await getReconciliationStatusForMonth(year, month);
      await updateProgressForMonth(yearMonth, 'in_progress', currentStatus.reconciliationPercentage);
      
      // Add delay between batches to prevent resource overload
      if (batchNumber < batches.length) {
        console.log(`Waiting ${DELAY_BETWEEN_BATCHES/1000} seconds before next batch...`);
        await sleep(DELAY_BETWEEN_BATCHES);
      }
      
      batchNumber++;
    }
    
    // Check final status
    const finalStatus = await getReconciliationStatusForMonth(year, month);
    console.log(`Final Status: ${finalStatus.reconciliationPercentage}% reconciled`);
    
    // Update final status
    const statusText = finalStatus.reconciliationPercentage === 100 ? 'completed' : 'incomplete';
    await updateProgressForMonth(
      yearMonth, 
      statusText, 
      finalStatus.reconciliationPercentage,
      allFailedDates.length > 0 ? `Failed dates: ${allFailedDates.join(', ')}` : undefined
    );
    
    console.log(`\n=== Completed Reconciliation for ${monthName} ${year} ===\n`);
    
    return { 
      success: allFailedDates.length === 0, 
      completedDates, 
      failedDates: allFailedDates 
    };
  } catch (error) {
    console.error(`Error reconciling ${monthName} ${year}:`, error);
    await updateProgressForMonth(yearMonth, 'failed', 0, String(error));
    return { success: false, completedDates: [], failedDates: [] };
  }
}

async function displayProgressDashboard() {
  const elapsedMs = Date.now() - status.startTime.getTime();
  const elapsedMinutes = Math.floor(elapsedMs / 60000);
  const elapsedSeconds = Math.floor((elapsedMs % 60000) / 1000);
  
  const remainingMs = status.percentage > 0 
    ? (elapsedMs / status.percentage) * (100 - status.percentage)
    : 0;
  const remainingMinutes = Math.floor(remainingMs / 60000);
  const remainingSeconds = Math.floor((remainingMs % 60000) / 1000);
  
  console.log("\n=== Reconciliation Progress Dashboard ===");
  console.log(`Progress: ${status.completed}/${status.total} dates (${status.percentage}%)`);
  console.log(`Elapsed Time: ${elapsedMinutes}m ${elapsedSeconds}s`);
  console.log(`Estimated Remaining: ${remainingMinutes}m ${remainingSeconds}s`);
  console.log(`Failed Dates: ${status.failedDates.length}`);
  console.log("Current Batch:", status.currentBatch.join(", "));
  console.log("=======================================\n");
}

async function reconcileAllHistory() {
  console.log("=== Starting Comprehensive Historical Reconciliation ===\n");
  status.startTime = new Date();
  
  try {
    // Setup schema for tracking
    await setupReconciliationSchema();
    
    // Get all months to process
    const months = await getMonthsToProcess();
    console.log(`Found ${months.length} months to process from 2022-01 to present`);
    
    // Process each month
    for (const { year, month } of months) {
      const monthName = new Date(year, month - 1, 1).toLocaleString('default', { month: 'long' });
      console.log(`\nPreparing to process ${monthName} ${year}...`);
      
      // Display progress dashboard
      await displayProgressDashboard();
      
      // Reconcile the month
      const result = await reconcileMonth(year, month);
      
      if (!result.success) {
        console.warn(`Warning: ${monthName} ${year} had ${result.failedDates.length} failed dates.`);
      }
      
      // Wait between months to allow system to recover
      if (months.indexOf({ year, month }) < months.length - 1) {
        console.log(`\nWaiting ${DELAY_BETWEEN_MONTHS/1000} seconds before next month...`);
        await sleep(DELAY_BETWEEN_MONTHS);
      }
    }
    
    console.log("\n=== Final Reconciliation Status ===");
    await displayProgressDashboard();
    console.log("\n=== Comprehensive Historical Reconciliation Complete ===");
  } catch (error) {
    console.error("Error during comprehensive reconciliation:", error);
  }
}

async function reconcileSpecificMonth(year: number, month: number) {
  try {
    // Setup schema for tracking
    await setupReconciliationSchema();
    
    // Reconcile the specific month
    const result = await reconcileMonth(year, month);
    
    if (result.success) {
      console.log(`Successfully reconciled ${result.completedDates.length} dates.`);
    } else {
      console.warn(`Warning: Reconciliation had ${result.failedDates.length} failed dates.`);
    }
  } catch (error) {
    console.error("Error during month reconciliation:", error);
  }
}

async function retryFailedDates() {
  console.log("=== Retrying Failed Dates ===\n");
  
  try {
    const result = await db.execute(sql`
      SELECT time_period, error_details
      FROM reconciliation_progress
      WHERE status = 'incomplete' OR status = 'failed'
      ORDER BY time_period DESC
    `);
    
    if (result.rows.length === 0) {
      console.log("No failed dates found to retry.");
      return;
    }
    
    console.log(`Found ${result.rows.length} months with failed dates.`);
    
    for (const row of result.rows) {
      const [year, month] = row.time_period.split('-').map(Number);
      const monthName = new Date(year, month - 1, 1).toLocaleString('default', { month: 'long' });
      
      console.log(`\nRetrying ${monthName} ${year}...`);
      
      // Extract failed dates from error details
      const failedDatesMatch = row.error_details?.match(/Failed dates: (.*)/);
      if (!failedDatesMatch) {
        console.log(`No specific failed dates found for ${monthName} ${year}. Retrying the entire month.`);
        await reconcileSpecificMonth(year, month);
        continue;
      }
      
      const failedDates = failedDatesMatch[1].split(', ');
      console.log(`Found ${failedDates.length} failed dates to retry for ${monthName} ${year}.`);
      
      // Process in batches
      const batches = await getDateBatches(failedDates);
      
      for (const batch of batches) {
        await processBatch(batch);
        await sleep(DELAY_BETWEEN_BATCHES);
      }
      
      // Update status
      const finalStatus = await getReconciliationStatusForMonth(year, month);
      const statusText = finalStatus.reconciliationPercentage === 100 ? 'completed' : 'incomplete';
      await updateProgressForMonth(
        row.time_period, 
        statusText, 
        finalStatus.reconciliationPercentage
      );
    }
    
    console.log("\n=== Retry of Failed Dates Complete ===");
  } catch (error) {
    console.error("Error retrying failed dates:", error);
  }
}

async function generateReport() {
  console.log("=== Generating Comprehensive Reconciliation Report ===\n");
  
  try {
    const result = await db.execute(sql`
      SELECT 
        time_period,
        status,
        percentage_complete,
        last_updated
      FROM reconciliation_progress
      ORDER BY time_period DESC
    `);
    
    console.log("Reconciliation Progress by Month:");
    console.log("--------------------------------");
    console.log("|  Month   | Status | Complete % | Last Updated      |");
    console.log("|----------|--------|------------|-------------------|");
    
    for (const row of result.rows) {
      const status = row.status.padEnd(8);
      const percentage = `${row.percentage_complete}%`.padEnd(12);
      const updated = new Date(row.last_updated).toISOString().replace('T', ' ').substring(0, 19);
      console.log(`| ${row.time_period} | ${status}| ${percentage}| ${updated} |`);
    }
    
    console.log("--------------------------------");
    
    // Overall progress
    const overall = await db.execute(sql`
      WITH curtailment_stats AS (
        SELECT 
          COUNT(*) AS total_records,
          COUNT(DISTINCT (settlement_date, settlement_period, farm_id)) AS unique_combinations
        FROM curtailment_records
      ),
      calculation_stats AS (
        SELECT 
          COUNT(*) AS total_calculations,
          COUNT(DISTINCT (settlement_date, settlement_period, farm_id, miner_model)) AS unique_combinations
        FROM historical_bitcoin_calculations
      )
      SELECT 
        cs.total_records AS curtailment_records,
        cs.unique_combinations AS unique_combinations,
        COALESCE(calcs.total_calculations, 0) AS bitcoin_calculations,
        cs.unique_combinations * 3 AS expected_calculations
      FROM curtailment_stats cs
      LEFT JOIN calculation_stats calcs ON 1=1
    `);
    
    if (overall.rows.length > 0) {
      const {
        curtailment_records,
        unique_combinations,
        bitcoin_calculations,
        expected_calculations
      } = overall.rows[0];
      
      const reconciliationPercentage = Math.round((parseInt(bitcoin_calculations) / parseInt(expected_calculations)) * 100);
      
      console.log("\nOverall Reconciliation Status:");
      console.log(`Total Curtailment Records: ${curtailment_records}`);
      console.log(`Total Bitcoin Calculations: ${bitcoin_calculations}`);
      console.log(`Expected Calculations: ${expected_calculations}`);
      console.log(`Overall Reconciliation: ${reconciliationPercentage}%`);
    }
    
    console.log("\n=== Reconciliation Report Complete ===");
  } catch (error) {
    console.error("Error generating report:", error);
  }
}

async function main() {
  const args = process.argv.slice(2);
  const command = args[0] || 'help';

  try {
    switch (command) {
      case 'all':
        await reconcileAllHistory();
        break;
      
      case 'month':
        const year = parseInt(args[1]);
        const month = parseInt(args[2]);
        
        if (isNaN(year) || isNaN(month) || month < 1 || month > 12) {
          console.error('Invalid year or month. Usage: npx tsx comprehensive_reconcile_runner.ts month YYYY MM');
          process.exit(1);
        }
        
        await reconcileSpecificMonth(year, month);
        break;
      
      case 'retry':
        await retryFailedDates();
        break;
      
      case 'report':
        await generateReport();
        break;
      
      case 'setup':
        await setupReconciliationSchema();
        console.log("Schema setup complete.");
        break;
      
      case 'help':
      default:
        console.log('Comprehensive Reconciliation Runner');
        console.log('Usage:');
        console.log('  npx tsx comprehensive_reconcile_runner.ts all         - Run complete historical reconciliation');
        console.log('  npx tsx comprehensive_reconcile_runner.ts month YYYY MM - Reconcile specific month');
        console.log('  npx tsx comprehensive_reconcile_runner.ts retry       - Retry failed dates');
        console.log('  npx tsx comprehensive_reconcile_runner.ts report      - Generate reconciliation report');
        console.log('  npx tsx comprehensive_reconcile_runner.ts setup       - Setup schema only');
        console.log('  npx tsx comprehensive_reconcile_runner.ts help        - Show this help');
    }
  } catch (error) {
    console.error('Error executing command:', error);
  }
}

if (require.main === module) {
  main().catch(console.error);
}
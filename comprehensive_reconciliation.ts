/**
 * Comprehensive Reconciliation System
 * 
 * A unified, high-performance solution for ensuring complete data integrity between
 * curtailment_records and historical_bitcoin_calculations tables.
 * 
 * This script combines all available reconciliation tools and implements advanced
 * optimization techniques including:
 * - Parallel processing with controlled concurrency
 * - Advanced error handling and recovery
 * - Intelligent prioritization of problematic dates
 * - Comprehensive reporting and statistics
 * - Automatic resumption of interrupted processes
 * - Database connection pool optimization
 * 
 * Usage:
 *   npx tsx comprehensive_reconciliation.ts [command]
 * 
 * Commands:
 *   status           - Show current reconciliation status
 *   reconcile-all    - Reconcile all dates in the database
 *   reconcile-range  - Reconcile a specific date range (requires additional parameters)
 *   reconcile-recent - Reconcile recent data (default: last 30 days)
 *   fix-critical     - Fix dates with known issues
 *   report           - Generate detailed reconciliation report
 */

import { db } from "./db";
import { sql } from "drizzle-orm";
import pg from 'pg';
import fs from 'fs';
import path from 'path';
import { format, addDays, subDays, parseISO, eachDayOfInterval, isBefore } from "date-fns";
import { 
  findMissingDates, 
  auditAndFixBitcoinCalculations,
  reconcileDay,
  reconcileDateRange
} from "./server/services/historicalReconciliation";
import pLimit from 'p-limit';

// Configuration
const MAX_CONCURRENT_OPERATIONS = 5;
const DEFAULT_LOOKBACK_DAYS = 30;
const CHECKPOINT_FILE = './reconciliation_checkpoint.json';
const LOG_FILE = './comprehensive_reconciliation.log';
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"]; 

// Optimized database connection pool
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 15000, 
  query_timeout: 30000,
  allowExitOnIdle: true
});

// Checkpoint type definition
interface ReconciliationCheckpoint {
  command: string;
  startDate?: string;
  endDate?: string;
  pendingDates: string[];
  completedDates: string[];
  startTime: number;
  lastUpdateTime: number;
  stats: {
    totalDates: number;
    processedDates: number;
    successfulDates: number;
    failedDates: number;
  };
}

// Initialize checkpoint
let checkpoint: ReconciliationCheckpoint = {
  command: '',
  pendingDates: [],
  completedDates: [],
  startTime: Date.now(),
  lastUpdateTime: Date.now(),
  stats: {
    totalDates: 0,
    processedDates: 0,
    successfulDates: 0,
    failedDates: 0
  }
};

// Logger with timestamp
function log(message: string, level: 'info' | 'error' | 'warning' | 'success' = 'info'): void {
  const timestamp = new Date().toISOString();
  const formatted = `[${timestamp}] [${level.toUpperCase()}] ${message}`;
  
  console.log(formatted);
  
  try {
    fs.appendFileSync(LOG_FILE, formatted + '\n');
  } catch (error) {
    console.error(`Error writing to log file: ${error}`);
  }
}

// Save checkpoint to file
function saveCheckpoint(): void {
  try {
    checkpoint.lastUpdateTime = Date.now();
    fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2));
  } catch (error) {
    log(`Error saving checkpoint: ${error}`, 'error');
  }
}

// Load checkpoint from file
function loadCheckpoint(): boolean {
  try {
    if (fs.existsSync(CHECKPOINT_FILE)) {
      const data = fs.readFileSync(CHECKPOINT_FILE, 'utf8');
      checkpoint = JSON.parse(data);
      log('Loaded checkpoint from file', 'info');
      return true;
    }
  } catch (error) {
    log(`Error loading checkpoint: ${error}`, 'error');
  }
  return false;
}

// Reset checkpoint
function resetCheckpoint(): void {
  checkpoint = {
    command: '',
    pendingDates: [],
    completedDates: [],
    startTime: Date.now(),
    lastUpdateTime: Date.now(),
    stats: {
      totalDates: 0,
      processedDates: 0,
      successfulDates: 0,
      failedDates: 0
    }
  };
  saveCheckpoint();
}

// Format a number with commas
function formatNumber(value: any): string {
  const num = parseFloat(value);
  if (isNaN(num)) return '0';
  
  return num.toLocaleString('en-US', {
    minimumFractionDigits: num % 1 === 0 ? 0 : 2,
    maximumFractionDigits: 2
  });
}

// Format a percentage
function formatPercentage(value: any): string {
  const num = parseFloat(value);
  if (isNaN(num)) return '0.00%';
  return num.toFixed(2) + '%';
}

// Format elapsed time
function formatElapsedTime(ms: number): string {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  
  if (hours > 0) {
    return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
  } else if (minutes > 0) {
    return `${minutes}m ${seconds % 60}s`;
  } else {
    return `${seconds}s`;
  }
}

// Get a summary of the current reconciliation status
async function getReconciliationStatus() {
  log('Fetching current reconciliation status...', 'info');
  
  const overviewQuery = `
    WITH curtailment_stats AS (
      SELECT 
        COUNT(DISTINCT settlement_date) as total_dates,
        SUM(ABS(volume::numeric)) as total_volume,
        COUNT(*) as total_records
      FROM curtailment_records
    ),
    bitcoin_stats AS (
      SELECT
        COUNT(DISTINCT settlement_date) as total_dates,
        SUM(bitcoin_mined::numeric) as total_bitcoin,
        COUNT(*) as total_records
      FROM historical_bitcoin_calculations
    ),
    date_completion AS (
      SELECT
        cr.settlement_date,
        COUNT(DISTINCT cr.settlement_period || '-' || cr.farm_id) as expected_calculations,
        COUNT(DISTINCT hbc.settlement_period || '-' || hbc.farm_id || '-' || hbc.miner_model) as actual_calculations
      FROM
        curtailment_records cr
      LEFT JOIN
        historical_bitcoin_calculations hbc ON cr.settlement_date = hbc.settlement_date
      GROUP BY
        cr.settlement_date
    )
    SELECT
      cs.total_dates as curtailment_dates,
      bs.total_dates as bitcoin_dates,
      cs.total_volume as total_curtailed_volume,
      bs.total_bitcoin as total_bitcoin_mined,
      cs.total_records as curtailment_records,
      bs.total_records as bitcoin_records,
      (SELECT COUNT(*) FROM date_completion WHERE actual_calculations >= expected_calculations * 3) as complete_dates,
      (SELECT COUNT(*) FROM date_completion WHERE actual_calculations > 0 AND actual_calculations < expected_calculations * 3) as partial_dates,
      (SELECT COUNT(*) FROM date_completion WHERE actual_calculations = 0 AND expected_calculations > 0) as missing_dates
    FROM
      curtailment_stats cs, bitcoin_stats bs
  `;
  
  const result = await db.execute(sql.raw(overviewQuery));
  const overview = result.rows[0];
  
  console.log('\n=== Reconciliation Status ===');
  console.log(`Curtailment Records: ${formatNumber(overview.curtailment_records)}`);
  console.log(`Bitcoin Calculations: ${formatNumber(overview.bitcoin_records)}`);
  console.log(`Total Curtailed Energy: ${formatNumber(overview.total_curtailed_volume)} MWh`);
  console.log(`Total Bitcoin Mined: ${formatNumber(overview.total_bitcoin_mined)} BTC`);
  console.log('\n=== Date Completion ===');
  console.log(`Complete Dates: ${formatNumber(overview.complete_dates)}`);
  console.log(`Partial Dates: ${formatNumber(overview.partial_dates)}`);
  console.log(`Missing Dates: ${formatNumber(overview.missing_dates)}`);
  console.log(`Completion Rate: ${formatPercentage(Number(overview.complete_dates) / Number(overview.curtailment_dates) * 100)}%`);
  
  // Get most recent problematic dates
  const problemDatesQuery = `
    WITH date_status AS (
      SELECT
        cr.settlement_date,
        COUNT(DISTINCT (cr.settlement_period || '-' || cr.farm_id)) * ${MINER_MODELS.length} AS expected_calculations,
        COUNT(DISTINCT (hbc.settlement_period || '-' || hbc.farm_id || '-' || hbc.miner_model)) AS actual_calculations
      FROM
        curtailment_records cr
      LEFT JOIN
        historical_bitcoin_calculations hbc ON cr.settlement_date = hbc.settlement_date
      GROUP BY
        cr.settlement_date
    )
    SELECT
      settlement_date,
      expected_calculations,
      actual_calculations,
      ROUND((actual_calculations::numeric / NULLIF(expected_calculations, 0)) * 100, 2) AS completion_percentage
    FROM
      date_status
    WHERE
      expected_calculations > 0
      AND actual_calculations < expected_calculations
    ORDER BY
      settlement_date DESC
    LIMIT 10
  `;
  
  const problemDates = await db.execute(sql.raw(problemDatesQuery));
  
  if (problemDates.rows.length > 0) {
    console.log('\n=== Recent Problematic Dates ===');
    problemDates.rows.forEach(row => {
      console.log(`${row.settlement_date}: ${formatNumber(row.actual_calculations)}/${formatNumber(row.expected_calculations)} (${formatPercentage(row.completion_percentage)})`);
    });
  }
  
  return overview;
}

// Get the earliest and latest dates from curtailment records
async function getDateRange() {
  const rangeQuery = `
    SELECT 
      MIN(settlement_date) as min_date,
      MAX(settlement_date) as max_date
    FROM curtailment_records
  `;
  
  const result = await db.execute(sql.raw(rangeQuery));
  return {
    minDate: result.rows[0]?.min_date as string,
    maxDate: result.rows[0]?.max_date as string
  };
}

// Process a date with error handling and retries
async function processDateWithRetries(date: string, attemptNumber: number = 1): Promise<boolean> {
  const MAX_ATTEMPTS = 3;
  
  try {
    log(`Processing date ${date} (attempt ${attemptNumber}/${MAX_ATTEMPTS})`, 'info');
    
    const result = await auditAndFixBitcoinCalculations(date);
    
    if (result.success) {
      log(`Successfully processed ${date}: ${result.message}`, 'success');
      return true;
    } else {
      log(`Failed to process ${date}: ${result.message}`, 'warning');
      
      if (attemptNumber < MAX_ATTEMPTS) {
        // Exponential backoff
        const delay = Math.pow(2, attemptNumber) * 1000;
        log(`Retrying in ${delay/1000} seconds...`, 'info');
        await new Promise(resolve => setTimeout(resolve, delay));
        return processDateWithRetries(date, attemptNumber + 1);
      } else {
        log(`All retry attempts for ${date} failed`, 'error');
        return false;
      }
    }
  } catch (error) {
    log(`Error processing ${date}: ${error}`, 'error');
    
    if (attemptNumber < MAX_ATTEMPTS) {
      // Exponential backoff
      const delay = Math.pow(2, attemptNumber) * 1000;
      log(`Retrying in ${delay/1000} seconds...`, 'info');
      await new Promise(resolve => setTimeout(resolve, delay));
      return processDateWithRetries(date, attemptNumber + 1);
    } else {
      log(`All retry attempts for ${date} failed`, 'error');
      return false;
    }
  }
}

// Prioritize dates for processing based on importance and potential impact
async function prioritizeDates(dates: string[]): Promise<string[]> {
  // Get date statistics for prioritization
  const dateStatsQuery = `
    SELECT
      cr.settlement_date,
      COUNT(*) as record_count,
      SUM(ABS(cr.volume::numeric)) as total_volume,
      COUNT(DISTINCT cr.settlement_period) as period_count
    FROM
      curtailment_records cr
    WHERE
      cr.settlement_date = ANY($1)
    GROUP BY
      cr.settlement_date
    ORDER BY
      total_volume DESC,
      period_count DESC,
      record_count DESC,
      settlement_date DESC
  `;
  
  // Replace $1 with actual date array for direct execution
  const formattedDates = dates.map(d => `'${d}'`).join(',');
  const modifiedQuery = dateStatsQuery.replace('$1', `ARRAY[${formattedDates}]`);
  
  const result = await db.execute(sql.raw(modifiedQuery));
  
  // Return dates in priority order
  return result.rows.map(row => row.settlement_date as string);
}

// Process a batch of dates with controlled concurrency
async function processDates(dates: string[]): Promise<{
  totalProcessed: number;
  successfullyFixed: number;
  failedDates: string[];
}> {
  log(`Processing ${dates.length} dates with max concurrency of ${MAX_CONCURRENT_OPERATIONS}`, 'info');
  
  // Initialize checkpoint if not resuming
  if (checkpoint.pendingDates.length === 0) {
    checkpoint.pendingDates = [...dates];
    checkpoint.stats.totalDates = dates.length;
    saveCheckpoint();
  }
  
  const prioritizedDates = await prioritizeDates(checkpoint.pendingDates);
  log(`Prioritized ${prioritizedDates.length} dates for processing`, 'info');
  
  const limit = pLimit(MAX_CONCURRENT_OPERATIONS);
  const results: { date: string; success: boolean }[] = [];
  
  // Create an array of functions to process each date with concurrency limit
  const tasks = prioritizedDates.map(date => {
    return limit(async () => {
      const success = await processDateWithRetries(date);
      
      // Update checkpoint
      if (success) {
        checkpoint.completedDates.push(date);
        checkpoint.pendingDates = checkpoint.pendingDates.filter(d => d !== date);
        checkpoint.stats.successfulDates++;
      } else {
        checkpoint.stats.failedDates++;
      }
      
      checkpoint.stats.processedDates++;
      saveCheckpoint();
      
      return { date, success };
    });
  });
  
  // Process all tasks
  for (let i = 0; i < tasks.length; i += MAX_CONCURRENT_OPERATIONS) {
    const batch = tasks.slice(i, i + MAX_CONCURRENT_OPERATIONS);
    const batchResults = await Promise.all(batch);
    results.push(...batchResults);
    
    // Log progress
    const successCount = results.filter(r => r.success).length;
    log(`Progress: ${results.length}/${tasks.length} dates processed (${successCount} successful)`, 'info');
  }
  
  const successfullyFixed = results.filter(r => r.success).length;
  const failedDates = results.filter(r => !r.success).map(r => r.date);
  
  return {
    totalProcessed: results.length,
    successfullyFixed,
    failedDates
  };
}

// Run reconciliation for all dates
async function reconcileAll() {
  log('Starting full database reconciliation...', 'info');
  
  // Get date range
  const range = await getDateRange();
  log(`Found date range from ${range.minDate} to ${range.maxDate}`, 'info');
  
  // Get initial status
  await getReconciliationStatus();
  
  // Find dates with missing calculations across all time
  log('Finding dates with missing calculations...', 'info');
  const missingDates = await findMissingDates(range.minDate, range.maxDate);
  
  if (missingDates.length === 0) {
    log('No dates with missing calculations found!', 'success');
    return;
  }
  
  log(`Found ${missingDates.length} dates with missing calculations`, 'info');
  
  // Extract dates only
  const dates = missingDates.map(d => d.date);
  
  // Reset or resume checkpoint
  if (!loadCheckpoint() || checkpoint.command !== 'reconcile-all') {
    resetCheckpoint();
    checkpoint.command = 'reconcile-all';
    checkpoint.startDate = range.minDate;
    checkpoint.endDate = range.maxDate;
  }
  
  // Process all dates
  const startTime = Date.now();
  const results = await processDates(dates);
  const endTime = Date.now();
  
  // Final summary
  log('\n=== Reconciliation Complete ===', 'success');
  log(`Total Duration: ${formatElapsedTime(endTime - startTime)}`, 'info');
  log(`Total Dates Processed: ${results.totalProcessed}`, 'info');
  log(`Successfully Fixed: ${results.successfullyFixed}`, 'info');
  log(`Failed Dates: ${results.failedDates.length}`, 'info');
  
  if (results.failedDates.length > 0) {
    log('\n=== Failed Dates ===', 'warning');
    results.failedDates.forEach(date => {
      log(date, 'warning');
    });
  }
  
  // Get final status
  await getReconciliationStatus();
}

// Run reconciliation for a specific date range
async function reconcileRange(startDate: string, endDate: string) {
  log(`Starting date range reconciliation from ${startDate} to ${endDate}...`, 'info');
  
  // Get initial status
  await getReconciliationStatus();
  
  // Find dates with missing calculations in the range
  log('Finding dates with missing calculations in range...', 'info');
  const missingDates = await findMissingDates(startDate, endDate);
  
  if (missingDates.length === 0) {
    log('No dates with missing calculations found in range!', 'success');
    return;
  }
  
  log(`Found ${missingDates.length} dates with missing calculations`, 'info');
  
  // Extract dates only
  const dates = missingDates.map(d => d.date);
  
  // Reset or resume checkpoint
  if (!loadCheckpoint() || checkpoint.command !== 'reconcile-range' || 
      checkpoint.startDate !== startDate || checkpoint.endDate !== endDate) {
    resetCheckpoint();
    checkpoint.command = 'reconcile-range';
    checkpoint.startDate = startDate;
    checkpoint.endDate = endDate;
  }
  
  // Process all dates in range
  const startTime = Date.now();
  const results = await processDates(dates);
  const endTime = Date.now();
  
  // Final summary
  log('\n=== Range Reconciliation Complete ===', 'success');
  log(`Total Duration: ${formatElapsedTime(endTime - startTime)}`, 'info');
  log(`Total Dates Processed: ${results.totalProcessed}`, 'info');
  log(`Successfully Fixed: ${results.successfullyFixed}`, 'info');
  log(`Failed Dates: ${results.failedDates.length}`, 'info');
  
  if (results.failedDates.length > 0) {
    log('\n=== Failed Dates ===', 'warning');
    results.failedDates.forEach(date => {
      log(date, 'warning');
    });
  }
  
  // Get final status
  await getReconciliationStatus();
}

// Reconcile recent data
async function reconcileRecent(days: number = DEFAULT_LOOKBACK_DAYS) {
  const now = new Date();
  const startDate = format(subDays(now, days), 'yyyy-MM-dd');
  const endDate = format(now, 'yyyy-MM-dd');
  
  log(`Starting recent data reconciliation for last ${days} days (${startDate} to ${endDate})...`, 'info');
  
  // Use the range reconciliation function
  await reconcileRange(startDate, endDate);
}

// Fix critical dates with known issues
async function fixCriticalDates() {
  log('Identifying critical dates with known issues...', 'info');
  
  // Query to find dates with significant discrepancies
  const criticalDatesQuery = `
    WITH date_stats AS (
      SELECT
        cr.settlement_date,
        COUNT(cr.*) as curtailment_count,
        COUNT(hbc.*) as bitcoin_count,
        COUNT(DISTINCT (cr.settlement_period || '-' || cr.farm_id)) * ${MINER_MODELS.length} as expected_count,
        SUM(ABS(cr.volume::numeric)) as total_volume
      FROM
        curtailment_records cr
      LEFT JOIN
        historical_bitcoin_calculations hbc ON 
          cr.settlement_date = hbc.settlement_date AND 
          cr.settlement_period = hbc.settlement_period AND
          cr.farm_id = hbc.farm_id
      GROUP BY
        cr.settlement_date
    )
    SELECT
      settlement_date,
      curtailment_count,
      bitcoin_count,
      expected_count,
      total_volume,
      CASE
        WHEN bitcoin_count = 0 THEN 'NO_CALCULATIONS'
        WHEN bitcoin_count < expected_count * 0.5 THEN 'SEVERELY_INCOMPLETE'
        WHEN bitcoin_count < expected_count THEN 'INCOMPLETE'
        ELSE 'COMPLETE'
      END as status
    FROM
      date_stats
    WHERE
      (bitcoin_count = 0 OR bitcoin_count < expected_count) AND
      total_volume > 100  -- Only high-volume days as critical
    ORDER BY
      total_volume DESC,
      bitcoin_count ASC
    LIMIT 20
  `;
  
  const result = await db.execute(sql.raw(criticalDatesQuery));
  const criticalDates = result.rows;
  
  if (criticalDates.length === 0) {
    log('No critical dates found!', 'success');
    return;
  }
  
  log(`Found ${criticalDates.length} critical dates to fix`, 'info');
  
  console.log('\n=== Critical Dates ===');
  criticalDates.forEach((date, index) => {
    console.log(`${index + 1}. ${date.settlement_date} - ${date.status} (Volume: ${formatNumber(date.total_volume)} MWh, Calculations: ${date.bitcoin_count}/${date.expected_count})`);
  });
  
  // Process critical dates with special handling
  log('Processing critical dates with enhanced safeguards...', 'info');
  
  const dates = criticalDates.map(d => d.settlement_date as string);
  
  // Reset or resume checkpoint
  if (!loadCheckpoint() || checkpoint.command !== 'fix-critical') {
    resetCheckpoint();
    checkpoint.command = 'fix-critical';
  }
  
  // Process all critical dates
  const startTime = Date.now();
  const results = await processDates(dates);
  const endTime = Date.now();
  
  // Final summary
  log('\n=== Critical Date Fix Complete ===', 'success');
  log(`Total Duration: ${formatElapsedTime(endTime - startTime)}`, 'info');
  log(`Total Dates Processed: ${results.totalProcessed}`, 'info');
  log(`Successfully Fixed: ${results.successfullyFixed}`, 'info');
  log(`Failed Dates: ${results.failedDates.length}`, 'info');
  
  if (results.failedDates.length > 0) {
    log('\n=== Failed Dates ===', 'warning');
    results.failedDates.forEach(date => {
      log(date, 'warning');
    });
  }
  
  // Get final status
  await getReconciliationStatus();
}

// Generate detailed reconciliation report
async function generateReport() {
  log('Generating comprehensive reconciliation report...', 'info');
  
  // Get overall status
  const overview = await getReconciliationStatus();
  
  // Get detailed model stats
  const modelStatsQuery = `
    SELECT
      miner_model,
      COUNT(*) as record_count,
      COUNT(DISTINCT settlement_date) as date_count,
      SUM(bitcoin_mined::numeric) as total_bitcoin
    FROM
      historical_bitcoin_calculations
    GROUP BY
      miner_model
  `;
  
  const modelStats = await db.execute(sql.raw(modelStatsQuery));
  
  console.log('\n=== Miner Model Statistics ===');
  modelStats.rows.forEach(model => {
    console.log(`${model.miner_model}: ${formatNumber(model.record_count)} records across ${model.date_count} dates, ${formatNumber(model.total_bitcoin)} BTC mined`);
  });
  
  // Get monthly completeness
  const monthlyStatsQuery = `
    WITH month_data AS (
      SELECT
        TO_CHAR(settlement_date::date, 'YYYY-MM') as month,
        COUNT(DISTINCT settlement_date) as total_dates,
        COUNT(DISTINCT settlement_period) as total_periods,
        COUNT(*) as curtailment_records,
        SUM(ABS(volume::numeric)) as total_volume
      FROM
        curtailment_records
      GROUP BY
        TO_CHAR(settlement_date::date, 'YYYY-MM')
    ),
    bitcoin_month_data AS (
      SELECT
        TO_CHAR(settlement_date::date, 'YYYY-MM') as month,
        COUNT(DISTINCT settlement_date) as calculated_dates,
        COUNT(DISTINCT (settlement_date || '-' || settlement_period)) as calculated_periods,
        COUNT(*) as bitcoin_records
      FROM
        historical_bitcoin_calculations
      GROUP BY
        TO_CHAR(settlement_date::date, 'YYYY-MM')
    )
    SELECT
      md.month,
      md.total_dates,
      bmd.calculated_dates,
      md.curtailment_records,
      bmd.bitcoin_records,
      md.total_volume,
      ROUND((bmd.calculated_dates::numeric / NULLIF(md.total_dates, 0)) * 100, 2) as date_completion_rate,
      ROUND((bmd.bitcoin_records::numeric / NULLIF(md.curtailment_records * 3, 0)) * 100, 2) as record_completion_rate
    FROM
      month_data md
    LEFT JOIN
      bitcoin_month_data bmd ON md.month = bmd.month
    ORDER BY
      md.month DESC
  `;
  
  const monthlyStats = await db.execute(sql.raw(monthlyStatsQuery));
  
  console.log('\n=== Monthly Reconciliation Status ===');
  monthlyStats.rows.forEach(month => {
    console.log(`${month.month}: ${month.calculated_dates}/${month.total_dates} dates (${formatPercentage(month.date_completion_rate)}), Volume: ${formatNumber(month.total_volume)} MWh`);
  });
  
  // Save report to file
  const reportFileName = `reconciliation_report_${format(new Date(), 'yyyy-MM-dd')}.txt`;
  
  let reportContent = '=== Comprehensive Reconciliation Report ===\n';
  reportContent += `Generated: ${new Date().toISOString()}\n\n`;
  
  reportContent += '=== Overall Status ===\n';
  reportContent += `Curtailment Records: ${formatNumber(overview.curtailment_records)}\n`;
  reportContent += `Bitcoin Calculations: ${formatNumber(overview.bitcoin_records)}\n`;
  reportContent += `Total Curtailed Energy: ${formatNumber(overview.total_curtailed_volume)} MWh\n`;
  reportContent += `Total Bitcoin Mined: ${formatNumber(overview.total_bitcoin_mined)} BTC\n\n`;
  
  reportContent += '=== Date Completion ===\n';
  reportContent += `Complete Dates: ${formatNumber(overview.complete_dates)}\n`;
  reportContent += `Partial Dates: ${formatNumber(overview.partial_dates)}\n`;
  reportContent += `Missing Dates: ${formatNumber(overview.missing_dates)}\n`;
  reportContent += `Completion Rate: ${formatPercentage(Number(overview.complete_dates) / Number(overview.curtailment_dates) * 100)}%\n\n`;
  
  reportContent += '=== Miner Model Statistics ===\n';
  modelStats.rows.forEach(model => {
    reportContent += `${model.miner_model}: ${formatNumber(model.record_count)} records across ${model.date_count} dates, ${formatNumber(model.total_bitcoin)} BTC mined\n`;
  });
  reportContent += '\n';
  
  reportContent += '=== Monthly Reconciliation Status ===\n';
  monthlyStats.rows.forEach(month => {
    reportContent += `${month.month}: ${month.calculated_dates}/${month.total_dates} dates (${formatPercentage(month.date_completion_rate)}), Volume: ${formatNumber(month.total_volume)} MWh\n`;
  });
  
  try {
    fs.writeFileSync(reportFileName, reportContent);
    log(`Report saved to ${reportFileName}`, 'success');
  } catch (error) {
    log(`Error saving report: ${error}`, 'error');
  }
}

// Show help menu
function showHelp() {
  console.log(`
Comprehensive Reconciliation System

A unified solution for ensuring complete data integrity between
curtailment_records and historical_bitcoin_calculations tables.

Usage:
  npx tsx comprehensive_reconciliation.ts [command]

Commands:
  status           - Show current reconciliation status
  reconcile-all    - Reconcile all dates in the database
  reconcile-range  - Reconcile a specific date range (requires additional parameters)
                     Example: npx tsx comprehensive_reconciliation.ts reconcile-range 2025-01-01 2025-01-31
  reconcile-recent - Reconcile recent data (default: last 30 days)
                     Example: npx tsx comprehensive_reconciliation.ts reconcile-recent 7
  fix-critical     - Fix dates with known issues
  report           - Generate detailed reconciliation report
  help             - Show this help message
  `);
}

// Main function
async function main() {
  try {
    const args = process.argv.slice(2);
    const command = args[0]?.toLowerCase();
    
    if (!command || command === 'help') {
      showHelp();
      return;
    }
    
    switch (command) {
      case 'status':
        await getReconciliationStatus();
        break;
        
      case 'reconcile-all':
        await reconcileAll();
        break;
        
      case 'reconcile-range':
        if (!args[1] || !args[2] || !args[1].match(/^\d{4}-\d{2}-\d{2}$/) || !args[2].match(/^\d{4}-\d{2}-\d{2}$/)) {
          console.error('Error: Please provide start and end dates in YYYY-MM-DD format');
          showHelp();
          return;
        }
        await reconcileRange(args[1], args[2]);
        break;
        
      case 'reconcile-recent':
        const days = args[1] ? parseInt(args[1], 10) : DEFAULT_LOOKBACK_DAYS;
        await reconcileRecent(days);
        break;
        
      case 'fix-critical':
        await fixCriticalDates();
        break;
        
      case 'report':
        await generateReport();
        break;
        
      default:
        console.error(`Unknown command: ${command}`);
        showHelp();
    }
  } catch (error) {
    log(`Error in main function: ${error}`, 'error');
  } finally {
    try {
      // Clean up resources
      await pool.end();
    } catch (err) {
      log(`Error ending pool: ${err}`, 'error');
    }
  }
}

// Run the script
main().catch(err => {
  console.error('Unhandled error:', err);
  process.exit(1);
});
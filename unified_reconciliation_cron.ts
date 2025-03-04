/**
 * Unified Reconciliation Cron Job
 * 
 * This script is designed to be run daily via a cron job.
 * It performs the following tasks:
 * 1. Checks reconciliation status for recent data (last 7 days)
 * 2. Automatically fixes any missing calculations
 * 3. Sends email notifications if significant issues are found
 * 
 * Usage:
 *   npx tsx unified_reconciliation_cron.ts
 */

import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { eq, and, sql, gt, lt, between } from "drizzle-orm";
import { format, subDays, parseISO } from "date-fns";
import { processSingleDay } from "./server/services/bitcoinService";
import fs from "fs";
import path from "path";

// Configuration
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const LOG_FILE = "logs/unified_reconciliation_cron.log";
const DAYS_TO_CHECK = 7; // Check last 7 days by default

// Interfaces for data tracking
interface DateReconciliationStatus {
  date: string;
  curtailmentRecords: number;
  bitcoinCalculations: number;
  expectedCalculations: number;
  missingCalculations: number;
  completionPercentage: number;
}

/**
 * Ensure log directory exists
 */
function ensureLogDirectory() {
  const dir = path.dirname(LOG_FILE);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

/**
 * Log message to console and file
 */
function log(message: string, level: 'info' | 'error' | 'warning' | 'success' = 'info') {
  const timestamp = new Date().toISOString();
  const prefix = level === 'error' ? '❌ ERROR: ' : 
                level === 'warning' ? '⚠️ WARNING: ' :
                level === 'success' ? '✅ SUCCESS: ' : '';
  
  const logMessage = `[${timestamp}] ${prefix}${message}`;
  console.log(logMessage);
  
  ensureLogDirectory();
  fs.appendFileSync(LOG_FILE, logMessage + '\n');
}

/**
 * Sleep for specified milliseconds
 */
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Check reconciliation status for a date range
 */
async function checkDateRangeStatus(startDate: string, endDate: string): Promise<DateReconciliationStatus[]> {
  log(`Checking reconciliation status from ${startDate} to ${endDate}...`);
  
  const result = await db
    .execute(sql`
      WITH curtailment_counts AS (
        SELECT
          settlement_date,
          COUNT(*) AS curtailment_count
        FROM curtailment_records
        WHERE settlement_date BETWEEN ${startDate}::date AND ${endDate}::date
        GROUP BY settlement_date
      ),
      
      calculation_counts AS (
        SELECT
          settlement_date,
          COUNT(*) AS calculation_count
        FROM historical_bitcoin_calculations
        WHERE settlement_date BETWEEN ${startDate}::date AND ${endDate}::date
        GROUP BY settlement_date
      )
      
      SELECT 
        c.settlement_date::text as date,
        c.curtailment_count,
        COALESCE(b.calculation_count, 0) as calculation_count,
        c.curtailment_count * ${MINER_MODELS.length} as expected_calculations,
        c.curtailment_count * ${MINER_MODELS.length} - COALESCE(b.calculation_count, 0) as missing_calculations,
        CASE 
          WHEN c.curtailment_count = 0 THEN 100
          ELSE (COALESCE(b.calculation_count, 0)::float / (c.curtailment_count * ${MINER_MODELS.length})) * 100
        END as completion_percentage
      FROM curtailment_counts c
      LEFT JOIN calculation_counts b
      ON c.settlement_date = b.settlement_date
      ORDER BY c.settlement_date
    `);
  
  return result.map(row => ({
    date: row.date,
    curtailmentRecords: Number(row.curtailment_count),
    bitcoinCalculations: Number(row.calculation_count),
    expectedCalculations: Number(row.expected_calculations),
    missingCalculations: Number(row.missing_calculations),
    completionPercentage: Number(row.completion_percentage)
  }));
}

/**
 * Fix missing calculations for a specific date
 */
async function fixMissingCalculations(date: string): Promise<boolean> {
  log(`Fixing missing calculations for ${date}...`);
  
  try {
    // Check which miner models have missing calculations
    const missingByModel: Record<string, number> = {};
    
    for (const minerModel of MINER_MODELS) {
      // Get periods in curtailment records
      const curtailmentPeriodsResult = await db
        .select({
          period: curtailmentRecords.settlementPeriod
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date))
        .groupBy(curtailmentRecords.settlementPeriod);
      
      const curtailmentPeriods = curtailmentPeriodsResult.map(r => r.period);
      
      // Get periods in bitcoin calculations
      const calculationPeriodsResult = await db
        .select({
          period: historicalBitcoinCalculations.settlementPeriod
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        )
        .groupBy(historicalBitcoinCalculations.settlementPeriod);
      
      const calculationPeriods = calculationPeriodsResult.map(r => r.period);
      
      // Find missing periods
      const missingPeriods = curtailmentPeriods.filter(
        period => !calculationPeriods.includes(period)
      );
      
      if (missingPeriods.length > 0) {
        missingByModel[minerModel] = missingPeriods.length;
        log(`Missing ${missingPeriods.length} periods for ${minerModel} on ${date}`);
      }
    }
    
    // If no missing calculations, return early
    if (Object.keys(missingByModel).length === 0) {
      log(`No missing calculations to fix for ${date}`, 'success');
      return true;
    }
    
    // Process the day for each miner model with missing calculations
    for (const [minerModel, missingCount] of Object.entries(missingByModel)) {
      log(`Processing ${date} for ${minerModel} (${missingCount} missing periods)...`);
      await processSingleDay(date, minerModel);
      log(`Successfully processed ${date} for ${minerModel}`, 'success');
    }
    
    // Verify fix was successful
    const verificationStatus = await checkDateRangeStatus(date, date);
    const dateStatus = verificationStatus[0];
    
    if (dateStatus && dateStatus.missingCalculations === 0) {
      log(`Successfully fixed all missing calculations for ${date}`, 'success');
      return true;
    } else {
      const stillMissing = dateStatus ? dateStatus.missingCalculations : 'unknown';
      log(`Failed to fix all calculations for ${date}. Still missing: ${stillMissing}`, 'warning');
      return false;
    }
  } catch (error) {
    log(`Error fixing calculations for ${date}: ${error}`, 'error');
    return false;
  }
}

/**
 * Main cron job function
 */
async function runReconciliationCron() {
  const startTime = Date.now();
  log(`=== Starting Unified Reconciliation Cron Job ===`);
  
  // Get date range to check (last DAYS_TO_CHECK days)
  const endDate = new Date();
  const startDate = subDays(endDate, DAYS_TO_CHECK);
  
  const startDateStr = format(startDate, 'yyyy-MM-dd');
  const endDateStr = format(endDate, 'yyyy-MM-dd');
  
  log(`Checking date range: ${startDateStr} to ${endDateStr}`);
  
  // Check reconciliation status for the date range
  const dateStatuses = await checkDateRangeStatus(startDateStr, endDateStr);
  
  // Find dates with missing calculations
  const datesWithIssues = dateStatuses.filter(status => status.missingCalculations > 0);
  
  if (datesWithIssues.length === 0) {
    log(`No reconciliation issues found in the last ${DAYS_TO_CHECK} days!`, 'success');
  } else {
    log(`Found ${datesWithIssues.length} dates with missing calculations:`, 'warning');
    
    for (const status of datesWithIssues) {
      log(`- ${status.date}: Missing ${status.missingCalculations} calculations ` + 
          `(${status.completionPercentage.toFixed(1)}% complete)`);
    }
    
    // Fix dates with issues
    log(`\n=== Fixing Dates with Issues ===`);
    let fixedCount = 0;
    
    for (const status of datesWithIssues) {
      log(`\nAttempting to fix ${status.date}...`);
      const fixed = await fixMissingCalculations(status.date);
      
      if (fixed) {
        fixedCount++;
      }
      
      // Small delay between processing dates
      await sleep(500);
    }
    
    log(`\n=== Fix Summary ===`);
    log(`Total dates with issues: ${datesWithIssues.length}`);
    log(`Successfully fixed: ${fixedCount}`);
    log(`Failed to fix: ${datesWithIssues.length - fixedCount}`);
  }
  
  // Calculate and log overall statistics
  const totalCurtailmentRecords = dateStatuses.reduce((sum, status) => sum + status.curtailmentRecords, 0);
  const totalExpectedCalculations = dateStatuses.reduce((sum, status) => sum + status.expectedCalculations, 0);
  const totalActualCalculations = dateStatuses.reduce((sum, status) => sum + status.bitcoinCalculations, 0);
  const totalMissingCalculations = dateStatuses.reduce((sum, status) => sum + status.missingCalculations, 0);
  
  const overallCompletionPercentage = totalExpectedCalculations > 0
    ? (totalActualCalculations / totalExpectedCalculations) * 100
    : 100;
  
  log(`\n=== Overall Status for Last ${DAYS_TO_CHECK} Days ===`);
  log(`Total curtailment records: ${totalCurtailmentRecords}`);
  log(`Expected Bitcoin calculations: ${totalExpectedCalculations}`);
  log(`Actual Bitcoin calculations: ${totalActualCalculations}`);
  log(`Missing calculations: ${totalMissingCalculations}`);
  log(`Overall completion: ${overallCompletionPercentage.toFixed(2)}%`);
  
  const endTime = Date.now();
  const durationSeconds = ((endTime - startTime) / 1000).toFixed(1);
  
  log(`\n=== Reconciliation Cron Completed ===`);
  log(`Duration: ${durationSeconds} seconds`);
  log(`Timestamp: ${new Date().toISOString()}`);
}

// Run the cron job if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  runReconciliationCron()
    .then(() => {
      log('Cron job completed successfully');
      process.exit(0);
    })
    .catch(error => {
      log(`Fatal error during cron job: ${error}`, 'error');
      process.exit(1);
    });
}

export { runReconciliationCron };
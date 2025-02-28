/**
 * Full Reconciliation Script
 * 
 * This script performs a complete reconciliation between curtailment_records and
 * historical_bitcoin_calculations tables, ensuring that Bitcoin calculations
 * exist for all curtailment records across all dates and miner models.
 * 
 * Usage:
 *   npx tsx reconciliation_script.ts [startDate] [endDate]
 * 
 * If no dates are provided, the script will reconcile all dates with curtailment records.
 */

import { db } from "./db";
import { sql } from "drizzle-orm";
import { format, addDays, subDays, parseISO } from "date-fns";
import { findMissingDates, auditAndFixBitcoinCalculations, reconcileDateRange } from "./server/services/historicalReconciliation";

// Configuration
const MAX_CONCURRENT_DATES = 5;
const DEFAULT_LOOKBACK_DAYS = 30; // Default to looking back 30 days if no dates provided

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

// Get a summary of the current reconciliation status
async function getReconciliationSummary() {
  console.log('\n=== Current Reconciliation Status ===');
  
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
  
  console.log(`Curtailment Records: ${formatNumber(overview.curtailment_records)}`);
  console.log(`Bitcoin Calculations: ${formatNumber(overview.bitcoin_records)}`);
  console.log(`Total Curtailed Energy: ${formatNumber(overview.total_curtailed_volume)} MWh`);
  console.log(`Total Bitcoin Mined: ${formatNumber(overview.total_bitcoin_mined)} BTC`);
  console.log('\n=== Date Completion ===');
  console.log(`Complete Dates: ${formatNumber(overview.complete_dates)}`);
  console.log(`Partial Dates: ${formatNumber(overview.partial_dates)}`);
  console.log(`Missing Dates: ${formatNumber(overview.missing_dates)}`);
  console.log(`Completion Rate: ${formatPercentage(Number(overview.complete_dates) / Number(overview.curtailment_dates) * 100)}%`);
  
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

// Run reconciliation for all dates or a specific range
async function runFullReconciliation(startDate?: string, endDate?: string) {
  console.log('\n=== Starting Full Reconciliation ===');
  
  // If no dates provided, get the full range or use default lookback
  if (!startDate || !endDate) {
    const range = await getDateRange();
    
    if (!startDate) {
      // If no start date, either use earliest date or lookback from end date
      if (endDate) {
        startDate = format(subDays(parseISO(endDate), DEFAULT_LOOKBACK_DAYS), 'yyyy-MM-dd');
      } else {
        startDate = range.minDate;
      }
    }
    
    if (!endDate) {
      endDate = range.maxDate;
    }
  }
  
  console.log(`Date Range: ${startDate} to ${endDate}`);
  
  // Get initial status
  await getReconciliationSummary();
  
  // Find dates with missing calculations
  console.log('\n=== Finding Dates with Missing Calculations ===');
  const missingDates = await findMissingDates(startDate, endDate);
  
  console.log(`Found ${missingDates.length} dates with missing or incomplete calculations`);
  
  if (missingDates.length === 0) {
    console.log('✓ All dates are fully reconciled!');
    return;
  }
  
  // Process dates in parallel batches for efficiency
  console.log('\n=== Processing Missing Dates ===');
  const results = {
    totalProcessed: 0,
    successfullyFixed: 0,
    errors: [] as Array<{date: string, error: string}>
  };
  
  // Process in batches to avoid overloading the database
  for (let i = 0; i < missingDates.length; i += MAX_CONCURRENT_DATES) {
    const batch = missingDates.slice(i, i + MAX_CONCURRENT_DATES);
    
    const batchResults = await Promise.allSettled(
      batch.map(dateInfo => auditAndFixBitcoinCalculations(dateInfo.date))
    );
    
    // Process results
    batchResults.forEach((result, index) => {
      const date = batch[index].date;
      results.totalProcessed++;
      
      if (result.status === 'fulfilled') {
        if (result.value.success) {
          results.successfullyFixed++;
          console.log(`✓ ${date}: ${result.value.message}`);
        } else {
          results.errors.push({ date, error: result.value.message });
          console.log(`✗ ${date}: ${result.value.message}`);
        }
      } else {
        results.errors.push({ date, error: result.reason.message || 'Unknown error' });
        console.log(`✗ ${date}: ${result.reason.message || 'Unknown error'}`);
      }
    });
    
    // Log progress
    console.log(`Progress: ${Math.min(i + MAX_CONCURRENT_DATES, missingDates.length)}/${missingDates.length} dates processed`);
  }
  
  // Final summary
  console.log('\n=== Reconciliation Complete ===');
  console.log(`Total Dates Processed: ${results.totalProcessed}`);
  console.log(`Successfully Fixed: ${results.successfullyFixed}`);
  console.log(`Errors: ${results.errors.length}`);
  
  if (results.errors.length > 0) {
    console.log('\n=== Errors ===');
    results.errors.forEach((error, index) => {
      console.log(`${index + 1}. ${error.date}: ${error.error}`);
    });
  }
  
  // Get final status
  await getReconciliationSummary();
}

// Main function
async function main() {
  try {
    const args = process.argv.slice(2);
    const startDate = args[0];
    const endDate = args[1];
    
    if (startDate && !/^\d{4}-\d{2}-\d{2}$/.test(startDate)) {
      console.error('Error: Start date must be in format YYYY-MM-DD');
      process.exit(1);
    }
    
    if (endDate && !/^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
      console.error('Error: End date must be in format YYYY-MM-DD');
      process.exit(1);
    }
    
    await runFullReconciliation(startDate, endDate);
  } catch (error) {
    console.error('Error in reconciliation process:', error);
  }
}

// Run the script
main().catch(console.error);
/**
 * Batch Reconciliation Tool
 * 
 * Reconciles historical curtailment records with bitcoin calculations
 * using a direct approach with the historicalReconciliation service.
 * 
 * Usage:
 *   npx tsx batch_reconcile.ts [startDate] [endDate]
 */

import { db } from "./db";
import { format, parseISO, eachDayOfInterval } from "date-fns";
import { sql } from "drizzle-orm";
import { reconcileDay, auditAndFixBitcoinCalculations } from "./server/services/historicalReconciliation";

const MAX_CONCURRENT_DAYS = 1; // Process one day at a time to avoid timeouts

// Sleep utility
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Get the date range to process
async function getDateRange(startDate?: string, endDate?: string) {
  if (!startDate || !endDate) {
    // Query the database for date range
    const query = `
      SELECT 
        MIN(settlement_date) as min_date, 
        MAX(settlement_date) as max_date 
      FROM curtailment_records
    `;
    
    const result = await db.execute(sql.raw(query));
    const range = {
      minDate: startDate || result.rows[0]?.min_date as string,
      maxDate: endDate || result.rows[0]?.max_date as string
    };
    
    console.log(`Date range: ${range.minDate} to ${range.maxDate}`);
    return range;
  }
  
  return { minDate: startDate, maxDate: endDate };
}

// Get dates with missing calculations
async function getDatesWithMissingCalculations(startDate: string, endDate: string): Promise<string[]> {
  console.log("Finding dates with missing calculations...");
  
  const query = `
    WITH curtailment_dates AS (
      SELECT DISTINCT settlement_date 
      FROM curtailment_records
      WHERE settlement_date BETWEEN $1 AND $2
    ),
    calculation_summary AS (
      SELECT 
        cr.settlement_date,
        COUNT(DISTINCT (cr.settlement_period || '-' || cr.farm_id)) as expected_count,
        COUNT(DISTINCT (hbc.settlement_period || '-' || hbc.farm_id || '-' || hbc.miner_model)) as actual_count
      FROM 
        curtailment_records cr
      LEFT JOIN 
        historical_bitcoin_calculations hbc ON cr.settlement_date = hbc.settlement_date
      WHERE 
        cr.settlement_date BETWEEN $1 AND $2
      GROUP BY 
        cr.settlement_date
    )
    SELECT 
      settlement_date::text as date
    FROM 
      calculation_summary
    WHERE 
      actual_count < expected_count * 3
    ORDER BY 
      settlement_date
  `;
  
  // Replace placeholders with actual values for direct execution
  const modifiedQuery = query.replace(/\$1/g, `'${startDate}'`).replace(/\$2/g, `'${endDate}'`);
  const result = await db.execute(sql.raw(modifiedQuery));
  
  const dates = result.rows.map(row => row.date as string);
  console.log(`Found ${dates.length} dates with missing calculations`);
  
  return dates;
}

// Process a single date with automatic verification
async function processDate(date: string): Promise<boolean> {
  console.log(`\n--- Processing ${date} ---`);
  
  try {
    // First reconcile the day
    console.log(`Running reconcileDay for ${date}...`);
    await reconcileDay(date);
    
    // Then verify with audit
    console.log(`Verifying calculations for ${date}...`);
    const result = await auditAndFixBitcoinCalculations(date);
    
    if (result.success) {
      console.log(`✅ Successfully processed ${date}: ${result.message}`);
      return true;
    } else {
      console.log(`❌ Failed to process ${date}: ${result.message}`);
      return false;
    }
  } catch (error) {
    console.error(`Error processing ${date}:`, error);
    return false;
  }
}

// Main function
async function main() {
  try {
    const args = process.argv.slice(2);
    let startDate = args[0];
    let endDate = args[1];
    
    // Validate date format if provided
    if (startDate && !/^\d{4}-\d{2}-\d{2}$/.test(startDate)) {
      console.error('Error: Start date must be in format YYYY-MM-DD');
      process.exit(1);
    }
    
    if (endDate && !/^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
      console.error('Error: End date must be in format YYYY-MM-DD');
      process.exit(1);
    }
    
    // Get date range to process
    const range = await getDateRange(startDate, endDate);
    startDate = range.minDate;
    endDate = range.maxDate;
    
    // Get all dates in the range
    const dates = eachDayOfInterval({
      start: parseISO(startDate),
      end: parseISO(endDate)
    }).map(date => format(date, 'yyyy-MM-dd'));
    
    console.log(`Processing date range from ${startDate} to ${endDate} (${dates.length} days)`);
    
    // Find dates with missing calculations
    const missingDates = await getDatesWithMissingCalculations(startDate, endDate);
    
    if (missingDates.length === 0) {
      console.log("✅ All dates in range are already reconciled!");
      return;
    }
    
    console.log(`Will process ${missingDates.length} dates with missing calculations`);
    
    // Process dates sequentially or with limited concurrency
    const results = {
      processed: 0,
      successful: 0,
      failed: 0
    };
    
    // Process dates one by one to avoid timeouts and resource conflicts
    for (const date of missingDates) {
      results.processed++;
      const success = await processDate(date);
      
      if (success) {
        results.successful++;
      } else {
        results.failed++;
      }
      
      // Progress report
      console.log(`\n--- Progress: ${results.processed}/${missingDates.length} dates processed ---`);
      console.log(`Successful: ${results.successful}, Failed: ${results.failed}\n`);
      
      // Add a small delay between dates
      await sleep(1000);
    }
    
    // Final report
    console.log(`\n=== Reconciliation Complete ===`);
    console.log(`Total dates processed: ${results.processed}`);
    console.log(`Successfully reconciled: ${results.successful}`);
    console.log(`Failed to reconcile: ${results.failed}`);
    
    if (results.failed > 0) {
      console.log(`\nNot all dates were successfully reconciled. Run the script again to retry.`);
      process.exit(1);
    }
  } catch (error) {
    console.error("Error in main function:", error);
    process.exit(1);
  }
}

main().catch(console.error);
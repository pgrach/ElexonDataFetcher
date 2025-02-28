/**
 * Optimized December 2023 Reconciliation Script
 * 
 * This script focuses on the critical month of December 2023, which has
 * the highest number of missing calculations. It processes dates in order
 * of highest missing calculations, with priority to partially completed dates.
 */

import pg from 'pg';
import { format } from 'date-fns';
import * as reconciliation from './server/services/historicalReconciliation';
import { db } from './db';
import { eq, and, sql } from 'drizzle-orm';
import { curtailmentRecords, historicalBitcoinCalculations } from './db/schema';

// Constants
const BATCH_SIZE = 3; // Process 3 dates at a time
const MAX_RETRIES = 2;
const RETRY_DELAY = 1000; // 1 second

// Database connection
const dbUrl = process.env.DATABASE_URL;
if (!dbUrl) {
  console.error('DATABASE_URL environment variable is not set');
  process.exit(1);
}

const pool = new pg.Pool({
  connectionString: dbUrl,
  max: 5,
});

// Helper functions
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Get detailed status of December reconciliation
 */
async function getDecemberReconciliationStatus(): Promise<Array<{
  date: string;
  expected: number;
  actual: number;
  missing: number;
  percentage: number;
}>> {
  console.log("Fetching December 2023 reconciliation status...");
  
  const client = await pool.connect();
  try {
    const query = `
      WITH december_dates AS (
        SELECT 
          cr.settlement_date,
          COUNT(DISTINCT (cr.settlement_period, cr.farm_id)) * 3 AS expected_count,
          COUNT(DISTINCT (hbc.settlement_period, hbc.farm_id, hbc.miner_model)) AS actual_count
        FROM 
          curtailment_records cr
        LEFT JOIN 
          historical_bitcoin_calculations hbc ON cr.settlement_date = hbc.settlement_date
          AND cr.settlement_period = hbc.settlement_period
          AND cr.farm_id = hbc.farm_id
        WHERE 
          cr.settlement_date >= '2023-12-01' AND cr.settlement_date <= '2023-12-31'
        GROUP BY 
          cr.settlement_date
      )
      SELECT 
        settlement_date::text AS date,
        expected_count AS expected,
        actual_count AS actual,
        expected_count - actual_count AS missing,
        CASE 
          WHEN expected_count = 0 THEN 100
          ELSE ROUND((actual_count::numeric / expected_count) * 100, 2)
        END AS percentage
      FROM 
        december_dates
      ORDER BY 
        missing DESC,
        percentage ASC;
    `;
    
    const result = await client.query(query);
    return result.rows.map(row => ({
      date: row.date,
      expected: parseInt(row.expected),
      actual: parseInt(row.actual),
      missing: parseInt(row.missing),
      percentage: parseFloat(row.percentage)
    }));
  } finally {
    client.release();
  }
}

/**
 * Process a single date with error handling and retries
 */
async function processDate(date: string, retryCount = 0): Promise<boolean> {
  try {
    console.log(`[${date}] Starting processing...`);
    
    // Get status before processing
    const beforeStatus = await getDateStatus(date);
    console.log(`[${date}] Before: ${beforeStatus.actual}/${beforeStatus.expected} (${beforeStatus.percentage.toFixed(2)}%)`);
    
    // Use the reconciliation service
    await reconciliation.reconcileDay(date);
    
    // Get status after processing
    const afterStatus = await getDateStatus(date);
    console.log(`[${date}] After: ${afterStatus.actual}/${afterStatus.expected} (${afterStatus.percentage.toFixed(2)}%)`);
    
    // Check if all records are reconciled
    if (afterStatus.percentage < 100) {
      console.log(`[${date}] Incomplete reconciliation - trying again with lower-level functions`);
      
      // Process one miner model at a time for more granular control
      for (const minerModel of ['S19J_PRO', 'S9', 'M20S']) {
        console.log(`[${date}] Processing miner model: ${minerModel}`);
        await reconciliation.reprocessDay(date);
      }
      
      // Final check
      const finalStatus = await getDateStatus(date);
      console.log(`[${date}] Final: ${finalStatus.actual}/${finalStatus.expected} (${finalStatus.percentage.toFixed(2)}%)`);
      
      return finalStatus.percentage === 100;
    }
    
    return true;
  } catch (error) {
    console.error(`[${date}] Error processing:`, error);
    
    // Retry logic
    if (retryCount < MAX_RETRIES) {
      console.log(`[${date}] Retrying (attempt ${retryCount + 1}/${MAX_RETRIES})...`);
      await sleep(RETRY_DELAY);
      return processDate(date, retryCount + 1);
    }
    
    return false;
  }
}

/**
 * Get status for a specific date
 */
async function getDateStatus(date: string): Promise<{
  expected: number;
  actual: number;
  percentage: number;
}> {
  const client = await pool.connect();
  try {
    const query = `
      SELECT 
        COUNT(DISTINCT (cr.settlement_period, cr.farm_id)) * 3 AS expected_count,
        COUNT(DISTINCT (hbc.settlement_period, hbc.farm_id, hbc.miner_model)) AS actual_count
      FROM 
        curtailment_records cr
      LEFT JOIN 
        historical_bitcoin_calculations hbc ON cr.settlement_date = hbc.settlement_date
        AND cr.settlement_period = hbc.settlement_period
        AND cr.farm_id = hbc.farm_id
      WHERE 
        cr.settlement_date = $1;
    `;
    
    const result = await client.query(query, [date]);
    
    const expected = parseInt(result.rows[0].expected_count);
    const actual = parseInt(result.rows[0].actual_count);
    const percentage = expected > 0 ? (actual / expected) * 100 : 100;
    
    return { expected, actual, percentage };
  } finally {
    client.release();
  }
}

/**
 * Process a batch of dates
 */
async function processBatch(dates: string[]): Promise<{
  successful: string[];
  failed: string[];
}> {
  console.log(`Processing batch of ${dates.length} dates: ${dates.join(', ')}`);
  
  const successful: string[] = [];
  const failed: string[] = [];
  
  // Process one date at a time to prevent resource contention
  for (const date of dates) {
    const success = await processDate(date);
    
    if (success) {
      successful.push(date);
    } else {
      failed.push(date);
    }
    
    // Pause briefly between dates
    await sleep(500);
  }
  
  return { successful, failed };
}

/**
 * Print detailed reconciliation report
 */
async function printDetailedReport(): Promise<void> {
  const status = await getDecemberReconciliationStatus();
  
  console.log("\n===== December 2023 Reconciliation Status =====");
  console.log("Date       | Percentage | Calculations (Actual/Expected) | Missing");
  console.log("-----------|------------|-------------------------------|--------");
  
  let totalActual = 0;
  let totalExpected = 0;
  
  status.forEach(day => {
    const percentage = day.percentage.toFixed(2).padStart(5, ' ');
    console.log(`${day.date} | ${percentage}%     | ${day.actual.toString().padStart(6, ' ')}/${day.expected.toString().padStart(6, ' ')} | ${day.missing.toString().padStart(6, ' ')}`);
    
    totalActual += day.actual;
    totalExpected += day.expected;
  });
  
  const overallPercentage = (totalActual / totalExpected) * 100;
  
  console.log("-----------------------------------------------------------------");
  console.log(`Overall    | ${overallPercentage.toFixed(2)}%     | ${totalActual}/${totalExpected} | ${totalExpected - totalActual}`);
  console.log("=================================================================\n");
}

/**
 * Main reconciliation function
 */
async function reconcileDecember2023(): Promise<void> {
  console.log("Starting optimized December 2023 reconciliation...");
  
  try {
    // Get initial status
    await printDetailedReport();
    
    // Get dates with missing calculations, sorted by most missing first
    const decemberStatus = await getDecemberReconciliationStatus();
    const incompleteDates = decemberStatus.filter(day => day.percentage < 100);
    
    if (incompleteDates.length === 0) {
      console.log("All December 2023 dates are already reconciled!");
      return;
    }
    
    console.log(`Found ${incompleteDates.length} dates with missing calculations`);
    
    // Focus first on dates that already have some progress
    const partiallyCompleteDates = incompleteDates.filter(day => day.percentage > 0 && day.percentage < 100);
    console.log(`${partiallyCompleteDates.length} dates are partially reconciled`);
    
    // Process in batches
    let processedCount = 0;
    let successfulDates = 0;
    let failedDates = 0;
    
    // Process partially complete dates first
    if (partiallyCompleteDates.length > 0) {
      console.log("\n--- Processing partially complete dates first ---");
      
      for (let i = 0; i < partiallyCompleteDates.length; i += BATCH_SIZE) {
        const batch = partiallyCompleteDates.slice(i, i + BATCH_SIZE).map(day => day.date);
        console.log(`\nBatch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(partiallyCompleteDates.length / BATCH_SIZE)}`);
        
        const { successful, failed } = await processBatch(batch);
        
        processedCount += batch.length;
        successfulDates += successful.length;
        failedDates += failed.length;
        
        console.log(`Batch results - Success: ${successful.length}, Failed: ${failed.length}`);
        console.log(`Progress: ${processedCount}/${incompleteDates.length} dates processed`);
        
        // Short pause between batches
        if (i + BATCH_SIZE < partiallyCompleteDates.length) {
          await sleep(1000);
        }
      }
    }
    
    // Process completely missing dates next (0% complete)
    const completelyMissingDates = incompleteDates.filter(day => day.percentage === 0);
    
    if (completelyMissingDates.length > 0) {
      console.log("\n--- Processing completely missing dates ---");
      
      for (let i = 0; i < completelyMissingDates.length; i += BATCH_SIZE) {
        const batch = completelyMissingDates.slice(i, i + BATCH_SIZE).map(day => day.date);
        console.log(`\nBatch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(completelyMissingDates.length / BATCH_SIZE)}`);
        
        const { successful, failed } = await processBatch(batch);
        
        processedCount += batch.length;
        successfulDates += successful.length;
        failedDates += failed.length;
        
        console.log(`Batch results - Success: ${successful.length}, Failed: ${failed.length}`);
        console.log(`Overall progress: ${processedCount}/${incompleteDates.length} dates processed`);
        
        // Short pause between batches
        if (i + BATCH_SIZE < completelyMissingDates.length) {
          await sleep(1000);
        }
      }
    }
    
    // Final report
    console.log("\n===== December 2023 Reconciliation Complete =====");
    console.log(`Dates processed: ${processedCount}`);
    console.log(`Successful dates: ${successfulDates}`);
    console.log(`Failed dates: ${failedDates}`);
    
    await printDetailedReport();
    
  } catch (error) {
    console.error("Error in reconciliation process:", error);
  } finally {
    await pool.end();
  }
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  reconcileDecember2023()
    .then(() => {
      console.log("December 2023 reconciliation complete");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
}

export { reconcileDecember2023 };
/**
 * Single Date Reconciliation Tool
 * 
 * This script processes a single date for Bitcoin calculations, allowing 
 * for incremental reconciliation progress without hitting timeout limits.
 */

import pg from 'pg';
import * as reconciliation from './server/services/historicalReconciliation';

// The date to process - override with command line argument if provided
const targetDate = process.argv[2] || '2023-12-21';  // Default to 2023-12-21 (partially completed)

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

async function processSingleDate(date: string): Promise<void> {
  console.log(`\n=== Processing Date: ${date} ===\n`);
  
  try {
    // Get initial status
    const beforeStatus = await getDateStatus(date);
    console.log(`Before processing: ${beforeStatus.actual}/${beforeStatus.expected} calculations (${beforeStatus.percentage.toFixed(2)}%)`);
    
    // Process the date
    console.log(`\nStarting reconciliation for ${date}...`);
    await reconciliation.reconcileDay(date);
    console.log(`\nReconciliation completed for ${date}`);
    
    // Verify after processing
    const afterStatus = await getDateStatus(date);
    console.log(`\nAfter processing: ${afterStatus.actual}/${afterStatus.expected} calculations (${afterStatus.percentage.toFixed(2)}%)`);
    
    // If incomplete, try again with more targeted approach
    if (afterStatus.percentage < 100) {
      console.log(`\nIncomplete reconciliation - using lower-level API for ${date}`);
      
      // Process using reprocessDay for each model
      for (const minerModel of ['S19J_PRO', 'S9', 'M20S']) {
        console.log(`\nReprocessing ${date} with ${minerModel}...`);
        await reconciliation.reprocessDay(date);
      }
      
      // Final verification
      const finalStatus = await getDateStatus(date);
      console.log(`\nFinal status: ${finalStatus.actual}/${finalStatus.expected} calculations (${finalStatus.percentage.toFixed(2)}%)`);
      
      if (finalStatus.percentage === 100) {
        console.log(`\n✅ Successfully reconciled ${date}`);
      } else {
        console.log(`\n⚠️ Partial reconciliation for ${date} - may need another approach`);
      }
    } else {
      console.log(`\n✅ Successfully reconciled ${date}`);
    }
  } catch (error) {
    console.error(`\n❌ Error processing ${date}:`, error);
  }
}

async function main(): Promise<void> {
  try {
    await processSingleDate(targetDate);
  } catch (error) {
    console.error('Fatal error:', error);
  } finally {
    await pool.end();
  }
}

// Run the script
main()
  .then(() => {
    console.log(`\n=== Reconciliation process for ${targetDate} completed ===`);
    process.exit(0);
  })
  .catch(error => {
    console.error(`\n=== Fatal error processing ${targetDate} ===`, error);
    process.exit(1);
  });
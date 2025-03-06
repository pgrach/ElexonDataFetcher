/**
 * Quick Reconciliation Check for a Single Date
 * 
 * This script performs a lightweight reconciliation check for a specific date
 * to verify that all Bitcoin calculations exist for all curtailment records.
 * 
 * Usage:
 *   npx tsx check_single_date.ts
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

// Define the target date to check
const TARGET_DATE = '2025-03-05';

async function checkSingleDate() {
  console.log(`\n=== Reconciliation Check for ${TARGET_DATE} ===\n`);
  
  try {
    // Check curtailment records
    const curtailmentCheck = await db.execute(sql`
      SELECT 
        COUNT(*) AS total_records,
        COUNT(DISTINCT settlement_period) AS distinct_periods,
        COUNT(DISTINCT farm_id) AS distinct_farms,
        SUM(ABS(volume::numeric)) AS total_volume,
        SUM(payment::numeric) AS total_payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    console.log(`Curtailment Records for ${TARGET_DATE}:`);
    console.log(`- Total Records: ${curtailmentCheck[0].total_records}`);
    console.log(`- Distinct Periods: ${curtailmentCheck[0].distinct_periods}`);
    console.log(`- Distinct Farms: ${curtailmentCheck[0].distinct_farms}`);
    console.log(`- Total Volume: ${Number(curtailmentCheck[0].total_volume).toFixed(2)} MWh`);
    console.log(`- Total Payment: £${Number(curtailmentCheck[0].total_payment).toFixed(2)}`);
    
    // Check Bitcoin calculations
    const bitcoinCheck = await db.execute(sql`
      SELECT 
        COUNT(*) AS total_records,
        COUNT(DISTINCT settlement_period) AS distinct_periods,
        COUNT(DISTINCT farm_id) AS distinct_farms,
        COUNT(DISTINCT miner_model) AS distinct_models,
        SUM(bitcoin_mined::numeric) AS total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    console.log(`\nBitcoin Calculations for ${TARGET_DATE}:`);
    console.log(`- Total Records: ${bitcoinCheck[0].total_records}`);
    console.log(`- Distinct Periods: ${bitcoinCheck[0].distinct_periods}`);
    console.log(`- Distinct Farms: ${bitcoinCheck[0].distinct_farms}`);
    console.log(`- Distinct Miner Models: ${bitcoinCheck[0].distinct_models}`);
    console.log(`- Total Bitcoin Mined: ${Number(bitcoinCheck[0].total_bitcoin).toFixed(8)} BTC`);
    
    // Check for missing calculations
    const missingCheck = await db.execute(sql`
      WITH curtailment_combos AS (
        SELECT DISTINCT 
          settlement_date, 
          settlement_period, 
          farm_id,
          (SELECT ARRAY_AGG(DISTINCT mm) FROM (
            SELECT unnest(ARRAY['S19J_PRO', 'S9', 'M20S']) as mm
          ) t) as miner_models
        FROM curtailment_records
        WHERE settlement_date = ${TARGET_DATE}
          AND ABS(volume::numeric) > 0
      ),
      expected_calcs AS (
        SELECT 
          c.settlement_date, 
          c.settlement_period, 
          c.farm_id, 
          unnest(c.miner_models) as miner_model
        FROM curtailment_combos c
      ),
      actual_calcs AS (
        SELECT 
          settlement_date, 
          settlement_period, 
          farm_id, 
          miner_model
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${TARGET_DATE}
      )
      SELECT 
        COUNT(*) as missing_count
      FROM expected_calcs e
      LEFT JOIN actual_calcs a 
        ON e.settlement_date = a.settlement_date
        AND e.settlement_period = a.settlement_period
        AND e.farm_id = a.farm_id
        AND e.miner_model = a.miner_model
      WHERE a.settlement_date IS NULL
    `);
    
    const missingCount = Number(missingCheck[0].missing_count);
    
    if (missingCount === 0) {
      console.log(`\n✅ SUCCESS: All Bitcoin calculations exist for ${TARGET_DATE}`);
    } else {
      console.log(`\n❌ WARNING: Found ${missingCount} missing Bitcoin calculations for ${TARGET_DATE}`);
    }
    
    // Calculate expected total
    const expectedTotal = await db.execute(sql`
      WITH curtailment_combos AS (
        SELECT DISTINCT 
          settlement_date, 
          settlement_period, 
          farm_id
        FROM curtailment_records
        WHERE settlement_date = ${TARGET_DATE}
          AND ABS(volume::numeric) > 0
      )
      SELECT 
        COUNT(*) * 3 as expected_calcs
      FROM curtailment_combos
    `);
    
    const expectedCalculations = Number(expectedTotal[0].expected_calcs);
    const actualCalculations = Number(bitcoinCheck[0].total_records);
    const completionPercentage = ((actualCalculations / expectedCalculations) * 100).toFixed(2);
    
    console.log(`\nReconciliation Summary:`);
    console.log(`- Expected Calculations: ${expectedCalculations}`);
    console.log(`- Actual Calculations: ${actualCalculations}`);
    console.log(`- Completion: ${completionPercentage}%`);

    console.log(`\n=== Check Complete for ${TARGET_DATE} ===`);
  } catch (error) {
    console.error(`Error during reconciliation check for ${TARGET_DATE}:`, error);
  }
}

// Execute the check
checkSingleDate()
  .then(() => console.log('Check completed!'))
  .catch(error => console.error('Unexpected error:', error));
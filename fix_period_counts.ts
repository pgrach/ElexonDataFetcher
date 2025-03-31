/**
 * Fix Period Counts
 * 
 * This script analyzes the historical_bitcoin_calculations table for 2025-03-29
 * to identify missing calculations by comparing expected vs. actual counts.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import { processSingleDay } from './server/services/bitcoinService';

// Date to process
const date = '2025-03-29';
const MINER_MODEL_LIST = ['S19J_PRO', 'S9', 'M20S'];

async function checkReconciliation() {
  try {
    console.log(`\n=== Checking Reconciliation for ${date} ===`);
    
    // Get expected calculations based on curtailment_records
    const expectedQuery = `
      WITH CurtailmentCounts AS (
        SELECT
          settlement_period,
          COUNT(*) AS farm_count
        FROM
          curtailment_records
        WHERE
          settlement_date = '${date}'
        GROUP BY
          settlement_period
      )
      SELECT
        SUM(farm_count) * 3 AS expected_total,
        COUNT(*) * 3 AS expected_by_periods
      FROM
        CurtailmentCounts
    `;
    
    const expectedResult = await db.execute(sql.raw(expectedQuery));
    const expectedTotal = parseInt(expectedResult.rows[0].expected_total);
    const expectedByPeriods = parseInt(expectedResult.rows[0].expected_by_periods);
    
    // Get actual calculation counts
    const actualQuery = `
      SELECT
        COUNT(*) AS actual_count,
        miner_model,
        COUNT(DISTINCT settlement_period) AS periods_covered
      FROM
        historical_bitcoin_calculations
      WHERE
        settlement_date = '${date}'
      GROUP BY
        miner_model
    `;
    
    const actualResult = await db.execute(sql.raw(actualQuery));
    let totalActual = 0;
    
    console.log(`\nMiner Model Calculations:`);
    for (const row of actualResult.rows) {
      console.log(`  ${row.miner_model}: ${row.actual_count} calculations across ${row.periods_covered} periods`);
      totalActual += parseInt(row.actual_count);
    }
    
    // Compare expected vs actual
    console.log(`\nReconciliation Status:`);
    console.log(`  Expected calculations (by farm count): ${expectedTotal}`);
    console.log(`  Expected calculations (by periods): ${expectedByPeriods}`);
    console.log(`  Actual calculations: ${totalActual}`);
    console.log(`  Completion rate: ${(totalActual / expectedTotal * 100).toFixed(2)}%`);
    
    if (totalActual < expectedTotal) {
      console.log(`\nMissing ${expectedTotal - totalActual} calculations...`);
      
      // Deep analyze to find specific missing calculations
      await deepAnalyze();
      
      // Try to fix
      await fixCalculations();
    } else {
      console.log(`\nAll calculations are present or exceeding expected count!`);
    }
    
  } catch (error) {
    console.error(`Error checking reconciliation:`, error);
  }
}

async function deepAnalyze() {
  try {
    console.log(`\n=== Deep Analysis of Missing Calculations ===`);
    
    // Get counts by period and model
    const byPeriodQuery = `
      WITH CurtailmentCounts AS (
        SELECT
          settlement_period,
          COUNT(*) AS farm_count
        FROM
          curtailment_records
        WHERE
          settlement_date = '${date}'
        GROUP BY
          settlement_period
      ),
      CalculationCounts AS (
        SELECT
          settlement_period,
          miner_model,
          COUNT(*) AS actual_count
        FROM
          historical_bitcoin_calculations
        WHERE
          settlement_date = '${date}'
        GROUP BY
          settlement_period, miner_model
      )
      SELECT
        c.settlement_period,
        c.farm_count,
        c.farm_count * 3 AS expected_calcs,
        COALESCE(SUM(calc.actual_count), 0) AS actual_calcs,
        c.farm_count * 3 - COALESCE(SUM(calc.actual_count), 0) AS missing_calcs
      FROM
        CurtailmentCounts c
      LEFT JOIN
        CalculationCounts calc ON c.settlement_period = calc.settlement_period
      GROUP BY
        c.settlement_period, c.farm_count
      HAVING
        c.farm_count * 3 > COALESCE(SUM(calc.actual_count), 0)
      ORDER BY
        c.settlement_period
    `;
    
    const byPeriodResult = await db.execute(sql.raw(byPeriodQuery));
    if (byPeriodResult.rows.length === 0) {
      console.log(`No periods with missing calculations identified.`);
      return;
    }
    
    console.log(`\nPeriods with missing calculations:`);
    let totalMissing = 0;
    for (const row of byPeriodResult.rows) {
      console.log(`  Period ${row.settlement_period}: ${row.actual_calcs}/${row.expected_calcs} (missing ${row.missing_calcs})`);
      totalMissing += parseInt(row.missing_calcs);
    }
    
    console.log(`\nTotal missing calculations: ${totalMissing}`);
    
    // Check by miner model
    const byModelQuery = `
      WITH FarmPeriodCounts AS (
        SELECT
          miner_model,
          settlement_period,
          COUNT(DISTINCT farm_id) AS farm_count
        FROM
          curtailment_records c
        CROSS JOIN
          (SELECT unnest(ARRAY['S19J_PRO', 'S9', 'M20S']) AS miner_model) m
        WHERE
          settlement_date = '${date}'
        GROUP BY
          miner_model, settlement_period
      ),
      CalculationCounts AS (
        SELECT
          miner_model,
          settlement_period,
          COUNT(*) AS actual_count
        FROM
          historical_bitcoin_calculations
        WHERE
          settlement_date = '${date}'
        GROUP BY
          miner_model, settlement_period
      )
      SELECT
        f.miner_model,
        f.settlement_period,
        f.farm_count AS expected_farms,
        COALESCE(c.actual_count, 0) AS actual_calcs,
        f.farm_count - COALESCE(c.actual_count, 0) AS missing_calcs
      FROM
        FarmPeriodCounts f
      LEFT JOIN
        CalculationCounts c ON f.miner_model = c.miner_model AND f.settlement_period = c.settlement_period
      WHERE
        f.farm_count > COALESCE(c.actual_count, 0)
      ORDER BY
        f.miner_model, f.settlement_period
    `;
    
    const byModelResult = await db.execute(sql.raw(byModelQuery));
    if (byModelResult.rows.length === 0) {
      console.log(`No model-specific missing calculations identified.`);
      return;
    }
    
    console.log(`\nMissing calculations by miner model and period:`);
    for (const row of byModelResult.rows) {
      console.log(`  ${row.miner_model} Period ${row.settlement_period}: ${row.actual_calcs}/${row.expected_farms} (missing ${row.missing_calcs})`);
    }
    
  } catch (error) {
    console.error(`Error during deep analysis:`, error);
  }
}

async function fixCalculations() {
  try {
    console.log(`\n=== Fixing Missing Calculations ===`);
    
    for (const minerModel of MINER_MODEL_LIST) {
      console.log(`Processing ${minerModel} for ${date}...`);
      await processSingleDay(date, minerModel);
      console.log(`Successfully processed ${minerModel} for ${date}`);
    }
    
    // Check if we fixed the issue
    const countsQuery = `
      SELECT
        COUNT(*) AS actual_count
      FROM
        historical_bitcoin_calculations
      WHERE
        settlement_date = '${date}'
    `;
    
    const countsResult = await db.execute(sql.raw(countsQuery));
    const newCount = parseInt(countsResult.rows[0].actual_count);
    
    console.log(`\nAfter fix: ${newCount} calculations`);
    
  } catch (error) {
    console.error(`Error fixing calculations:`, error);
  }
}

// Run the check and fix
checkReconciliation()
  .then(() => {
    console.log('\nProcess complete!');
    process.exit(0);
  })
  .catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
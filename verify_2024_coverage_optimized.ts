/**
 * Optimized 2024 Data Coverage Verification
 * 
 * This script uses optimized SQL queries and batch processing to efficiently verify
 * the reconciliation between curtailment_records and historicalBitcoinCalculations tables.
 */

import { db } from "./db";
import { sql } from "drizzle-orm";

// Configuration
const START_DATE = '2024-01-01';
const END_DATE = '2024-12-31';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const BATCH_SIZE = 10; // Process in batches to avoid timeouts

// Type definitions for database results
interface ReconciliationIssue {
  date: string;
  curtailment_period_count: number;
  calculation_counts: Record<string, number>;
  curtailment_periods: number[];
  calculation_periods: Record<string, number[]>;
}

async function verifyReconciliationOptimized() {
  console.log('=== Optimized Verification of 2024 Data Coverage ===');
  
  // Get all dates with curtailment records in 2024
  const allDatesQuery = `
    SELECT settlement_date::text as date
    FROM curtailment_records
    WHERE settlement_date BETWEEN '${START_DATE}' AND '${END_DATE}'
    AND ABS(volume::numeric) > 0
    GROUP BY settlement_date
    ORDER BY settlement_date
  `;
  
  const allDatesResult = await db.execute(sql.raw(allDatesQuery));
  const allDates = allDatesResult.rows.map((row: any) => row.date);
  
  console.log(`Found ${allDates.length} dates in 2024 with curtailment records`);
  
  // Find all potential issues using a single optimized query
  const discrepancyQuery = `
    WITH curtailment_periods AS (
      SELECT 
        settlement_date::text as date,
        array_agg(DISTINCT settlement_period) as periods,
        COUNT(DISTINCT settlement_period) as period_count
      FROM curtailment_records
      WHERE settlement_date BETWEEN '${START_DATE}' AND '${END_DATE}'
      AND ABS(volume::numeric) > 0
      GROUP BY settlement_date
    ),
    calculation_periods AS (
      SELECT 
        settlement_date::text as date,
        miner_model,
        array_agg(DISTINCT settlement_period) as periods,
        COUNT(DISTINCT settlement_period) as period_count
      FROM historical_bitcoin_calculations
      WHERE settlement_date BETWEEN '${START_DATE}' AND '${END_DATE}'
      GROUP BY settlement_date, miner_model
    )
    SELECT
      cp.date,
      cp.period_count as curtailment_period_count,
      jsonb_object_agg(
        COALESCE(calc.miner_model, 'missing'),
        COALESCE(calc.period_count, 0)
      ) as calculation_counts,
      cp.periods as curtailment_periods,
      jsonb_object_agg(
        COALESCE(calc.miner_model, 'missing'),
        COALESCE(calc.periods, '{}'::int[])
      ) as calculation_periods
    FROM
      curtailment_periods cp
    LEFT JOIN
      calculation_periods calc ON cp.date = calc.date
    GROUP BY
      cp.date, cp.period_count, cp.periods
    HAVING
      cp.period_count > COALESCE(MIN(calc.period_count), 0)
      OR COUNT(DISTINCT calc.miner_model) < 3
    ORDER BY
      cp.date
  `;
  
  const discrepancyResult = await db.execute(sql.raw(discrepancyQuery));
  const datesWithIssues = discrepancyResult.rows as ReconciliationIssue[];
  
  if (datesWithIssues.length === 0) {
    console.log('\n✓ All dates fully reconciled! No issues found across all 2024 data.');
    console.log(`Verified ${allDates.length} dates with complete coverage for all miner models.`);
    return;
  }
  
  // Process dates with issues
  console.log(`\nFound ${datesWithIssues.length} dates with potential reconciliation issues`);
  console.log('\n=== Detailed Analysis of Issues ===');
  
  // Process in batches to avoid timeout
  for (let i = 0; i < datesWithIssues.length; i += BATCH_SIZE) {
    const batch = datesWithIssues.slice(i, Math.min(i + BATCH_SIZE, datesWithIssues.length));
    
    console.log(`\nProcessing batch ${Math.floor(i/BATCH_SIZE) + 1}/${Math.ceil(datesWithIssues.length/BATCH_SIZE)}`);
    
    for (const issue of batch) {
      const typedIssue = issue as ReconciliationIssue;
      console.log(`\nDate: ${typedIssue.date}`);
      console.log(`Curtailment periods: ${typedIssue.curtailment_period_count}`);
      
      // Check each miner model
      for (const model of MINER_MODELS) {
        const calculationCount = typedIssue.calculation_counts[model] || 0;
        
        if (calculationCount < typedIssue.curtailment_period_count) {
          console.log(`- ${model}: Found ${calculationCount}/${typedIssue.curtailment_period_count} periods`);
          
          // Find missing periods
          const curtailmentPeriods = typedIssue.curtailment_periods;
          const calculationPeriods = typedIssue.calculation_periods[model] || [];
          const missingPeriods = curtailmentPeriods.filter((p: number) => !calculationPeriods.includes(p));
          
          if (missingPeriods.length > 0) {
            console.log(`  Missing periods: ${missingPeriods.join(', ')}`);
          }
        }
      }
    }
  }
  
  // Check for dates with mining data but no curtailment
  const orphanedCalculationsQuery = `
    WITH calculation_dates AS (
      SELECT settlement_date::text as date
      FROM historical_bitcoin_calculations
      WHERE settlement_date BETWEEN '${START_DATE}' AND '${END_DATE}'
      GROUP BY settlement_date
    ),
    curtailment_dates AS (
      SELECT settlement_date::text as date
      FROM curtailment_records
      WHERE settlement_date BETWEEN '${START_DATE}' AND '${END_DATE}'
      AND ABS(volume::numeric) > 0
      GROUP BY settlement_date
    )
    SELECT
      cd.date
    FROM
      calculation_dates cd
    LEFT JOIN
      curtailment_dates cud ON cd.date = cud.date
    WHERE
      cud.date IS NULL
    ORDER BY
      cd.date
  `;
  
  const orphanedResult = await db.execute(sql.raw(orphanedCalculationsQuery));
  const orphanedDates = orphanedResult.rows;
  
  if (orphanedDates.length > 0) {
    console.log(`\n=== Found ${orphanedDates.length} dates with Bitcoin calculations but no curtailment ===`);
    orphanedDates.forEach((row: any) => {
      console.log(`- ${row.date}`);
    });
  }
  
  // Summary
  console.log('\n=== Verification Summary ===');
  console.log(`Total dates with curtailment: ${allDates.length}`);
  console.log(`Dates with reconciliation issues: ${datesWithIssues.length}`);
  console.log(`Dates with orphaned calculations: ${orphanedDates.length}`);
  
  if (datesWithIssues.length === 0 && orphanedDates.length === 0) {
    console.log('✓ No issues found. The 2024 data is fully reconciled.');
  } else {
    console.log('× Reconciliation issues found. Use reconcile2024.ts to fix these issues.');
  }
}

// Run the verification
verifyReconciliationOptimized()
  .then(() => {
    console.log('\nVerification completed');
    process.exit(0);
  })
  .catch(error => {
    console.error('Error during verification:', error);
    process.exit(1);
  });
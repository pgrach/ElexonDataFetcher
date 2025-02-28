/**
 * Simple script to run the reconciliation between curtailment_records and historical_bitcoin_calculations.
 * This is a streamlined version of the various reconciliation tools.
 */

import { db } from "./db";
import { reconcileDay } from "./server/services/historicalReconciliation";
import { sql } from "drizzle-orm";
import pLimit from "p-limit";
import { format } from "date-fns";

// Configuration
const CONCURRENCY_LIMIT = 3; // Number of days to process in parallel
const BATCH_SIZE = 14; // Days to process in each batch
const DELAY_BETWEEN_BATCHES = 5000; // 5 seconds between batches
const BACKOFF_TIME = 200; // 200ms between individual day processing

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function getReconciliationStatus() {
  console.log("=== Bitcoin Calculations Reconciliation Status ===\n");
  console.log("Checking current reconciliation status...\n");

  try {
    const result = await db.execute(sql`
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
      ),
      miner_stats AS (
        SELECT 
          miner_model,
          COUNT(*) AS model_count
        FROM historical_bitcoin_calculations
        GROUP BY miner_model
      )
      SELECT 
        cs.total_records AS curtailment_records,
        cs.unique_combinations AS unique_combinations,
        COALESCE(calcs.total_calculations, 0) AS bitcoin_calculations,
        cs.unique_combinations * 3 AS expected_calculations
      FROM curtailment_stats cs
      LEFT JOIN calculation_stats calcs ON 1=1
    `);

    if (result.rows.length > 0) {
      const {
        curtailment_records,
        unique_combinations,
        bitcoin_calculations,
        expected_calculations
      } = result.rows[0];

      const missingCalculations = parseInt(expected_calculations) - parseInt(bitcoin_calculations);
      const reconciliationPercentage = Math.round((parseInt(bitcoin_calculations) / parseInt(expected_calculations)) * 100);

      console.log("=== Overall Status ===");
      console.log(`Curtailment Records: ${curtailment_records}`);
      console.log(`Unique Period-Farm Combinations: ${unique_combinations}`);
      console.log(`Bitcoin Calculations: ${bitcoin_calculations}`);
      console.log(`Expected Calculations: ${expected_calculations}`);
      console.log(`Missing Calculations: ${missingCalculations}`);
      console.log(`Reconciliation: ${reconciliationPercentage}%\n`);

      const minerStats = await db.execute(sql`
        SELECT miner_model, COUNT(*) as count
        FROM historical_bitcoin_calculations
        GROUP BY miner_model
        ORDER BY miner_model
      `);

      console.log("Bitcoin Calculations by Model:");
      if (minerStats.rows.length === 0) {
        console.log("- No calculations found");
      } else {
        minerStats.rows.forEach(row => {
          console.log(`- ${row.miner_model}: ${row.count}`);
        });
      }
    }
  } catch (error) {
    console.error("Error getting reconciliation status:", error);
  }
}

async function findDatesWithMissingCalculations() {
  console.log("=== Finding Dates with Missing Calculations ===\n");

  try {
    const result = await db.execute(sql`
      WITH date_combinations AS (
        SELECT 
          settlement_date,
          COUNT(DISTINCT (settlement_period, farm_id)) AS combinations
        FROM curtailment_records
        GROUP BY settlement_date
      ),
      date_calculations AS (
        SELECT 
          settlement_date,
          COUNT(*) / 3 AS calculations
        FROM historical_bitcoin_calculations
        GROUP BY settlement_date
      )
      SELECT 
        dc.settlement_date,
        dc.combinations,
        COALESCE(calcs.calculations, 0) AS calculations,
        dc.combinations * 3 AS expected,
        CASE 
          WHEN COALESCE(calcs.calculations, 0) = dc.combinations THEN 100
          ELSE ROUND((COALESCE(calcs.calculations, 0) / dc.combinations::float) * 100)
        END AS percentage
      FROM date_combinations dc
      LEFT JOIN date_calculations calcs ON dc.settlement_date = calcs.settlement_date
      WHERE COALESCE(calcs.calculations, 0) < dc.combinations
      ORDER BY dc.settlement_date DESC
    `);

    if (result.rows.length > 0) {
      const dates = result.rows.map(row => row.settlement_date);
      const total = result.rows.reduce((acc, row) => {
        return acc + (parseInt(row.expected) - parseInt(row.calculations));
      }, 0);

      console.log(`Found ${dates.length} dates with missing calculations (${total} calculations missing total)\n`);
      
      console.log("Top 10 dates with most missing calculations:");
      result.rows.slice(0, 10).forEach(row => {
        const missing = parseInt(row.expected) - parseInt(row.calculations);
        console.log(`- ${row.settlement_date}: ${row.calculations}/${row.expected} (${row.percentage}%) - Missing ${missing} calculations`);
      });

      return dates;
    } else {
      console.log("No dates with missing calculations found");
      return [];
    }
  } catch (error) {
    console.error("Error finding dates with missing calculations:", error);
    return [];
  }
}

async function getDecemberReconciliationStatus() {
  console.log("=== December 2023 Reconciliation Status ===\n");

  try {
    const result = await db.execute(sql`
      WITH december_curtailment AS (
        SELECT 
          COUNT(*) AS total_records,
          COUNT(DISTINCT (settlement_date, settlement_period, farm_id)) AS unique_combinations
        FROM curtailment_records
        WHERE settlement_date BETWEEN '2023-12-01' AND '2023-12-31'
      ),
      december_calculations AS (
        SELECT 
          COUNT(*) AS total_calculations,
          COUNT(DISTINCT (settlement_date, settlement_period, farm_id, miner_model)) AS unique_combinations
        FROM historical_bitcoin_calculations
        WHERE settlement_date BETWEEN '2023-12-01' AND '2023-12-31'
      ),
      december_miner_stats AS (
        SELECT 
          miner_model,
          COUNT(*) AS model_count
        FROM historical_bitcoin_calculations
        WHERE settlement_date BETWEEN '2023-12-01' AND '2023-12-31'
        GROUP BY miner_model
      )
      SELECT 
        dc.total_records AS curtailment_records,
        dc.unique_combinations AS unique_combinations,
        COALESCE(dcc.total_calculations, 0) AS bitcoin_calculations,
        dc.unique_combinations * 3 AS expected_calculations
      FROM december_curtailment dc
      LEFT JOIN december_calculations dcc ON 1=1
    `);

    if (result.rows.length > 0) {
      const {
        curtailment_records,
        unique_combinations,
        bitcoin_calculations,
        expected_calculations
      } = result.rows[0];

      const missingCalculations = parseInt(expected_calculations) - parseInt(bitcoin_calculations);
      const reconciliationPercentage = Math.round((parseInt(bitcoin_calculations) / parseInt(expected_calculations)) * 100);

      console.log("Overall Status:");
      console.log(`Curtailment Records: ${curtailment_records}`);
      console.log(`Unique Period-Farm Combinations: ${unique_combinations}`);
      console.log(`Bitcoin Calculations: ${bitcoin_calculations}`);
      console.log(`Expected Calculations: ${expected_calculations}`);
      console.log(`Reconciliation: ${reconciliationPercentage}%\n`);

      const minerStats = await db.execute(sql`
        SELECT miner_model, COUNT(*) as count
        FROM historical_bitcoin_calculations
        WHERE settlement_date BETWEEN '2023-12-01' AND '2023-12-31'
        GROUP BY miner_model
        ORDER BY miner_model
      `);

      console.log("Bitcoin Calculations by Model:");
      if (minerStats.rows.length === 0) {
        console.log("- No calculations found");
      } else {
        minerStats.rows.forEach(row => {
          console.log(`- ${row.miner_model}: ${row.count}`);
        });
      }

      return {
        curtailmentRecords: parseInt(curtailment_records),
        uniqueCombinations: parseInt(unique_combinations),
        bitcoinCalculations: parseInt(bitcoin_calculations),
        expectedCalculations: parseInt(expected_calculations),
        reconciliationPercentage
      };
    }
  } catch (error) {
    console.error("Error getting December reconciliation status:", error);
  }

  return null;
}

async function findDecemberDatesWithMissingCalculations() {
  console.log("=== Finding December 2023 Dates with Missing Calculations ===\n");

  try {
    const result = await db.execute(sql`
      WITH date_combinations AS (
        SELECT 
          settlement_date,
          COUNT(DISTINCT (settlement_period, farm_id)) AS combinations
        FROM curtailment_records
        WHERE settlement_date BETWEEN '2023-12-01' AND '2023-12-31'
        GROUP BY settlement_date
      ),
      date_calculations AS (
        SELECT 
          settlement_date,
          COUNT(*) / 3 AS calculations
        FROM historical_bitcoin_calculations
        WHERE settlement_date BETWEEN '2023-12-01' AND '2023-12-31'
        GROUP BY settlement_date
      )
      SELECT 
        dc.settlement_date,
        dc.combinations,
        COALESCE(calcs.calculations, 0) AS calculations,
        dc.combinations * 3 AS expected,
        CASE 
          WHEN COALESCE(calcs.calculations, 0) = dc.combinations THEN 100
          ELSE ROUND((COALESCE(calcs.calculations, 0) / dc.combinations::float) * 100)
        END AS percentage
      FROM date_combinations dc
      LEFT JOIN date_calculations calcs ON dc.settlement_date = calcs.settlement_date
      ORDER BY dc.settlement_date
    `);

    if (result.rows.length > 0) {
      const datesWithMissing = result.rows.filter(row => parseInt(row.calculations) < parseInt(row.combinations));
      const total = datesWithMissing.reduce((acc, row) => {
        return acc + (parseInt(row.expected) - parseInt(row.calculations));
      }, 0);

      console.log(`Found ${datesWithMissing.length} dates with missing calculations (${total} calculations missing total)\n`);
      
      console.log("All December dates with missing calculations:");
      datesWithMissing.forEach(row => {
        const missing = parseInt(row.expected) - parseInt(row.calculations);
        console.log(`- ${row.settlement_date}: ${row.calculations}/${row.expected} (${row.percentage}%) - Missing ${missing} calculations`);
      });

      return datesWithMissing.map(row => ({
        date: row.settlement_date,
        missing: parseInt(row.expected) - parseInt(row.calculations),
        calculationCount: parseInt(row.calculations),
        expectedCount: parseInt(row.expected),
        completionPercentage: parseInt(row.percentage)
      }));
    } else {
      console.log("No December dates with missing calculations found");
      return [];
    }
  } catch (error) {
    console.error("Error finding December dates with missing calculations:", error);
    return [];
  }
}

async function processBatch(dates: string[], concurrency = CONCURRENCY_LIMIT): Promise<void> {
  console.log(`Processing batch of ${dates.length} dates with concurrency ${concurrency}...`);
  
  const limit = pLimit(concurrency);
  const promises = dates.map(date => limit(async () => {
    try {
      console.log(`[${date}] Starting reconciliation...`);
      await reconcileDay(date);
      console.log(`[${date}] Reconciliation complete`);
      // Add backoff between individual days to prevent overloading
      await sleep(BACKOFF_TIME);
      return true;
    } catch (error) {
      console.error(`[${date}] Error during reconciliation:`, error);
      return false;
    }
  }));

  const results = await Promise.all(promises);
  const successCount = results.filter(Boolean).length;
  console.log(`Batch processing complete. ${successCount}/${dates.length} dates successfully reconciled.`);
}

async function getBatchesForMonthlyReconciliation(year: number, month: number): Promise<string[][]> {
  // Get all the dates for the specified month
  const startDate = new Date(year, month - 1, 1);
  const endDate = new Date(year, month, 0); // Last day of the month
  
  const allDates: string[] = [];
  const currentDate = new Date(startDate);
  
  while (currentDate <= endDate) {
    allDates.push(format(currentDate, 'yyyy-MM-dd'));
    currentDate.setDate(currentDate.getDate() + 1);
  }
  
  // Split into batches
  const batches: string[][] = [];
  for (let i = 0; i < allDates.length; i += BATCH_SIZE) {
    batches.push(allDates.slice(i, i + BATCH_SIZE));
  }
  
  return batches;
}

async function reconcileMonth(year: number, month: number): Promise<void> {
  const monthName = new Date(year, month - 1, 1).toLocaleString('default', { month: 'long' });
  console.log(`=== Starting Reconciliation for ${monthName} ${year} ===`);
  
  const batches = await getBatchesForMonthlyReconciliation(year, month);
  console.log(`Divided month into ${batches.length} batches of up to ${BATCH_SIZE} days each`);
  
  let batchNumber = 1;
  for (const batch of batches) {
    console.log(`\nProcessing batch ${batchNumber}/${batches.length} for ${monthName} ${year}`);
    console.log(`Dates in this batch: ${batch.join(', ')}`);
    
    await processBatch(batch);
    
    // Add delay between batches to prevent resource overload
    if (batchNumber < batches.length) {
      console.log(`Waiting ${DELAY_BETWEEN_BATCHES/1000} seconds before next batch...`);
      await sleep(DELAY_BETWEEN_BATCHES);
    }
    
    batchNumber++;
  }
  
  console.log(`\n=== Completed Reconciliation for ${monthName} ${year} ===`);
}

async function main() {
  const args = process.argv.slice(2);
  const command = args[0] || 'status';

  try {
    switch (command) {
      case 'status':
        await getReconciliationStatus();
        break;
      
      case 'find':
        await findDatesWithMissingCalculations();
        break;
      
      case 'december-status':
        await getDecemberReconciliationStatus();
        break;
      
      case 'december-find':
        await findDecemberDatesWithMissingCalculations();
        break;
      
      case 'reconcile-month':
        const year = parseInt(args[1]);
        const month = parseInt(args[2]);
        
        if (isNaN(year) || isNaN(month) || month < 1 || month > 12) {
          console.error('Invalid year or month. Usage: npm run reconcile month YYYY MM');
          process.exit(1);
        }
        
        await reconcileMonth(year, month);
        break;
      
      case 'reconcile-date':
        const date = args[1];
        if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
          console.error('Invalid date format. Use: YYYY-MM-DD');
          process.exit(1);
        }
        
        console.log(`=== Reconciling Bitcoin Calculations for ${date} ===`);
        await reconcileDay(date);
        console.log(`=== Reconciliation for ${date} Complete ===`);
        break;
      
      default:
        console.log('Available commands:');
        console.log('  status               - Show overall reconciliation status');
        console.log('  find                 - Find dates with missing calculations');
        console.log('  december-status      - Show December 2023 reconciliation status');
        console.log('  december-find        - Find dates with missing calculations in December 2023');
        console.log('  reconcile-month YYYY MM - Reconcile specified year and month');
        console.log('  reconcile-date YYYY-MM-DD - Reconcile a specific date');
    }
  } catch (error) {
    console.error('Error executing command:', error);
  }

  console.log('\n=== Reconciliation Run Complete ===');
}

if (require.main === module) {
  main().catch(console.error);
}
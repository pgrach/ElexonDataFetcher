/**
 * Simple script to run the reconciliation between curtailment_records and historical_bitcoin_calculations.
 * This is a streamlined version of the various reconciliation tools.
 */

import { db } from "./db";
import { sql } from "drizzle-orm";
import { fixDate } from "./reconciliation";

const DECEMBER_2023_DATES = [
  "2023-12-01", "2023-12-02", "2023-12-03", "2023-12-04", "2023-12-05",
  "2023-12-06", "2023-12-07", "2023-12-08", "2023-12-09", "2023-12-10",
  "2023-12-11", "2023-12-12", "2023-12-13", "2023-12-14", "2023-12-15",
  "2023-12-16", "2023-12-17", "2023-12-18", "2023-12-19", "2023-12-20",
  "2023-12-21", "2023-12-22", "2023-12-23", "2023-12-24", "2023-12-25",
  "2023-12-26", "2023-12-27", "2023-12-28", "2023-12-29", "2023-12-30",
  "2023-12-31"
];

async function getDecemberReconciliationStatus() {
  const result = await db.execute(sql`
    WITH december_curtailment AS (
      SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT (settlement_date || '-' || settlement_period || '-' || farm_id)) as unique_combinations
      FROM curtailment_records
      WHERE settlement_date >= '2023-12-01' AND settlement_date <= '2023-12-31'
    ),
    december_bitcoin AS (
      SELECT
        miner_model,
        COUNT(*) as calculation_count
      FROM historical_bitcoin_calculations
      WHERE settlement_date >= '2023-12-01' AND settlement_date <= '2023-12-31'
      GROUP BY miner_model
    )
    SELECT 
      c.total_records as curtailment_records,
      c.unique_combinations as unique_combinations,
      COALESCE(b.miner_model, 'TOTAL') as miner_model,
      COALESCE(b.calculation_count, SUM(b.calculation_count) OVER ()) as calculation_count,
      c.unique_combinations * 3 as expected_calculations,
      ROUND((COALESCE(b.calculation_count, SUM(b.calculation_count) OVER ()) * 100.0) / 
        NULLIF(c.unique_combinations * 3, 0), 2) as reconciliation_percentage
    FROM december_curtailment c
    LEFT JOIN december_bitcoin b ON 1=1
    GROUP BY c.total_records, c.unique_combinations, b.miner_model, b.calculation_count
  `);

  console.log("=== December 2023 Reconciliation Status ===\n");
  
  if (result.rows.length === 0) {
    console.log("No data found for December 2023.");
    return null;
  }
  
  let totalRow = result.rows.find(row => row.miner_model === 'TOTAL');
  if (!totalRow) totalRow = result.rows[0];
  
  const status = {
    curtailmentRecords: Number(totalRow.curtailment_records),
    uniqueCombinations: Number(totalRow.unique_combinations),
    totalCalculations: Number(totalRow.calculation_count || 0),
    expectedCalculations: Number(totalRow.expected_calculations || 0),
    reconciliationPercentage: Number(totalRow.reconciliation_percentage || 0)
  };
  
  console.log(`Curtailment Records: ${status.curtailmentRecords}`);
  console.log(`Unique Period-Farm Combinations: ${status.uniqueCombinations}`);
  console.log(`Bitcoin Calculations: ${status.totalCalculations}`);
  console.log(`Expected Calculations: ${status.expectedCalculations}`);
  console.log(`December 2023 Reconciliation: ${status.reconciliationPercentage}%\n`);

  // Print by miner model
  result.rows.forEach(row => {
    if (row.miner_model !== 'TOTAL') {
      console.log(`- ${row.miner_model}: ${row.calculation_count || 0} calculations`);
    }
  });
  
  return status;
}

async function findDecemberDatesWithMissingCalculations() {
  const result = await db.execute(sql`
    WITH date_combinations AS (
      SELECT 
        settlement_date,
        COUNT(DISTINCT (settlement_period || '-' || farm_id)) as unique_combinations
      FROM curtailment_records
      WHERE settlement_date >= '2023-12-01' AND settlement_date <= '2023-12-31'
      GROUP BY settlement_date
    ),
    date_calculations AS (
      SELECT 
        d.settlement_date,
        COUNT(DISTINCT b.id) as calculation_count,
        d.unique_combinations * 3 as expected_count
      FROM date_combinations d
      LEFT JOIN historical_bitcoin_calculations b 
        ON d.settlement_date = b.settlement_date
      GROUP BY d.settlement_date, d.unique_combinations
    )
    SELECT 
      settlement_date::text as date,
      calculation_count,
      expected_count,
      ROUND((calculation_count * 100.0) / expected_count, 2) as completion_percentage
    FROM date_calculations
    WHERE calculation_count < expected_count
    ORDER BY completion_percentage ASC, settlement_date
  `);
  
  const missingDates = result.rows.map(row => ({
    date: String(row.date),
    actual: Number(row.calculation_count),
    expected: Number(row.expected_count),
    completionPercentage: Number(row.completion_percentage)
  }));
  
  console.log("\n=== December 2023 Dates With Missing Calculations ===\n");
  
  if (missingDates.length === 0) {
    console.log("No missing dates found for December 2023!");
    return [];
  }
  
  console.log(`Found ${missingDates.length} dates with missing calculations:`);
  missingDates.forEach(d => {
    console.log(`- ${d.date}: ${d.actual}/${d.expected} (${d.completionPercentage}%)`);
  });
  
  return missingDates;
}

async function main() {
  try {
    console.log("=== Starting December 2023 Reconciliation ===\n");
    
    // Check initial status
    const initialStatus = await getDecemberReconciliationStatus();
    
    if (!initialStatus) {
      console.log("No data available for December 2023. Exiting.");
      return;
    }
    
    // If already at 100%, we're done
    if (initialStatus.reconciliationPercentage === 100) {
      console.log("✅ December 2023 is already at 100% reconciliation! No action needed.");
      return;
    }
    
    // Find dates with missing calculations
    const missingDates = await findDecemberDatesWithMissingCalculations();
    
    if (missingDates.length === 0) return;
    
    // Ask user if they want to proceed with fixing
    console.log("\nReady to fix missing calculations for December 2023.");
    console.log("This process may take several minutes depending on the amount of missing data.");
    console.log("\nTo proceed, run: npx tsx reconciliation.ts reconcile");
    
  } catch (error) {
    console.error("Error during reconciliation process:", error);
    throw error;
  }
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .then(() => {
      console.log("\n=== December 2023 Reconciliation Check Complete ===");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
}
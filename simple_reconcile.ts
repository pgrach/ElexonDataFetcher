/**
 * Simplified Reconciliation Tool
 * 
 * This tool provides a streamlined way to reconcile missing bitcoin calculations.
 * It focuses on the core functionality without complex dependencies to ensure reliability.
 */

import { db } from "./db";
import { sql } from "drizzle-orm";
import { auditAndFixBitcoinCalculations } from "./server/services/historicalReconciliation";

// Constants
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

/**
 * Get summary statistics about reconciliation status
 */
async function getReconciliationStatus() {
  console.log("=== Bitcoin Calculations Reconciliation Status ===\n");
  console.log("Checking current reconciliation status...\n");

  // Get total curtailment records and unique date-period-farm combinations
  const curtailmentResult = await db.execute(sql`
    SELECT 
      COUNT(*) as total_records,
      COUNT(DISTINCT (settlement_date || '-' || settlement_period || '-' || farm_id)) as unique_combinations
    FROM curtailment_records
  `);
  
  const totalCurtailmentRecords = Number(curtailmentResult.rows[0].total_records);
  const uniqueCombinations = Number(curtailmentResult.rows[0].unique_combinations);
  
  // Get Bitcoin calculation counts by miner model
  const bitcoinCounts: Record<string, number> = {};
  
  for (const model of MINER_MODELS) {
    const result = await db.execute(sql`
      SELECT COUNT(*) as count
      FROM historical_bitcoin_calculations
      WHERE miner_model = ${model}
    `);
    
    bitcoinCounts[model] = Number(result.rows[0].count) || 0;
  }
  
  // Expected Bitcoin calculation count for 100% reconciliation
  // For each unique date-period-farm combination, we should have one calculation per miner model
  const expectedTotal = uniqueCombinations * MINER_MODELS.length;
  const actualTotal = Object.values(bitcoinCounts).reduce((sum, count) => sum + Number(count), 0);
  
  // Calculate reconciliation percentage with safety checks
  let reconciliationPercentage = 100;
  if (expectedTotal > 0) {
    reconciliationPercentage = Math.min((actualTotal / expectedTotal) * 100, 100);
  }
  
  const status = {
    totalCurtailmentRecords,
    uniqueDatePeriodFarmCombinations: uniqueCombinations,
    bitcoinCalculationsByModel: bitcoinCounts,
    totalBitcoinCalculations: actualTotal,
    expectedBitcoinCalculations: expectedTotal,
    missingCalculations: expectedTotal - actualTotal,
    reconciliationPercentage: Math.round(reconciliationPercentage * 100) / 100
  };
  
  // Print status
  console.log("=== Overall Status ===");
  console.log(`Curtailment Records: ${status.totalCurtailmentRecords}`);
  console.log(`Unique Period-Farm Combinations: ${status.uniqueDatePeriodFarmCombinations}`);
  console.log(`Bitcoin Calculations: ${status.totalBitcoinCalculations}`);
  console.log(`Expected Calculations: ${status.expectedBitcoinCalculations}`);
  console.log(`Missing Calculations: ${status.missingCalculations}`);
  console.log(`Reconciliation: ${status.reconciliationPercentage}%\n`);
  
  console.log("Bitcoin Calculations by Model:");
  for (const [model, count] of Object.entries(status.bitcoinCalculationsByModel)) {
    console.log(`- ${model}: ${count}`);
  }
  
  return status;
}

/**
 * Find dates with missing Bitcoin calculations
 */
async function findDatesWithMissingCalculations() {
  const result = await db.execute(sql`
    WITH dates_with_curtailment AS (
      SELECT DISTINCT settlement_date
      FROM curtailment_records
      ORDER BY settlement_date DESC
    ),
    unique_date_combos AS (
      SELECT 
        settlement_date,
        COUNT(DISTINCT (settlement_period || '-' || farm_id)) as unique_combinations
      FROM curtailment_records
      GROUP BY settlement_date
    ),
    date_calculations AS (
      SELECT 
        c.settlement_date,
        COUNT(DISTINCT b.id) as calculation_count,
        u.unique_combinations * ${MINER_MODELS.length} as expected_count
      FROM dates_with_curtailment c
      JOIN unique_date_combos u ON c.settlement_date = u.settlement_date
      LEFT JOIN historical_bitcoin_calculations b 
        ON c.settlement_date = b.settlement_date
      GROUP BY c.settlement_date, u.unique_combinations
    )
    SELECT 
      settlement_date::text as date,
      calculation_count,
      expected_count,
      ROUND((calculation_count * 100.0) / expected_count, 2) as completion_percentage
    FROM date_calculations
    WHERE calculation_count < expected_count
    ORDER BY completion_percentage ASC, settlement_date DESC
    LIMIT 30
  `);
  
  const missingDates = result.rows.map(row => ({
    date: String(row.date),
    actual: Number(row.calculation_count),
    expected: Number(row.expected_count),
    completionPercentage: Number(row.completion_percentage)
  }));
  
  console.log("Finding dates with missing calculations...\n");
  
  if (missingDates.length === 0) {
    console.log("No dates with missing calculations found!");
    return [];
  }
  
  console.log(`Found ${missingDates.length} dates with missing calculations:`);
  missingDates.forEach(d => {
    console.log(`- ${d.date}: ${d.actual}/${d.expected} (${d.completionPercentage}%)`);
  });
  
  return missingDates;
}

/**
 * Fix a specific date
 */
async function fixDate(date: string) {
  console.log(`\n=== Fixing Bitcoin Calculations for ${date} ===\n`);
  
  try {
    const result = await auditAndFixBitcoinCalculations(date);
    
    if (result.success) {
      console.log(`✅ ${date}: Fixed - ${result.message}`);
    } else {
      console.log(`❌ ${date}: Failed - ${result.message}`);
    }
    
    return result;
  } catch (error) {
    console.error(`Error fixing ${date}:`, error);
    throw error;
  }
}

/**
 * Fix all missing dates
 */
async function fixAllMissingDates() {
  console.log("=== Fixing All Missing Calculations ===\n");
  
  // Get initial reconciliation status
  const initialStatus = await getReconciliationStatus();
  
  // Find dates with missing calculations
  const missingDates = await findDatesWithMissingCalculations();
  
  if (missingDates.length === 0) return;
  
  // Process the dates
  console.log(`\nProcessing ${missingDates.length} dates...\n`);
  
  let successful = 0;
  let failed = 0;
  
  for (const item of missingDates) {
    try {
      console.log(`Processing ${item.date}...`);
      const result = await auditAndFixBitcoinCalculations(item.date);
      
      if (result.success) {
        console.log(`✅ ${item.date}: Fixed - ${result.message}\n`);
        successful++;
      } else {
        console.log(`❌ ${item.date}: Failed - ${result.message}\n`);
        failed++;
      }
    } catch (error) {
      console.error(`Error processing ${item.date}:`, error);
      failed++;
    }
  }
  
  // Get final reconciliation status
  console.log("\nChecking final reconciliation status...");
  const finalStatus = await getReconciliationStatus();
  
  console.log("\n=== Reconciliation Summary ===");
  console.log(`Dates Processed: ${missingDates.length}`);
  console.log(`Successful: ${successful}`);
  console.log(`Failed: ${failed}`);
  console.log(`Initial Reconciliation: ${initialStatus.reconciliationPercentage}%`);
  console.log(`Final Reconciliation: ${finalStatus.reconciliationPercentage}%`);
}

/**
 * Get December 2023 status
 */
async function getDecemberStatus() {
  console.log("\n=== December 2023 Reconciliation Status ===\n");
  
  const result = await db.execute(sql`
    WITH december_stats AS (
      SELECT 
        settlement_date,
        COUNT(DISTINCT (settlement_period || '-' || farm_id)) * 3 AS expected_count,
        (
          SELECT COUNT(*) 
          FROM historical_bitcoin_calculations
          WHERE historical_bitcoin_calculations.settlement_date = curtailment_records.settlement_date
        ) AS actual_count
      FROM 
        curtailment_records
      WHERE 
        settlement_date >= '2023-12-01' AND settlement_date <= '2023-12-31'
      GROUP BY 
        settlement_date
    )
    SELECT 
      SUM(expected_count) as total_expected,
      SUM(actual_count) as total_actual,
      ROUND((SUM(actual_count) * 100.0) / SUM(expected_count), 2) as overall_percentage
    FROM december_stats
  `);
  
  const expected = Number(result.rows[0].total_expected) || 0;
  const actual = Number(result.rows[0].total_actual) || 0;
  const percentage = Number(result.rows[0].overall_percentage) || 0;
  
  console.log(`December 2023 Status: ${actual}/${expected} (${percentage}%)\n`);
  
  const datesResult = await db.execute(sql`
    WITH december_stats AS (
      SELECT 
        settlement_date,
        COUNT(DISTINCT (settlement_period || '-' || farm_id)) * 3 AS expected_count,
        (
          SELECT COUNT(*) 
          FROM historical_bitcoin_calculations
          WHERE historical_bitcoin_calculations.settlement_date = curtailment_records.settlement_date
        ) AS actual_count
      FROM 
        curtailment_records
      WHERE 
        settlement_date >= '2023-12-01' AND settlement_date <= '2023-12-31'
      GROUP BY 
        settlement_date
    )
    SELECT 
      settlement_date::text as date,
      expected_count,
      actual_count,
      ROUND((actual_count * 100.0) / expected_count, 2) as completion_percentage
    FROM december_stats
    WHERE actual_count < expected_count
    ORDER BY completion_percentage ASC
    LIMIT 10
  `);
  
  if (datesResult.rows.length > 0) {
    console.log("Top 10 dates needing reconciliation:");
    datesResult.rows.forEach((row, index) => {
      console.log(
        `${index+1}. ${row.date}: ${row.actual_count}/${row.expected_count} (${row.completion_percentage}%)`
      );
    });
  }
  
  return { expected, actual, percentage };
}

/**
 * Fix December 2023 dates
 */
async function fixDecember2023() {
  console.log("\n=== Fixing December 2023 Data ===\n");
  
  // Get status first
  await getDecemberStatus();
  
  // Get top dates to fix
  const datesResult = await db.execute(sql`
    WITH december_stats AS (
      SELECT 
        settlement_date,
        COUNT(DISTINCT (settlement_period || '-' || farm_id)) * 3 AS expected_count,
        (
          SELECT COUNT(*) 
          FROM historical_bitcoin_calculations
          WHERE historical_bitcoin_calculations.settlement_date = curtailment_records.settlement_date
        ) AS actual_count
      FROM 
        curtailment_records
      WHERE 
        settlement_date >= '2023-12-01' AND settlement_date <= '2023-12-31'
      GROUP BY 
        settlement_date
    )
    SELECT 
      settlement_date::text as date,
      expected_count,
      actual_count,
      ROUND((actual_count * 100.0) / expected_count, 2) as completion_percentage
    FROM december_stats
    WHERE actual_count < expected_count
    ORDER BY completion_percentage ASC
    LIMIT 5
  `);
  
  if (datesResult.rows.length === 0) {
    console.log("All December dates are fully reconciled!");
    return;
  }
  
  console.log(`\nProcessing ${datesResult.rows.length} dates...\n`);
  
  // Process each date
  for (const row of datesResult.rows) {
    const date = String(row.date);
    console.log(`Processing ${date}...`);
    await fixDate(date);
  }
  
  // Show updated status
  await getDecemberStatus();
}

/**
 * Main function
 */
async function main() {
  const command = process.argv[2]?.toLowerCase();
  const param = process.argv[3];
  
  switch (command) {
    case "status":
      await getReconciliationStatus();
      console.log("\nTo find missing calculations, run:\nnpx tsx simple_reconcile.ts find");
      break;
      
    case "find":
      await getReconciliationStatus();
      await findDatesWithMissingCalculations();
      break;
      
    case "reconcile":
      await fixAllMissingDates();
      break;
      
    case "date":
      if (!param) {
        console.error("Error: Date parameter required");
        console.log("Usage: npx tsx simple_reconcile.ts date YYYY-MM-DD");
        process.exit(1);
      }
      await fixDate(param);
      break;
      
    case "december":
      await fixDecember2023();
      break;
      
    default:
      // Default behavior - just show status
      console.log("Simplified Reconciliation Tool\n");
      console.log("Commands:");
      console.log("  status     - Show reconciliation status");
      console.log("  find       - Find dates with missing calculations");
      console.log("  reconcile  - Fix all missing calculations");
      console.log("  date YYYY-MM-DD - Fix a specific date");
      console.log("  december   - Fix December 2023 data");
      console.log("\nExample: npx tsx simple_reconcile.ts status");
      
      await getReconciliationStatus();
      await findDatesWithMissingCalculations();
  }
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .then(() => {
      console.log("\n=== Reconciliation Complete ===");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
}

export { 
  getReconciliationStatus, 
  findDatesWithMissingCalculations, 
  fixDate,
  fixAllMissingDates,
  getDecemberStatus,
  fixDecember2023
};
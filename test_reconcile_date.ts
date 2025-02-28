/**
 * Test script to reconcile a specific date with missing calculations
 */

import { db } from "./db";
import { sql } from "drizzle-orm";
import { fixDate } from "./reconciliation";

// Set the date to test here
const TEST_DATE = "2023-12-25";

async function getReconciliationStatusForDate(date: string) {
  const result = await db.execute(sql`
    WITH date_curtailment AS (
      SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT (settlement_period || '-' || farm_id)) as unique_combinations
      FROM curtailment_records
      WHERE settlement_date = ${date}
    ),
    date_bitcoin AS (
      SELECT
        miner_model,
        COUNT(*) as calculation_count
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${date}
      GROUP BY miner_model
    )
    SELECT 
      c.total_records as curtailment_records,
      c.unique_combinations as unique_combinations,
      COALESCE(b.miner_model, 'TOTAL') as miner_model,
      COALESCE(b.calculation_count, SUM(b.calculation_count) OVER ()) as calculation_count,
      c.unique_combinations * 1 as expected_per_model,
      c.unique_combinations * 3 as expected_total,
      ROUND((COALESCE(b.calculation_count, SUM(b.calculation_count) OVER ()) * 100.0) / 
        NULLIF(c.unique_combinations * 3, 0), 2) as reconciliation_percentage
    FROM date_curtailment c
    LEFT JOIN date_bitcoin b ON 1=1
    GROUP BY c.total_records, c.unique_combinations, b.miner_model, b.calculation_count
  `);
  
  console.log(`=== Reconciliation Status for ${date} ===\n`);
  
  if (result.rows.length === 0) {
    console.log(`No data found for ${date}.`);
    return null;
  }

  const totalRow = result.rows.find(row => row.miner_model === 'TOTAL') || result.rows[0];
  
  const status = {
    curtailmentRecords: Number(totalRow.curtailment_records),
    uniqueCombinations: Number(totalRow.unique_combinations),
    totalCalculations: Number(totalRow.calculation_count || 0),
    expectedPerModel: Number(totalRow.expected_per_model || 0),
    expectedTotal: Number(totalRow.expected_total || 0),
    reconciliationPercentage: Number(totalRow.reconciliation_percentage || 0)
  };
  
  console.log(`Curtailment Records: ${status.curtailmentRecords}`);
  console.log(`Unique Period-Farm Combinations: ${status.uniqueCombinations}`);
  console.log(`Bitcoin Calculations: ${status.totalCalculations}`);
  console.log(`Expected Per Model: ${status.expectedPerModel}`);
  console.log(`Expected Total: ${status.expectedTotal}`);
  console.log(`Reconciliation: ${status.reconciliationPercentage}%\n`);
  
  // Print by miner model
  result.rows.forEach(row => {
    if (row.miner_model !== 'TOTAL') {
      console.log(`- ${row.miner_model}: ${row.calculation_count || 0}/${row.expected_per_model} calculations`);
    }
  });
  
  // Detailed status - missing combinations
  if (status.reconciliationPercentage < 100) {
    await getMissingCombinations(date);
  }
  
  return status;
}

// Define type for missing combinations row
type MissingCombinationRow = Record<string, unknown> & {
  settlement_period: number;
  farm_id: string;
  s19j_pro_calculations: number;
  s9_calculations: number;
  m20s_calculations: number;
  status: string;
}

async function getMissingCombinations(date: string) {
  const result = await db.execute(sql`
    WITH period_farm_combinations AS (
      SELECT 
        settlement_period,
        farm_id,
        COUNT(*) as record_count
      FROM curtailment_records
      WHERE settlement_date = ${date}
      GROUP BY settlement_period, farm_id
    ),
    bitcoin_calculations AS (
      SELECT 
        settlement_period,
        farm_id,
        miner_model,
        COUNT(*) as calculation_count
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${date}
      GROUP BY settlement_period, farm_id, miner_model
    )
    SELECT
      pf.settlement_period,
      pf.farm_id,
      COALESCE(bc_s19.calculation_count, 0) as s19j_pro_calculations,
      COALESCE(bc_s9.calculation_count, 0) as s9_calculations,
      COALESCE(bc_m20s.calculation_count, 0) as m20s_calculations,
      CASE 
        WHEN COALESCE(bc_s19.calculation_count, 0) = 0 OR 
             COALESCE(bc_s9.calculation_count, 0) = 0 OR 
             COALESCE(bc_m20s.calculation_count, 0) = 0 THEN 'Incomplete'
        ELSE 'Complete'
      END as status
    FROM period_farm_combinations pf
    LEFT JOIN bitcoin_calculations bc_s19 
      ON pf.settlement_period = bc_s19.settlement_period 
      AND pf.farm_id = bc_s19.farm_id
      AND bc_s19.miner_model = 'S19J_PRO'
    LEFT JOIN bitcoin_calculations bc_s9
      ON pf.settlement_period = bc_s9.settlement_period 
      AND pf.farm_id = bc_s9.farm_id
      AND bc_s9.miner_model = 'S9'
    LEFT JOIN bitcoin_calculations bc_m20s
      ON pf.settlement_period = bc_m20s.settlement_period 
      AND pf.farm_id = bc_m20s.farm_id
      AND bc_m20s.miner_model = 'M20S'
    WHERE 
      COALESCE(bc_s19.calculation_count, 0) = 0 OR 
      COALESCE(bc_s9.calculation_count, 0) = 0 OR 
      COALESCE(bc_m20s.calculation_count, 0) = 0
    ORDER BY pf.settlement_period, pf.farm_id
  `);
  
  if (result.rows.length === 0) {
    console.log("No missing combinations found!");
    return [];
  }
  
  console.log(`\n=== Missing Period-Farm Combinations for ${date} ===\n`);
  console.log("Period | Farm ID | S19J_PRO | S9 | M20S | Status");
  console.log("-------|---------|----------|----|----|--------");
  
  result.rows.forEach(row => {
    const period = String(row.settlement_period || '');
    const farmId = String(row.farm_id || '');
    const s19jPro = String(row.s19j_pro_calculations || 0);
    const s9 = String(row.s9_calculations || 0);
    const m20s = String(row.m20s_calculations || 0);
    const status = String(row.status || 'Unknown');
    
    console.log(
      `${period.padStart(6)} | ` +
      `${farmId.padEnd(7)} | ` +
      `${s19jPro.padStart(8)} | ` +
      `${s9.padStart(2)} | ` +
      `${m20s.padStart(3)} | ` +
      `${status}`
    );
  });
  
  return result.rows;
}

async function testReconcileDate() {
  try {
    console.log(`=== Testing Reconciliation for ${TEST_DATE} ===\n`);
    
    // Get initial status
    console.log("Initial Status:");
    const initialStatus = await getReconciliationStatusForDate(TEST_DATE);
    
    if (!initialStatus) {
      console.log(`No data available for ${TEST_DATE}. Exiting.`);
      return;
    }
    
    // If already at 100%, we're done
    if (initialStatus.reconciliationPercentage === 100) {
      console.log(`\n✅ ${TEST_DATE} is already at 100% reconciliation! No action needed.`);
      return;
    }
    
    // Fix the date
    console.log(`\nAttempting to fix ${TEST_DATE}...`);
    const result = await fixDate(TEST_DATE);
    console.log(`\nFix result: ${result.success ? 'Success' : 'Failed'}`);
    console.log(`Message: ${result.message}`);
    
    // Check final status
    console.log("\nFinal Status:");
    await getReconciliationStatusForDate(TEST_DATE);
    
  } catch (error) {
    console.error("Error during reconciliation process:", error);
    throw error;
  }
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  testReconcileDate()
    .then(() => {
      console.log("\n=== Test Complete ===");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
}
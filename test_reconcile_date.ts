/**
 * Test script to reconcile a specific date with missing calculations
 */

import { db } from "./db";
import { sql } from "drizzle-orm";
import { reconcileDay } from "./server/services/historicalReconciliation";

async function getReconciliationStatusForDate(date: string) {
  console.log(`\n=== Checking Reconciliation Status for ${date} ===\n`);
  
  try {
    const result = await db.execute(sql`
      WITH date_curtailment AS (
        SELECT 
          COUNT(*) AS total_records,
          COUNT(DISTINCT (settlement_period, farm_id)) AS unique_combinations
        FROM curtailment_records
        WHERE settlement_date = ${date}
      ),
      date_calculations AS (
        SELECT 
          COUNT(*) AS total_calculations,
          COUNT(DISTINCT (settlement_period, farm_id, miner_model)) / 3 AS unique_combinations
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${date}
      )
      SELECT 
        dc.total_records AS curtailment_records,
        dc.unique_combinations AS unique_combinations,
        COALESCE(calcs.total_calculations, 0) AS bitcoin_calculations,
        dc.unique_combinations * 3 AS expected_calculations
      FROM date_curtailment dc
      LEFT JOIN date_calculations calcs ON 1=1
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

      console.log("=== Status ===");
      console.log(`Curtailment Records: ${curtailment_records}`);
      console.log(`Unique Period-Farm Combinations: ${unique_combinations}`);
      console.log(`Bitcoin Calculations: ${bitcoin_calculations}`);
      console.log(`Expected Calculations: ${expected_calculations}`);
      console.log(`Missing Calculations: ${missingCalculations}`);
      console.log(`Reconciliation: ${reconciliationPercentage}%\n`);

      const minerStats = await db.execute(sql`
        SELECT miner_model, COUNT(*) as count
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${date}
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
        reconciliationPercentage,
        isMissingCalculations: missingCalculations > 0
      };
    }
  } catch (error) {
    console.error(`Error getting status for ${date}:`, error);
  }

  return {
    curtailmentRecords: 0,
    uniqueCombinations: 0,
    bitcoinCalculations: 0,
    expectedCalculations: 0,
    reconciliationPercentage: 0,
    isMissingCalculations: true
  };
}

type MissingCombinationRow = Record<string, unknown> & {
  settlement_period: number;
  farm_id: string;
  s19j_pro_calculations: number;
  s9_calculations: number;
  m20s_calculations: number;
  status: string;
}

async function getMissingCombinations(date: string) {
  console.log(`\n=== Finding Missing Combinations for ${date} ===\n`);
  
  try {
    const result = await db.execute(sql`
      WITH curtail_combos AS (
        SELECT 
          settlement_period, 
          farm_id
        FROM curtailment_records
        WHERE settlement_date = ${date}
        GROUP BY settlement_period, farm_id
      ),
      s19j_calcs AS (
        SELECT 
          settlement_period, 
          farm_id, 
          COUNT(*) as calc_count
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${date} AND miner_model = 'S19J_PRO'
        GROUP BY settlement_period, farm_id
      ),
      s9_calcs AS (
        SELECT 
          settlement_period, 
          farm_id, 
          COUNT(*) as calc_count
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${date} AND miner_model = 'S9'
        GROUP BY settlement_period, farm_id
      ),
      m20s_calcs AS (
        SELECT 
          settlement_period, 
          farm_id, 
          COUNT(*) as calc_count
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${date} AND miner_model = 'M20S'
        GROUP BY settlement_period, farm_id
      )
      SELECT 
        cc.settlement_period,
        cc.farm_id,
        COALESCE(s19.calc_count, 0) as s19j_pro_calculations,
        COALESCE(s9.calc_count, 0) as s9_calculations,
        COALESCE(m20s.calc_count, 0) as m20s_calculations,
        CASE
          WHEN s19.calc_count IS NULL OR s9.calc_count IS NULL OR m20s.calc_count IS NULL THEN 'Missing'
          ELSE 'Complete'
        END as status
      FROM curtail_combos cc
      LEFT JOIN s19j_calcs s19 ON cc.settlement_period = s19.settlement_period AND cc.farm_id = s19.farm_id
      LEFT JOIN s9_calcs s9 ON cc.settlement_period = s9.settlement_period AND cc.farm_id = s9.farm_id
      LEFT JOIN m20s_calcs m20s ON cc.settlement_period = m20s.settlement_period AND cc.farm_id = m20s.farm_id
      WHERE s19.calc_count IS NULL OR s9.calc_count IS NULL OR m20s.calc_count IS NULL
      ORDER BY cc.settlement_period, cc.farm_id
    `);
    
    if (result.rows.length > 0) {
      console.log(`Found ${result.rows.length} combinations with missing calculations:`);
      result.rows.forEach((row: MissingCombinationRow) => {
        console.log(`- Period ${row.settlement_period}, Farm ${row.farm_id}:`);
        console.log(`  S19J_PRO: ${row.s19j_pro_calculations}, S9: ${row.s9_calculations}, M20S: ${row.m20s_calculations}`);
      });
      
      return result.rows as MissingCombinationRow[];
    } else {
      console.log("No missing combinations found.");
      return [];
    }
  } catch (error) {
    console.error(`Error finding missing combinations for ${date}:`, error);
    return [];
  }
}

async function testReconcileDate() {
  try {
    // Use a date from 2023-12 that we know has missing calculations
    const testDate = '2023-12-16';
    
    // Check initial status
    console.log(`Testing reconciliation for date: ${testDate}`);
    const initialStatus = await getReconciliationStatusForDate(testDate);
    console.log(`Initial reconciliation status: ${initialStatus.reconciliationPercentage}%`);
    
    if (!initialStatus.isMissingCalculations) {
      console.log("This date is already fully reconciled. Please select a different date.");
      return;
    }
    
    // Check what combinations are missing
    const missingCombos = await getMissingCombinations(testDate);
    console.log(`Found ${missingCombos.length} missing combinations before reconciliation.`);
    
    // Perform reconciliation
    console.log(`\n=== Reconciling ${testDate} ===\n`);
    await reconcileDay(testDate);
    
    // Check status after reconciliation
    const finalStatus = await getReconciliationStatusForDate(testDate);
    console.log(`Final reconciliation status: ${finalStatus.reconciliationPercentage}%`);
    
    // Check if any combinations are still missing
    const remainingMissingCombos = await getMissingCombinations(testDate);
    console.log(`Found ${remainingMissingCombos.length} missing combinations after reconciliation.`);
    
    console.log("\n=== Test Summary ===");
    console.log(`Date: ${testDate}`);
    console.log(`Before: ${initialStatus.reconciliationPercentage}% (${initialStatus.bitcoinCalculations}/${initialStatus.expectedCalculations})`);
    console.log(`After: ${finalStatus.reconciliationPercentage}% (${finalStatus.bitcoinCalculations}/${finalStatus.expectedCalculations})`);
    console.log(`Missing combinations before: ${missingCombos.length}`);
    console.log(`Missing combinations after: ${remainingMissingCombos.length}`);
    
    if (finalStatus.reconciliationPercentage === 100) {
      console.log("\n✅ TEST PASSED: Reconciliation successfully completed!");
    } else {
      console.log("\n❌ TEST FAILED: Some calculations are still missing after reconciliation.");
    }
  } catch (error) {
    console.error("Error during test:", error);
  }
}

// Run when executed directly
testReconcileDate().catch(console.error);
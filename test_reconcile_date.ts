/**
 * Test script to reconcile a specific date with missing calculations
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { sql } from "drizzle-orm";
import { auditAndFixBitcoinCalculations } from "./server/services/historicalReconciliation";

// Target date with missing calculations (based on SQL query)
const TARGET_DATE = "2025-02-28";

async function getReconciliationStatusForDate(date: string) {
  // First, analyze the unique combinations of period-farm pairs
  const uniqueCombosResult = await db.execute(sql`
    WITH unique_combos AS (
      SELECT DISTINCT settlement_period, farm_id
      FROM curtailment_records
      WHERE settlement_date = ${date}::date
    )
    SELECT COUNT(*) as unique_combo_count
    FROM unique_combos
  `);
  
  const uniqueCombinations = Number(uniqueCombosResult.rows[0]?.unique_combo_count || 0);
  
  // Get curtailment count for the date
  const curtailmentResult = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(curtailmentRecords)
    .where(sql`settlement_date = ${date}::date`);
  
  const curtailmentCount = curtailmentResult[0]?.count || 0;
  
  // Get Bitcoin calculation counts for the date by model
  const bitcoinResult = await db
    .select({
      model: historicalBitcoinCalculations.minerModel,
      count: sql<number>`COUNT(*)`
    })
    .from(historicalBitcoinCalculations)
    .where(sql`settlement_date = ${date}::date`)
    .groupBy(historicalBitcoinCalculations.minerModel);
  
  const modelCounts = bitcoinResult.reduce((acc, { model, count }) => {
    acc[model as string] = Number(count);
    return acc;
  }, {} as Record<string, number>);
  
  const totalCalculations = bitcoinResult.reduce((sum, { count }) => sum + Number(count), 0);
  
  // The expected calculations should be based on unique period-farm combinations
  // Each unique combination needs one calculation per miner model
  const MINER_MODELS = 3; // S19J_PRO, S9, M20S
  const expectedCalculations = uniqueCombinations * MINER_MODELS;
  
  // Also get the calculation distribution
  const calculationDistributionResult = await db.execute(sql`
    SELECT 
      hbc.settlement_period,
      hbc.farm_id,
      COUNT(DISTINCT hbc.miner_model) as model_count
    FROM historical_bitcoin_calculations hbc
    WHERE hbc.settlement_date = ${date}::date
    GROUP BY hbc.settlement_period, hbc.farm_id
  `);
  
  const calculationDistribution = calculationDistributionResult.rows.map(row => ({
    period: Number(row.settlement_period),
    farmId: row.farm_id,
    modelCount: Number(row.model_count)
  }));
  
  const reconciliationPercentage = expectedCalculations > 0 
    ? (totalCalculations / expectedCalculations) * 100 
    : 100;
    
  return {
    date,
    curtailmentCount,
    uniquePeriodFarmCombinations: uniqueCombinations,
    modelCounts,
    totalCalculations,
    expectedCalculations,
    calculationDistribution,
    reconciliationPercentage: Math.round(reconciliationPercentage * 100) / 100
  };
}

async function testReconcileDate() {
  try {
    console.log(`\n===== TESTING RECONCILIATION FOR ${TARGET_DATE} =====\n`);
    
    // Get initial status
    console.log("Checking initial reconciliation status...");
    const initialStatus = await getReconciliationStatusForDate(TARGET_DATE);
    
    console.log("\n=== Initial Status ===");
    console.log(`Date: ${initialStatus.date}`);
    console.log(`Curtailment Records: ${initialStatus.curtailmentCount}`);
    console.log(`Bitcoin Calculations: ${initialStatus.totalCalculations}`);
    console.log(`Expected Calculations: ${initialStatus.expectedCalculations}`);
    console.log(`Reconciliation: ${initialStatus.reconciliationPercentage}%`);
    
    console.log("\nBy Miner Model:");
    for (const [model, count] of Object.entries(initialStatus.modelCounts)) {
      console.log(`- ${model}: ${count}`);
    }
    
    // If already at 100%, no need to reconcile
    if (initialStatus.reconciliationPercentage === 100) {
      console.log("\n✅ Already at 100% reconciliation! No action needed.");
      return;
    }
    
    // Reconcile the date
    console.log("\nReconciling date...");
    const result = await auditAndFixBitcoinCalculations(TARGET_DATE);
    
    console.log(`\nReconciliation result: ${result.success ? "Success" : "Failure"}`);
    console.log(`Message: ${result.message}`);
    console.log(`Fixed: ${result.fixed}`);
    
    // Get final status
    console.log("\nChecking final reconciliation status...");
    const finalStatus = await getReconciliationStatusForDate(TARGET_DATE);
    
    console.log("\n=== Final Status ===");
    console.log(`Date: ${finalStatus.date}`);
    console.log(`Curtailment Records: ${finalStatus.curtailmentCount}`);
    console.log(`Bitcoin Calculations: ${finalStatus.totalCalculations}`);
    console.log(`Expected Calculations: ${finalStatus.expectedCalculations}`);
    console.log(`Reconciliation: ${finalStatus.reconciliationPercentage}%`);
    
    console.log("\nBy Miner Model:");
    for (const [model, count] of Object.entries(finalStatus.modelCounts)) {
      console.log(`- ${model}: ${count}`);
    }
    
    // Report improvement
    const improvement = finalStatus.reconciliationPercentage - initialStatus.reconciliationPercentage;
    console.log(`\nImprovement: ${improvement.toFixed(2)}%`);
    
    if (finalStatus.reconciliationPercentage === 100) {
      console.log("\n✅ Successfully achieved 100% reconciliation!");
    } else {
      console.log(`\n⚠️ Reconciliation incomplete at ${finalStatus.reconciliationPercentage}%`);
      console.log("Check logs for details on any errors.");
    }
    
  } catch (error) {
    console.error("Error during test:", error);
  }
}

// Run the test
testReconcileDate().catch(console.error);
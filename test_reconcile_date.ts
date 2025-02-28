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
  const expectedCalculations = curtailmentCount * 3; // 3 miner models
  const reconciliationPercentage = expectedCalculations > 0 
    ? (totalCalculations / expectedCalculations) * 100 
    : 100;
    
  return {
    date,
    curtailmentCount,
    modelCounts,
    totalCalculations,
    expectedCalculations,
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
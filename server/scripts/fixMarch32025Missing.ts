/**
 * Script to fix missing curtailment records for March 3, 2025
 * 
 * This script specifically addresses missing data around hour 15:00 (settlement period 30)
 * by re-ingesting the data from the Elexon API.
 */

import { processDailyCurtailment } from "../services/curtailment";
import { reprocessDay } from "../services/historicalReconciliation";
import { processSingleDay } from "../services/bitcoinService";
import { minerModels } from "../types/bitcoin";
import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { eq, and, count } from "drizzle-orm";

const TARGET_DATE = "2025-03-03";
const TARGET_PERIOD = 30; // Hour 15:00

/**
 * Main function to process missing records for March 3, 2025
 */
async function fixMissingRecords() {
  try {
    console.log(`=== Fixing missing curtailment records for ${TARGET_DATE} ===`);
    
    // Check initial state
    console.log("Checking initial state of data...");
    const initialState = await getRecordCounts(TARGET_DATE, TARGET_PERIOD);
    console.log(`Initial state: Period ${TARGET_PERIOD} has ${initialState.length > 0 ? initialState[0].count : 0} records`);
    
    // Process the entire day's curtailment data
    console.log(`Processing curtailment data for ${TARGET_DATE}...`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Check state after reprocessing
    console.log("Checking state after reprocessing...");
    const afterState = await getRecordCounts(TARGET_DATE, TARGET_PERIOD);
    console.log(`After reprocessing: Period ${TARGET_PERIOD} has ${afterState.length > 0 ? afterState[0].count : 0} records`);
    
    // Update Bitcoin calculations for all miner models
    console.log("Updating Bitcoin calculations...");
    for (const minerModel of Object.keys(minerModels)) {
      console.log(`Processing calculations for model ${minerModel}...`);
      await processSingleDay(TARGET_DATE, minerModel);
    }
    
    // Run reconciliation to ensure data consistency
    console.log("Running reconciliation for the day...");
    await reprocessDay(TARGET_DATE);
    
    // Verify database state
    const finalCheck = await verifyDataCompleteness(TARGET_DATE);
    
    if (finalCheck.success) {
      console.log("✅ Fix completed successfully!");
      console.log(`Final state: ${finalCheck.totalRecords} total records across ${finalCheck.periodCount} periods`);
    } else {
      console.log("⚠️ Fix completed, but data may still be incomplete.");
      console.log(`Missing periods: ${finalCheck.missingPeriods.join(', ')}`);
    }
    
  } catch (error) {
    console.error("Error fixing missing records:", error);
    process.exit(1);
  }
}

/**
 * Get record counts for a specific period
 */
async function getRecordCounts(date: string, period: number) {
  return db
    .select({
      count: count()
    })
    .from(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, date),
        eq(curtailmentRecords.settlementPeriod, period)
      )
    );
}

/**
 * Verify data completeness after processing
 */
async function verifyDataCompleteness(date: string) {
  const periodCounts = await db
    .select({
      settlementPeriod: curtailmentRecords.settlementPeriod,
      recordCount: count()
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
  
  const allPeriods = Array.from({length: 48}, (_, i) => i + 1);
  const existingPeriods = periodCounts.map(p => p.settlementPeriod);
  const missingPeriods = allPeriods.filter(p => !existingPeriods.includes(p));
  
  const totalRecords = periodCounts.reduce((sum, p) => sum + Number(p.recordCount), 0);
  
  return {
    success: missingPeriods.length === 0,
    periodCount: periodCounts.length,
    totalRecords,
    missingPeriods
  };
}

// Run the script
fixMissingRecords().then(() => {
  console.log("Script execution completed.");
  process.exit(0);
}).catch(error => {
  console.error("Fatal error:", error);
  process.exit(1);
});
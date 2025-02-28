/**
 * Simplified script to ensure 100% reconciliation between curtailment_records and historical_bitcoin_calculations
 * 
 * This script:
 * 1. Identifies dates with missing or incomplete Bitcoin calculations
 * 2. Processes those dates using the proven historicalReconciliation service
 * 3. Verifies the results to ensure 100% reconciliation
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { sql } from "drizzle-orm";
import { format, parseISO } from "date-fns";
import { auditAndFixBitcoinCalculations } from "./server/services/historicalReconciliation";

// Core miner models used throughout the application
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Get summary statistics about reconciliation status
 */
async function getReconciliationStatus() {
  // Get total curtailment records count
  const curtailmentResult = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(curtailmentRecords);
  
  const totalCurtailmentRecords = curtailmentResult[0]?.count || 0;
  
  // Get Bitcoin calculation counts by model
  const bitcoinCounts: Record<string, number> = {};
  
  for (const model of MINER_MODELS) {
    const result = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(historicalBitcoinCalculations)
      .where(sql`miner_model = ${model}`);
    
    bitcoinCounts[model] = result[0]?.count || 0;
  }
  
  // Expected Bitcoin calculation count for 100% reconciliation
  // For each curtailment record, we should have one calculation per miner model
  const expectedTotal = totalCurtailmentRecords * MINER_MODELS.length;
  const actualTotal = Object.values(bitcoinCounts).reduce((sum, count) => sum + count, 0);
  const reconciliationPercentage = expectedTotal > 0 ? (actualTotal / expectedTotal) * 100 : 100;
  
  return {
    totalCurtailmentRecords,
    bitcoinCalculationsByModel: bitcoinCounts,
    totalBitcoinCalculations: actualTotal,
    expectedBitcoinCalculations: expectedTotal,
    missingCalculations: expectedTotal - actualTotal,
    reconciliationPercentage: Math.round(reconciliationPercentage * 100) / 100
  };
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
    date_calculations AS (
      SELECT 
        c.settlement_date,
        COUNT(DISTINCT b.id) as calculation_count,
        (
          SELECT COUNT(*) 
          FROM curtailment_records cr 
          WHERE cr.settlement_date = c.settlement_date
        ) * ${MINER_MODELS.length} as expected_count
      FROM dates_with_curtailment c
      LEFT JOIN historical_bitcoin_calculations b 
        ON c.settlement_date = b.settlement_date
      GROUP BY c.settlement_date
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
  
  return result.rows.map(row => ({
    date: row.date,
    actual: parseInt(row.calculation_count),
    expected: parseInt(row.expected_count),
    completionPercentage: parseFloat(row.completion_percentage)
  }));
}

/**
 * Process a batch of dates to ensure complete reconciliation
 */
async function processDateBatch(dates: string[]) {
  console.log(`Processing ${dates.length} dates for reconciliation...`);
  
  let successful = 0;
  let failed = 0;
  const errors: Array<{date: string, error: string}> = [];
  
  for (const date of dates) {
    try {
      console.log(`\nProcessing ${date}...`);
      const result = await auditAndFixBitcoinCalculations(date);
      
      if (result.success) {
        if (result.fixed) {
          console.log(`✅ ${date}: Fixed - ${result.message}`);
        } else {
          console.log(`✓ ${date}: Already complete - ${result.message}`);
        }
        successful++;
      } else {
        console.log(`❌ ${date}: Failed - ${result.message}`);
        errors.push({ date, error: result.message });
        failed++;
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.error(`Error processing ${date}:`, errorMessage);
      errors.push({ date, error: errorMessage });
      failed++;
    }
  }
  
  return { successful, failed, errors };
}

/**
 * Main function to run the reconciliation process
 */
async function reconcileBitcoinCalculations() {
  try {
    console.log("=== Starting Bitcoin Calculation Reconciliation ===\n");
    
    // Get initial reconciliation status
    console.log("Checking current reconciliation status...");
    const initialStatus = await getReconciliationStatus();
    
    console.log("\n=== Initial Status ===");
    console.log(`Curtailment Records: ${initialStatus.totalCurtailmentRecords}`);
    console.log(`Bitcoin Calculations: ${initialStatus.totalBitcoinCalculations}`);
    console.log(`Expected Calculations: ${initialStatus.expectedBitcoinCalculations}`);
    console.log(`Missing Calculations: ${initialStatus.missingCalculations}`);
    console.log(`Reconciliation: ${initialStatus.reconciliationPercentage}%`);
    
    console.log("\nBitcoin Calculations by Model:");
    for (const [model, count] of Object.entries(initialStatus.bitcoinCalculationsByModel)) {
      console.log(`- ${model}: ${count}`);
    }
    
    // If we're already at 100%, we're done
    if (initialStatus.reconciliationPercentage === 100) {
      console.log("\n✅ Already at 100% reconciliation! No action needed.");
      return {
        initialStatus,
        finalStatus: initialStatus,
        datesProcessed: 0,
        successful: 0,
        failed: 0
      };
    }
    
    // Find dates with missing calculations
    console.log("\nFinding dates with missing calculations...");
    const missingDates = await findDatesWithMissingCalculations();
    
    if (missingDates.length === 0) {
      console.log("No dates with missing calculations found!");
      return {
        initialStatus,
        finalStatus: initialStatus,
        datesProcessed: 0,
        successful: 0,
        failed: 0
      };
    }
    
    console.log(`\nFound ${missingDates.length} dates with missing calculations:`);
    missingDates.forEach(d => {
      console.log(`- ${d.date}: ${d.actual}/${d.expected} (${d.completionPercentage}%)`);
    });
    
    // Process the dates in batches
    const BATCH_SIZE = 5;
    let totalSuccessful = 0;
    let totalFailed = 0;
    const allErrors: Array<{date: string, error: string}> = [];
    
    const datesToProcess = missingDates.map(d => d.date);
    
    for (let i = 0; i < datesToProcess.length; i += BATCH_SIZE) {
      const batch = datesToProcess.slice(i, i + BATCH_SIZE);
      const batchProgress = Math.round(((i + batch.length) / datesToProcess.length) * 100);
      
      console.log(`\n=== Processing Batch ${Math.floor(i/BATCH_SIZE) + 1} (${batchProgress}% complete) ===`);
      const batchResult = await processDateBatch(batch);
      
      totalSuccessful += batchResult.successful;
      totalFailed += batchResult.failed;
      allErrors.push(...batchResult.errors);
      
      // Sleep between batches to avoid rate limits
      if (i + BATCH_SIZE < datesToProcess.length) {
        console.log(`Waiting before processing next batch...`);
        await new Promise(resolve => setTimeout(resolve, 5000));
      }
    }
    
    // Get final reconciliation status
    console.log("\nChecking final reconciliation status...");
    const finalStatus = await getReconciliationStatus();
    
    console.log("\n=== Final Status ===");
    console.log(`Curtailment Records: ${finalStatus.totalCurtailmentRecords}`);
    console.log(`Bitcoin Calculations: ${finalStatus.totalBitcoinCalculations}`);
    console.log(`Expected Calculations: ${finalStatus.expectedBitcoinCalculations}`);
    console.log(`Missing Calculations: ${finalStatus.missingCalculations}`);
    console.log(`Reconciliation: ${finalStatus.reconciliationPercentage}%`);
    
    console.log("\nBitcoin Calculations by Model:");
    for (const [model, count] of Object.entries(finalStatus.bitcoinCalculationsByModel)) {
      console.log(`- ${model}: ${count}`);
    }
    
    console.log("\n=== Reconciliation Summary ===");
    console.log(`Dates Processed: ${datesToProcess.length}`);
    console.log(`Successful: ${totalSuccessful}`);
    console.log(`Failed: ${totalFailed}`);
    
    if (allErrors.length > 0) {
      console.log("\nErrors:");
      allErrors.forEach(e => {
        console.log(`- ${e.date}: ${e.error}`);
      });
    }
    
    return {
      initialStatus,
      finalStatus,
      datesProcessed: datesToProcess.length,
      successful: totalSuccessful,
      failed: totalFailed,
      errors: allErrors
    };
    
  } catch (error) {
    console.error("Error during reconciliation process:", error);
    throw error;
  }
}

// Run the reconciliation if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  reconcileBitcoinCalculations()
    .then(() => {
      console.log("\n=== Reconciliation Complete ===");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
}

export { reconcileBitcoinCalculations, getReconciliationStatus, findDatesWithMissingCalculations };
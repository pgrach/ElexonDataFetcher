/**
 * Batch Reconciliation Tool
 * 
 * This script processes a specific batch of dates for reconciliation,
 * allowing for incremental progress towards 100% reconciliation.
 */

import { auditAndFixBitcoinCalculations, reconcileDay } from "./server/services/historicalReconciliation";

// Configure the batch to process - change these dates for each batch
const BATCH_DATES = [
  '2023-12-22', // 4671 missing
  '2023-12-21', // 4329 missing
  '2023-12-17'  // 3258 missing
];

async function reconcileBatch() {
  console.log("=== Starting Batch Reconciliation ===\n");
  console.log(`Processing batch of ${BATCH_DATES.length} dates: ${BATCH_DATES.join(', ')}`);
  
  let successful = 0;
  let failed = 0;
  const errors: Array<{date: string, error: string}> = [];
  
  // Process each date one by one
  for (const date of BATCH_DATES) {
    try {
      console.log(`\nProcessing ${date}...`);
      
      // First check for discrepancies in the curtailment data itself and fix if needed
      await reconcileDay(date);
      
      // Then audit and fix the Bitcoin calculations
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
    
    // Print progress after each date
    console.log(`Progress: ${successful + failed}/${BATCH_DATES.length} (${Math.round(((successful + failed) / BATCH_DATES.length) * 100)}%)`);
  }
  
  // Print summary
  console.log("\n=== Batch Reconciliation Summary ===");
  console.log(`Total Batch Dates: ${BATCH_DATES.length}`);
  console.log(`Successful: ${successful}`);
  console.log(`Failed: ${failed}`);
  
  if (errors.length > 0) {
    console.log("\nErrors:");
    errors.forEach(({ date, error }) => {
      console.log(`- ${date}: ${error}`);
    });
  }
  
  return { successful, failed, errors };
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  reconcileBatch()
    .then(() => {
      console.log("\n=== Batch Reconciliation Complete ===");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
}
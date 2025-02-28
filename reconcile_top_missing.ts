/**
 * Focused script to reconcile dates with the most missing Bitcoin calculations
 * 
 * This script targets specific dates with the highest number of missing calculations
 * based on the pre-identified list from our SQL query. It focuses on efficiency by
 * prioritizing dates with the most missing data.
 * 
 * Updated to focus on December 2023, which has been identified as having the largest
 * number of missing calculations.
 */

import { auditAndFixBitcoinCalculations, reconcileDay } from "./server/services/historicalReconciliation";

// List of dates with the most missing Bitcoin calculations (from our SQL query)
const PRIORITY_DATES = [
  '2023-12-16', // 8613 missing
  '2023-12-22', // 4671 missing
  '2023-12-24', // 4659 missing
  '2023-12-21', // 4329 missing
  '2023-12-23', // 3258 missing
  '2023-12-17', // 3258 missing
  '2023-12-20', // 2562 missing
  '2023-12-19', // 1953 missing
  '2023-12-18'  // 453 missing
];

async function reconcileMissingCalculations() {
  console.log("=== Starting Priority Reconciliation ===\n");
  console.log(`Found ${PRIORITY_DATES.length} priority dates to process`);
  
  let successful = 0;
  let failed = 0;
  const errors: Array<{date: string, error: string}> = [];
  
  // Process each date one by one
  for (const date of PRIORITY_DATES) {
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
    console.log(`Progress: ${successful + failed}/${PRIORITY_DATES.length} (${Math.round(((successful + failed) / PRIORITY_DATES.length) * 100)}%)`);
  }
  
  // Print summary
  console.log("\n=== Priority Reconciliation Summary ===");
  console.log(`Total Priority Dates: ${PRIORITY_DATES.length}`);
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
  reconcileMissingCalculations()
    .then(() => {
      console.log("\n=== Priority Reconciliation Complete ===");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
}
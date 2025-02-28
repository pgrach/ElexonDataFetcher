/**
 * Specialized script to reconcile December 2023 data
 * 
 * This script targets the specific dates in December 2023 that have missing Bitcoin calculations.
 */

import { auditAndFixBitcoinCalculations, reconcileDay } from "./server/services/historicalReconciliation";
import { format, parseISO, eachDayOfInterval } from "date-fns";

// Process each day in December 2023
async function reconcileDecember2023() {
  console.log("=== Starting December 2023 Reconciliation ===\n");
  
  // Generate all dates in December 2023
  const start = new Date(2023, 11, 1); // December 1, 2023
  const end = new Date(2023, 11, 31);  // December 31, 2023
  
  const dates = eachDayOfInterval({ start, end }).map(date => format(date, 'yyyy-MM-dd'));
  
  console.log(`Found ${dates.length} days to process in December 2023`);
  
  let successful = 0;
  let failed = 0;
  const errors: Array<{date: string, error: string}> = [];
  
  // Process each date one by one
  for (const date of dates) {
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
    console.log(`Progress: ${successful + failed}/${dates.length} (${Math.round(((successful + failed) / dates.length) * 100)}%)`);
  }
  
  // Print summary
  console.log("\n=== December 2023 Reconciliation Summary ===");
  console.log(`Total Days: ${dates.length}`);
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
  reconcileDecember2023()
    .then(() => {
      console.log("\n=== December 2023 Reconciliation Complete ===");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
}
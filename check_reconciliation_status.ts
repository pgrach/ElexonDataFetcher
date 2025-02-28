/**
 * Simple utility to check the current status of reconciliation
 * between curtailment_records and historical_bitcoin_calculations tables.
 */

import { getReconciliationStatus, findDatesWithMissingCalculations } from "./reconciliation";

async function main() {
  console.log("=== Bitcoin Reconciliation Status Check ===\n");
  
  // Get current reconciliation status
  await getReconciliationStatus();
  
  console.log("\n=== Finding Missing Dates ===\n");
  
  // Identify dates with missing calculations
  const missingDates = await findDatesWithMissingCalculations();
  
  if (missingDates.length === 0) {
    console.log("\n✅ No missing dates found! 100% reconciliation achieved.");
  } else {
    console.log(`\n❌ Found ${missingDates.length} dates with missing calculations.`);
    console.log("\nTo fix, run: npx tsx reconciliation.ts reconcile");
  }
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .then(() => process.exit(0))
    .catch(error => {
      console.error("Error:", error);
      process.exit(1);
    });
}
/**
 * One-time script to reingest data for March 5, 2025
 * 
 * This script will:
 * 1. Reingest curtailment data from Elexon API for 2025-03-05
 * 2. Update all curtailment_records for this date
 * 3. Trigger cascading updates to all dependent tables (including Bitcoin calculations)
 */

import { reconcileDay } from "./server/services/historicalReconciliation";

const TARGET_DATE = "2025-03-05";

async function main() {
  console.log(`\n=== Starting Full Data Reingestion for ${TARGET_DATE} ===\n`);
  
  try {
    console.log("Step 1: Reconciling data (checking for differences)...");
    // The reconcileDay function handles both checking if reprocessing is needed
    // and performing the full update if needed
    await reconcileDay(TARGET_DATE);
    
    console.log(`\n=== Data Reingestion Complete for ${TARGET_DATE} ===\n`);
  } catch (error) {
    console.error("Error during data reingestion:", error);
    process.exit(1);
  }
}

// Run the script
main();
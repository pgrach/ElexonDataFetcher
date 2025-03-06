#!/usr/bin/env tsx
/**
 * Run Data Reprocessing for a Specific Date
 * 
 * This script uses the centralized reconciliation system to reprocess data for a specific date,
 * including curtailment records and Bitcoin calculations.
 * 
 * Usage:
 *   npx tsx run-reprocess.ts 2025-03-04
 */

import { reconcileDay } from "./server/services/historicalReconciliation";
import { isValidDateString } from "./server/utils/dates";
import { performance } from "perf_hooks";

// Parse date from command line
const dateArg = process.argv[2];
if (!dateArg || !isValidDateString(dateArg)) {
  console.error("Please provide a valid date in YYYY-MM-DD format");
  console.error("Example: npx tsx run-reprocess.ts 2025-03-04");
  process.exit(1);
}

async function main() {
  const startTime = performance.now();
  
  console.log(`\n=== Starting Data Reprocessing for ${dateArg} ===\n`);
  
  try {
    // Use the reconciliation service to reprocess the date
    await reconcileDay(dateArg);
    
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(1);
    
    console.log(`\n=== Reprocessing Completed Successfully in ${duration}s ===\n`);
    process.exit(0);
  } catch (error) {
    console.error("\n❌ Error during reprocessing:", error);
    process.exit(1);
  }
}

main();
/**
 * Reprocess data for 2025-03-24
 * 
 * This script uses the built-in reconciliation functionality to 
 * reprocess data for 2025-03-24. This is more compatible with the existing
 * architecture and will properly update all dependent tables.
 */

import { reprocessDay } from "../services/historicalReconciliation";

const TARGET_DATE = '2025-03-24';

async function main() {
  try {
    console.log(`\n=== Starting reprocessing for ${TARGET_DATE} ===`);
    
    const startTime = Date.now();
    await reprocessDay(TARGET_DATE);
    const endTime = Date.now();
    
    console.log(`\n=== Completed reprocessing for ${TARGET_DATE} in ${((endTime - startTime) / 1000).toFixed(2)} seconds ===`);
    
    process.exit(0);
  } catch (error) {
    console.error('Error during reprocessing:', error);
    process.exit(1);
  }
}

main();
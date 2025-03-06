/**
 * Reconcile Historical Bitcoin Calculations for March 5, 2025
 * 
 * This script is specifically designed to fix missing Bitcoin calculations
 * for March 5, 2025 after the curtailment records have been reprocessed.
 */

import { processDate } from './server/services/historicalReconciliation';

const TARGET_DATE = '2025-03-05';

async function main() {
  console.log(`🔄 Starting reconciliation of Bitcoin calculations for ${TARGET_DATE}`);
  
  try {
    const result = await processDate(TARGET_DATE);
    
    console.log("\n===== Reconciliation Result =====");
    console.log(`Success: ${result.success}`);
    console.log(`Message: ${result.message}`);
    
    if (result.success) {
      console.log("✅ SUCCESS: Bitcoin calculations have been successfully reconciled.");
    } else {
      console.log("❌ WARNING: Bitcoin calculation reconciliation encountered issues.");
      console.log("Please review the logs and consider running the script again.");
    }
  } catch (error) {
    console.error("Error during reconciliation:", error);
    console.log("❌ ERROR: Bitcoin calculation reconciliation failed.");
  }
}

// Run the main function
main()
  .then(() => {
    console.log("Reconciliation complete");
    process.exit(0);
  })
  .catch((err) => {
    console.error("Error during reconciliation:", err);
    process.exit(1);
  });
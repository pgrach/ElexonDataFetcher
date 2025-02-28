/**
 * Simple script to run the reconciliation between curtailment_records and historical_bitcoin_calculations.
 * This is a streamlined version of the various reconciliation tools.
 */

import { reconcileBitcoinCalculations } from './reconcile';

async function main() {
  console.log("\n===== BITCOIN CALCULATION RECONCILIATION =====");
  console.log("Starting reconciliation process to ensure 100% completeness");
  console.log("between curtailment_records and historical_bitcoin_calculations tables.\n");
  
  try {
    const startTime = Date.now();
    
    // Run the reconciliation process
    const result = await reconcileBitcoinCalculations();
    
    const duration = (Date.now() - startTime) / 1000;
    
    // Print summary
    console.log(`\n===== RECONCILIATION COMPLETE =====`);
    console.log(`Duration: ${duration.toFixed(1)} seconds`);
    console.log(`Initial Reconciliation: ${result.initialStatus.reconciliationPercentage}%`);
    console.log(`Final Reconciliation: ${result.finalStatus.reconciliationPercentage}%`);
    console.log(`Improvement: ${(result.finalStatus.reconciliationPercentage - result.initialStatus.reconciliationPercentage).toFixed(2)}%`);
    console.log(`Dates Processed: ${result.datesProcessed}`);
    console.log(`Successful: ${result.successful}`);
    console.log(`Failed: ${result.failed}`);
    
    if (result.finalStatus.reconciliationPercentage < 100) {
      console.log("\nNote: Some calculations could not be completed.");
      console.log("Check the logs above for specific errors.");
      console.log("You may need to run the reconciliation again to address these issues.");
    } else {
      console.log("\n✅ Successfully achieved 100% reconciliation!");
    }
    
  } catch (error) {
    console.error("Error running reconciliation:", error);
    process.exit(1);
  }
}

main().catch(console.error);
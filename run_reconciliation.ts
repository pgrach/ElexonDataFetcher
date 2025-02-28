/**
 * Simple script to run the reconciliation between curtailment_records and historical_bitcoin_calculations.
 * This is a streamlined version of the various reconciliation tools.
 */

import { reconcileBitcoinCalculations } from "./reconcile";

async function main() {
  console.log("=== Bitcoin Mining Calculations Reconciliation Tool ===\n");
  console.log("Starting reconciliation process...");
  
  try {
    const result = await reconcileBitcoinCalculations();
    
    if (result.datesProcessed === 0) {
      console.log("\nNo dates needed processing. Reconciliation appears to be complete.");
      console.log(`Current reconciliation: ${result.initialStatus.reconciliationPercentage}%`);
    } else {
      console.log("\n=== Reconciliation Complete ===");
      console.log(`Dates processed: ${result.datesProcessed}`);
      console.log(`Successful: ${result.successful}`);
      console.log(`Failed: ${result.failed}`);
      console.log(`Initial reconciliation: ${result.initialStatus.reconciliationPercentage}%`);
      console.log(`Final reconciliation: ${result.finalStatus.reconciliationPercentage}%`);
      
      const improvement = result.finalStatus.reconciliationPercentage - result.initialStatus.reconciliationPercentage;
      console.log(`Improvement: ${improvement.toFixed(2)}%`);
      
      if (result.finalStatus.reconciliationPercentage === 100) {
        console.log("\n✅ Successfully achieved 100% reconciliation!");
      } else {
        console.log(`\n⚠️ Reconciliation incomplete at ${result.finalStatus.reconciliationPercentage}%`);
        
        if (result.errors && result.errors.length > 0) {
          console.log("\nErrors encountered:");
          result.errors.slice(0, 5).forEach(e => {
            console.log(`- ${e.date}: ${e.error}`);
          });
          
          if (result.errors.length > 5) {
            console.log(`... and ${result.errors.length - 5} more errors`);
          }
          
          console.log("\nRun the tool again to continue reconciliation.");
        }
      }
    }
  } catch (error) {
    console.error("Error during reconciliation:", error);
    process.exit(1);
  }
}

// Run the script
main().catch(console.error);
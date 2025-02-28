/**
 * Simple utility to check the current status of reconciliation
 * between curtailment_records and historical_bitcoin_calculations tables.
 */

import { getReconciliationStatus, findDatesWithMissingCalculations } from "./reconcile";

async function main() {
  console.log("=== Bitcoin Calculations Reconciliation Status ===\n");
  
  try {
    // Get overall status
    console.log("Checking current reconciliation status...");
    const status = await getReconciliationStatus();
    
    console.log("\n=== Overall Status ===");
    console.log(`Curtailment Records: ${status.totalCurtailmentRecords}`);
    console.log(`Unique Period-Farm Combinations: ${status.uniqueDatePeriodFarmCombinations}`);
    console.log(`Bitcoin Calculations: ${status.totalBitcoinCalculations}`);
    console.log(`Expected Calculations: ${status.expectedBitcoinCalculations}`);
    console.log(`Missing Calculations: ${status.missingCalculations}`);
    console.log(`Reconciliation: ${status.reconciliationPercentage}%`);
    
    console.log("\nBitcoin Calculations by Model:");
    for (const [model, count] of Object.entries(status.bitcoinCalculationsByModel)) {
      console.log(`- ${model}: ${count}`);
    }
    
    // If not at 100%, find problematic dates
    if (status.reconciliationPercentage < 100) {
      console.log("\nFinding dates with missing calculations...");
      const missingDates = await findDatesWithMissingCalculations();
      
      if (missingDates.length === 0) {
        console.log("No specific dates with missing calculations found.");
      } else {
        console.log(`\nFound ${missingDates.length} dates with missing calculations:`);
        missingDates.slice(0, 10).forEach(d => {
          console.log(`- ${d.date}: ${d.actual}/${d.expected} (${d.completionPercentage}%)`);
        });
        
        if (missingDates.length > 10) {
          console.log(`... and ${missingDates.length - 10} more dates`);
        }
        
        console.log("\nTo fix missing calculations, run:");
        console.log("npx tsx reconcile.ts");
      }
    } else {
      console.log("\n✅ Reconciliation is at 100%. All calculations are up to date!");
    }
  } catch (error) {
    console.error("Error checking reconciliation status:", error);
    process.exit(1);
  }
}

// Run the script
main().catch(console.error);
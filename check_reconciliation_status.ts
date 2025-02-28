/**
 * Simple utility to check the current status of reconciliation
 * between curtailment_records and historical_bitcoin_calculations tables.
 */

import { getReconciliationStatus, findDatesWithMissingCalculations } from './reconcile';

async function main() {
  console.log("\n===== BITCOIN CALCULATION RECONCILIATION STATUS =====");
  
  try {
    // Get current reconciliation status
    console.log("Checking current reconciliation status...");
    const status = await getReconciliationStatus();
    
    console.log("\n=== Status ===");
    console.log(`Curtailment Records: ${status.totalCurtailmentRecords}`);
    console.log(`Bitcoin Calculations: ${status.totalBitcoinCalculations}`);
    console.log(`Expected Calculations: ${status.expectedBitcoinCalculations}`);
    console.log(`Missing Calculations: ${status.missingCalculations}`);
    console.log(`Reconciliation: ${status.reconciliationPercentage}%`);
    
    console.log("\nBitcoin Calculations by Model:");
    for (const [model, count] of Object.entries(status.bitcoinCalculationsByModel)) {
      console.log(`- ${model}: ${count}`);
    }
    
    // If not at 100%, find dates with missing calculations
    if (status.reconciliationPercentage < 100) {
      console.log("\nFinding dates with missing calculations...");
      const missingDates = await findDatesWithMissingCalculations();
      
      if (missingDates.length === 0) {
        console.log("No dates with missing calculations found!");
      } else {
        console.log(`\nFound ${missingDates.length} dates with missing calculations:`);
        missingDates.forEach(d => {
          console.log(`- ${d.date}: ${d.actual}/${d.expected} (${d.completionPercentage}%)`);
        });
        
        console.log("\nTo fix these issues, run the reconciliation script:");
        console.log("npx tsx run_reconciliation.ts");
      }
    } else {
      console.log("\n✅ At 100% reconciliation! No action needed.");
    }
    
  } catch (error) {
    console.error("Error checking reconciliation status:", error);
    process.exit(1);
  }
}

main().catch(console.error);
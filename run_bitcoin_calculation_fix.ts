/**
 * This script runs the complete Bitcoin calculation reconciliation process.
 * It performs verification, targeted fixes, and final validation.
 */

import { execSync } from "child_process";
import { writeFileSync } from "fs";

/**
 * Main function to run the entire reconciliation process
 */
async function runReconciliationProcess() {
  console.log("=== Starting Bitcoin Calculation Reconciliation Process ===");
  console.log("This script will run the following steps:");
  console.log("1. Verify current calculation status");
  console.log("2. Fix missing and incomplete calculations");
  console.log("3. Verify final calculation status");
  
  try {
    // Step 1: Initial verification
    console.log("\n=== Step 1: Initial Verification ===");
    execSync("tsx verify_bitcoin_calculations.ts", { stdio: "inherit" });
    
    // Step 2: Run targeted fixes
    console.log("\n=== Step 2: Running Targeted Fixes ===");
    execSync("tsx reconcile_missing_calculations.ts", { stdio: "inherit" });
    
    // Step 3: Verify again
    console.log("\n=== Step 3: Final Verification ===");
    execSync("tsx verify_bitcoin_calculations.ts", { stdio: "inherit" });
    
    console.log("\n=== Reconciliation Process Complete ===");
    console.log("All steps have been executed successfully.");
    
    // Create a completion summary
    const summary = {
      completed: new Date().toISOString(),
      status: "Success",
      steps: [
        "Initial verification",
        "Targeted fixes for missing periods",
        "Final verification"
      ]
    };
    
    writeFileSync(
      "bitcoin_reconciliation_summary.json", 
      JSON.stringify(summary, null, 2)
    );
    
    console.log("A summary has been saved to bitcoin_reconciliation_summary.json");
    
  } catch (error) {
    console.error("An error occurred during the reconciliation process:", error);
    process.exit(1);
  }
}

/**
 * Entry point for the script
 */
async function main() {
  try {
    await runReconciliationProcess();
    process.exit(0);
  } catch (error) {
    console.error("Fatal error:", error);
    process.exit(1);
  }
}

// Run the script if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
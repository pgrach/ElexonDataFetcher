/**
 * Update Bitcoin calculations for April 1, 2025
 * 
 * This script focuses only on recalculating the Bitcoin mining potential
 * for April 1, 2025 for all miner models.
 */

// Import bitcoin service
import { processSingleDay } from "../services/bitcoinService";

// Target date for recalculation
const TARGET_DATE = "2025-04-01";

// List of miner models
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

async function updateBitcoinCalculations() {
  try {
    console.log(`\n===== UPDATING BITCOIN CALCULATIONS FOR ${TARGET_DATE} =====\n`);
    
    // Process each miner model
    for (const model of MINER_MODELS) {
      console.log(`Processing ${model}...`);
      await processSingleDay(TARGET_DATE, model);
      console.log(`Completed ${model}\n`);
    }
    
    console.log("All Bitcoin calculations completed successfully!");
    
  } catch (error) {
    console.error("ERROR UPDATING BITCOIN CALCULATIONS:", error);
    process.exit(1);
  }
}

// Run the update
updateBitcoinCalculations()
  .then(() => process.exit(0))
  .catch(error => {
    console.error("UNHANDLED ERROR:", error);
    process.exit(1);
  });
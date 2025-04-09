/**
 * Update Bitcoin Calculations for 2025-03-24
 * 
 * This script updates the historical Bitcoin calculations, monthly summaries,
 * and yearly summaries for March 24, 2025 for all miner models.
 */

import { processSingleDay } from "../services/bitcoinService";

const TARGET_DATE = '2025-03-24';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function main() {
  try {
    console.log(`\n=== Starting Bitcoin Calculations Update for ${TARGET_DATE} ===\n`);
    
    const startTime = Date.now();
    
    // Loop through each miner model and process
    for (const minerModel of MINER_MODELS) {
      try {
        console.log(`Processing Bitcoin calculations for ${minerModel}...`);
        
        // This will update:
        // 1. historical_bitcoin_calculations - for the specific date
        // 2. bitcoin_monthly_summaries - for March 2025
        // 3. bitcoin_yearly_summaries - for 2025
        await processSingleDay(TARGET_DATE, minerModel);
        
        console.log(`✓ Completed Bitcoin calculations for ${minerModel}\n`);
      } catch (error) {
        console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
        // Continue with other models even if one fails
      }
    }
    
    const endTime = Date.now();
    
    console.log(`\n=== Bitcoin Calculations Update Completed ===`);
    console.log(`Duration: ${((endTime - startTime) / 1000).toFixed(2)} seconds`);
    console.log(`All calculations for ${TARGET_DATE} have been updated\n`);
    
    process.exit(0);
  } catch (error) {
    console.error('Error during Bitcoin calculations update:', error);
    process.exit(1);
  }
}

main();
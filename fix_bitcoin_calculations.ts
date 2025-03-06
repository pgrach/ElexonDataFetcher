/**
 * Fix Bitcoin Calculations for 2025-03-05
 * 
 * This script is a simplified version that uses the processSingleDay function
 * directly from the bitcoinService to regenerate calculations for all miner models.
 * 
 * Usage:
 *   npx tsx fix_bitcoin_calculations.ts
 */

import { processSingleDay } from './server/services/bitcoinService';

// Define the target date to fix
const TARGET_DATE = '2025-03-05';

// Define the miner models to process
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function fixBitcoinCalculations() {
  console.log(`\n=== Fixing Bitcoin Calculations for ${TARGET_DATE} ===\n`);
  
  try {
    // Process each miner model
    for (const model of MINER_MODELS) {
      console.log(`Processing model: ${model}`);
      await processSingleDay(TARGET_DATE, model);
      console.log(`✅ Completed processing for ${model}`);
    }
    
    console.log(`\n=== Bitcoin Calculations Successfully Fixed for ${TARGET_DATE} ===`);
    console.log(`All miner models processed: ${MINER_MODELS.join(', ')}`);
  } catch (error) {
    console.error(`Error fixing Bitcoin calculations for ${TARGET_DATE}:`, error);
  }
}

// Execute the fix
fixBitcoinCalculations()
  .then(() => console.log('Processing complete!'))
  .catch(error => console.error('Unexpected error:', error));
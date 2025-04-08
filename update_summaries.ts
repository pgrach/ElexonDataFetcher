/**
 * Update Bitcoin Summaries
 * 
 * This script updates the monthly and yearly Bitcoin summaries
 * to ensure they reflect the latest data.
 */

import { calculateMonthlyBitcoinSummary, manualUpdateYearlyBitcoinSummary } from './server/services/bitcoinService';

const YEAR_MONTH = '2025-03';
const YEAR = '2025';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function updateSummaries() {
  try {
    console.log(`\n===== Updating Summaries =====\n`);
    
    // Step 1: Update monthly Bitcoin summary
    console.log(`Updating monthly Bitcoin summaries for ${YEAR_MONTH}...`);
    for (const minerModel of MINER_MODELS) {
      console.log(`- Processing ${minerModel}`);
      await calculateMonthlyBitcoinSummary(YEAR_MONTH, minerModel);
    }
    
    // Step 2: Update yearly Bitcoin summary
    console.log(`\nUpdating yearly Bitcoin summaries for ${YEAR}...`);
    await manualUpdateYearlyBitcoinSummary(YEAR);
    
    console.log(`\n===== Summary Update Complete =====\n`);
  } catch (error) {
    console.error(`Error updating summaries:`, error);
    process.exit(1);
  }
}

// Run the update
updateSummaries();
/**
 * Update S9 Model Summaries
 */

import { calculateMonthlyBitcoinSummary } from './server/services/bitcoinService';

const YEAR_MONTH = '2025-03';
const MINER_MODEL = 'S9';

async function updateS9Summary() {
  try {
    console.log(`\n===== Updating Summary for ${MINER_MODEL} =====\n`);
    
    console.log(`Updating monthly Bitcoin summary for ${YEAR_MONTH} with ${MINER_MODEL}...`);
    await calculateMonthlyBitcoinSummary(YEAR_MONTH, MINER_MODEL);
    
    console.log(`\n===== Summary Update Complete =====\n`);
  } catch (error) {
    console.error(`Error updating summaries:`, error);
    process.exit(1);
  }
}

// Run the update
updateS9Summary();
/**
 * Update Yearly Summary
 */

import { manualUpdateYearlyBitcoinSummary } from './server/services/bitcoinService';

const YEAR = '2025';

async function updateYearlySummary() {
  try {
    console.log(`\n===== Updating Yearly Summary for ${YEAR} =====\n`);
    
    console.log(`Updating yearly Bitcoin summaries...`);
    await manualUpdateYearlyBitcoinSummary(YEAR);
    
    console.log(`\n===== Yearly Summary Update Complete =====\n`);
  } catch (error) {
    console.error(`Error updating yearly summary:`, error);
    process.exit(1);
  }
}

// Run the update
updateYearlySummary();
/**
 * Run Bitcoin Calculations Update
 * 
 * Simple helper script to update Bitcoin calculations for 2025-04-01
 */

import { updateBitcoinCalculations } from './update_2025_04_01_complete';

// Run the Bitcoin calculations update
console.log('Starting Bitcoin calculations update for 2025-04-01...');
updateBitcoinCalculations()
  .then(() => {
    console.log('Bitcoin calculations update completed successfully.');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Bitcoin calculations update failed:', error);
    process.exit(1);
  });
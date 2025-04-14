/**
 * Reprocess Today's Bitcoin Calculations
 * 
 * This script manually triggers a reprocessing of Bitcoin calculations
 * for the current day to ensure all calculations are up to date.
 */

import { format } from 'date-fns';
import { processSingleDay } from './server/services/bitcoinService';

// Constants
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S']; // Standard miner models
const TODAY = format(new Date(), 'yyyy-MM-dd'); // Today's date in YYYY-MM-DD format

async function reprocessTodaysCalculations() {
  console.log(`=== Starting Bitcoin Calculation Reprocessing for ${TODAY} ===\n`);

  try {
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing calculations for ${minerModel}...`);
      
      try {
        await processSingleDay(TODAY, minerModel);
        console.log(`✓ Successfully processed ${minerModel}`);
      } catch (error) {
        console.error(`Error processing ${minerModel}:`, error);
      }
    }

    console.log(`\n=== Bitcoin Calculation Reprocessing Complete ===`);
  } catch (error) {
    console.error('Error during reprocessing:', error);
    process.exit(1);
  }
}

// Run the reprocessing
reprocessTodaysCalculations().catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});
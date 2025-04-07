/**
 * Update Yearly Bitcoin Summaries
 * 
 * This script updates the yearly Bitcoin summaries for all miner models
 * for a specific year.
 */

import { manualUpdateYearlyBitcoinSummary } from './server/services/bitcoinService';

async function main() {
  try {
    // Get the year from command-line arguments
    const year = process.argv[2];
    
    if (!year || !year.match(/^\d{4}$/)) {
      console.error('Please provide a valid year in YYYY format:');
      console.error('npx tsx process_yearly.ts 2025');
      process.exit(1);
    }
    
    console.log(`\n=== Updating Yearly Bitcoin Summaries for ${year} ===\n`);
    
    // Update yearly summaries for all miner models
    await manualUpdateYearlyBitcoinSummary(year);
    
    console.log(`\n=== Yearly Bitcoin Summaries Updated for ${year} ===\n`);
    
    console.log(`Data processing pipeline is now complete!`);
  } catch (error) {
    console.error('Error updating yearly Bitcoin summaries:', error);
    process.exit(1);
  }
}

main();
/**
 * Process Bitcoin Calculations for a Specific Date and Miner Model
 * 
 * This script processes Bitcoin mining calculations for a specific date
 * and miner model using the curtailment data.
 */

import { processSingleDay, calculateMonthlyBitcoinSummary, manualUpdateYearlyBitcoinSummary } from './server/services/bitcoinService';

async function main() {
  try {
    // Get command-line arguments
    const date = process.argv[2];
    const minerModel = process.argv[3];
    
    if (!date || !minerModel) {
      console.error('Please provide both a date and miner model:');
      console.error('npx tsx process_bitcoin.ts 2025-03-25 S19J_PRO');
      process.exit(1);
    }
    
    console.log(`\n=== Processing Bitcoin Calculations for ${date} with ${minerModel} ===\n`);
    
    // Process the Bitcoin calculations for the day
    await processSingleDay(date, minerModel);
    
    console.log(`\n=== Bitcoin Calculations Complete for ${date} with ${minerModel} ===\n`);
    
    console.log(`Next steps:`);
    console.log(`1. Process other miner models if needed`);
    console.log(`2. Update monthly summary:`);
    console.log(`   npx tsx process_monthly.ts ${date.substring(0, 7)}`);
  } catch (error) {
    console.error('Error processing Bitcoin calculations:', error);
    process.exit(1);
  }
}

main();
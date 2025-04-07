/**
 * Update Monthly Bitcoin Summaries
 * 
 * This script updates the monthly Bitcoin summaries for all miner models
 * for a specific month (format: YYYY-MM).
 */

import { calculateMonthlyBitcoinSummary } from './server/services/bitcoinService';

async function main() {
  try {
    // Get the year-month from command-line arguments
    const yearMonth = process.argv[2];
    
    if (!yearMonth || !yearMonth.match(/^\d{4}-\d{2}$/)) {
      console.error('Please provide a valid year-month in YYYY-MM format:');
      console.error('npx tsx process_monthly.ts 2025-03');
      process.exit(1);
    }
    
    console.log(`\n=== Updating Monthly Bitcoin Summaries for ${yearMonth} ===\n`);
    
    // Process all miner models
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const model of minerModels) {
      console.log(`\nProcessing ${model}...`);
      await calculateMonthlyBitcoinSummary(yearMonth, model);
    }
    
    console.log(`\n=== Monthly Bitcoin Summaries Updated for ${yearMonth} ===\n`);
    
    console.log(`Next steps:`);
    console.log(`1. Update yearly summary:`);
    console.log(`   npx tsx process_yearly.ts ${yearMonth.substring(0, 4)}`);
  } catch (error) {
    console.error('Error updating monthly Bitcoin summaries:', error);
    process.exit(1);
  }
}

main();
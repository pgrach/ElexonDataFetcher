/**
 * Fix Incomplete Data
 * 
 * This script automates the complete process of fixing incomplete data for a specific date:
 * 1. Process curtailment records
 * 2. Process Bitcoin calculations for all miner models
 * 3. Update monthly summaries
 * 4. Update yearly summaries
 */

import { processDailyCurtailment } from './server/services/curtailment';
import { processSingleDay, calculateMonthlyBitcoinSummary, manualUpdateYearlyBitcoinSummary } from './server/services/bitcoinService';

async function fixDate(date: string): Promise<void> {
  try {
    console.log(`\n=== Starting Data Fix for ${date} ===\n`);
    
    // Step 1: Process curtailment records
    console.log(`\n--- Step 1: Processing Curtailment Records ---\n`);
    await processDailyCurtailment(date);
    
    // Step 2: Process Bitcoin calculations for all miner models
    console.log(`\n--- Step 2: Processing Bitcoin Calculations ---\n`);
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const model of minerModels) {
      console.log(`\nProcessing ${model}...`);
      await processSingleDay(date, model);
    }
    
    // Step 3: Update monthly summaries
    console.log(`\n--- Step 3: Updating Monthly Summaries ---\n`);
    const yearMonth = date.substring(0, 7);
    
    for (const model of minerModels) {
      console.log(`\nUpdating monthly summary for ${model}...`);
      await calculateMonthlyBitcoinSummary(yearMonth, model);
    }
    
    // Step 4: Update yearly summaries
    console.log(`\n--- Step 4: Updating Yearly Summaries ---\n`);
    const year = date.substring(0, 4);
    await manualUpdateYearlyBitcoinSummary(year);
    
    console.log(`\n=== Data Fix Complete for ${date} ===\n`);
    
    // Verify the fix
    console.log(`\n=== Verifying Fix Results ===\n`);
    console.log(`To verify the fix, run: npx tsx check_elexon_data.ts ${date}`);
    
  } catch (error) {
    console.error('Error fixing data:', error);
    throw error;
  }
}

async function main() {
  try {
    // Get the date from command-line arguments
    const date = process.argv[2];
    
    if (!date || !date.match(/^\d{4}-\d{2}-\d{2}$/)) {
      console.error('Please provide a valid date in YYYY-MM-DD format:');
      console.error('npx tsx fix_incomplete_data.ts 2025-03-25');
      process.exit(1);
    }
    
    await fixDate(date);
  } catch (error) {
    console.error('Error in fix_incomplete_data:', error);
    process.exit(1);
  }
}

main();
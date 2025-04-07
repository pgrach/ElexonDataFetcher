/**
 * Fix Incomplete Data (Optimized Version)
 * 
 * This script automates the complete process of fixing incomplete data for a specific date
 * using the optimized processors that handle all 48 periods and fetch DynamoDB data only once.
 * 
 * Key improvements:
 * 1. Processes all 48 periods in batches to avoid API rate limits
 * 2. Fetches difficulty data only once per date for all calculations
 * 3. Handles all miner models in a single pass
 * 4. Updates all summary tables in a cascade
 */

import { processAllPeriods } from './process_all_periods';
import { processFullCascade } from './process_bitcoin_optimized';
import { format } from 'date-fns';

async function fixDate(date: string): Promise<void> {
  try {
    console.log(`\n=== Starting Data Fix for ${date} ===\n`);
    
    // Step 1: Process curtailment data for all 48 periods
    console.log('\n--- Step 1: Processing Curtailment Records ---\n');
    const curtailmentResult = await processAllPeriods(date);
    
    if (curtailmentResult.totalRecords === 0) {
      console.log(`No curtailment data found for ${date}, stopping process`);
      return;
    }
    
    // Step 2: Process Bitcoin calculations for all miner models
    console.log('\n--- Step 2: Processing Bitcoin Calculations ---\n');
    await processFullCascade(date);
    
    console.log(`\n=== Data Fix Complete for ${date} ===\n`);
    console.log('Summary:');
    console.log(`- Date: ${date}`);
    console.log(`- Curtailment Records: ${curtailmentResult.totalRecords}`);
    console.log(`- Periods Processed: ${curtailmentResult.totalPeriods}/48`);
    console.log(`- Total Volume: ${curtailmentResult.totalVolume.toFixed(2)} MWh`);
    console.log(`- Total Payment: £${curtailmentResult.totalPayment.toFixed(2)}`);
    
    console.log('\nTo verify the fix, run:');
    console.log(`npx tsx check_elexon_data.ts ${date}`);
  } catch (error) {
    console.error('Error fixing data:', error);
    throw error;
  }
}

async function main() {
  try {
    // Get the date from command-line arguments or use default
    const dateToFix = process.argv[2] || format(new Date(), 'yyyy-MM-dd');
    
    await fixDate(dateToFix);
  } catch (error) {
    console.error('Error in fix_incomplete_data_optimized:', error);
    process.exit(1);
  }
}

main();
/**
 * Process Complete Data Cascade
 * 
 * This script runs the complete data processing cascade for a date:
 * 1. Processes all 48 settlement periods for curtailment data
 * 2. Calculates Bitcoin mining potential for all miner models
 * 3. Updates all monthly and yearly summaries
 * 
 * Key optimizations:
 * - Fetches DynamoDB difficulty data only once
 * - Uses batch processing for API calls
 * - Adds proper retry logic and error handling
 */

import { processAllPeriods } from './process_all_periods';
import { processFullCascade } from './process_bitcoin_optimized';
import { format } from 'date-fns';

async function main() {
  try {
    // Get the date from command-line arguments or use default
    const dateToProcess = process.argv[2] || format(new Date(), 'yyyy-MM-dd');
    
    console.log(`\n=== Starting Complete Data Cascade for ${dateToProcess} ===\n`);
    
    // Step 1: Process all curtailment periods
    console.log('Step 1: Processing curtailment data...');
    const curtailmentResult = await processAllPeriods(dateToProcess);
    
    if (curtailmentResult.totalRecords === 0) {
      console.log(`No curtailment data found for ${dateToProcess}, stopping process`);
      return;
    }
    
    // Step 2: Process Bitcoin calculations and summaries
    console.log('\nStep 2: Processing Bitcoin calculations and summaries...');
    await processFullCascade(dateToProcess);
    
    console.log(`\n=== Complete Data Cascade Finished for ${dateToProcess} ===\n`);
    console.log('Summary:');
    console.log(`- Date: ${dateToProcess}`);
    console.log(`- Curtailment Records: ${curtailmentResult.totalRecords}`);
    console.log(`- Periods Processed: ${curtailmentResult.totalPeriods}/48`);
    console.log(`- Total Volume: ${curtailmentResult.totalVolume.toFixed(2)} MWh`);
    console.log(`- Total Payment: £${curtailmentResult.totalPayment.toFixed(2)}`);
    
    console.log('\nAll data processing complete!');
  } catch (error) {
    console.error('Error in complete data cascade:', error);
    process.exit(1);
  }
}

main();
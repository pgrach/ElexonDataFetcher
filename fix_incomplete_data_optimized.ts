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

import { format } from 'date-fns';
import { processAllPeriods } from './fix_bmu_mapping';
import { processFullCascade } from './process_bitcoin_optimized';

async function fixDate(date: string): Promise<void> {
  console.log(`\n=== Starting Complete Data Fix for ${date} ===\n`);
  
  try {
    // Step 1: Process all periods for curtailment data
    console.log(`\n==== Step 1: Processing Curtailment Records ====\n`);
    const curtailmentResult = await processAllPeriods(date);
    
    if (curtailmentResult.totalRecords === 0) {
      console.log(`\nNo curtailment records found for ${date}, skipping Bitcoin calculations`);
      return;
    }
    
    console.log(`\nProcessed ${curtailmentResult.totalRecords} curtailment records across ${curtailmentResult.totalPeriods} periods`);
    console.log(`Total Energy: ${curtailmentResult.totalVolume.toFixed(2)} MWh`);
    console.log(`Total Payment: £${curtailmentResult.totalPayment.toFixed(2)}`);
    
    // Step 2: Process Bitcoin calculations and cascade updates
    console.log(`\n==== Step 2: Processing Bitcoin Calculations and Summaries ====\n`);
    await processFullCascade(date);
    
    console.log(`\n=== Complete Data Fix Successful for ${date} ===\n`);
  } catch (error) {
    console.error(`\nError fixing data for ${date}:`, error);
    console.error(`\nSuggested recovery steps:`);
    console.error(`1. Try processing curtailment records only: npx tsx fix_bmu_mapping_minimal.ts ${date}`);
    console.error(`2. Check the database for existing records: SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = '${date}'`);
    console.error(`3. See DATA_VERIFICATION.md for more detailed troubleshooting steps`);
  }
}

async function main() {
  try {
    // Get the date from command-line arguments or use default
    const dateToProcess = process.argv[2] || format(new Date(), 'yyyy-MM-dd');
    
    await fixDate(dateToProcess);
  } catch (error) {
    console.error('Unexpected error:', error);
    process.exit(1);
  }
}

main();
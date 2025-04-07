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

import { format } from 'date-fns';
import { processAllPeriods } from './fix_bmu_mapping';
import { processFullCascade } from './process_bitcoin_optimized';
import { runMigration } from './run_wind_data_migration';

async function main() {
  try {
    // Get the date from command-line arguments or use default
    const dateToProcess = process.argv[2] || format(new Date(), 'yyyy-MM-dd');
    
    console.log(`\n=== Starting Complete Data Cascade for ${dateToProcess} ===\n`);
    
    // Step 0: Ensure the wind_generation_data table exists
    console.log(`\n==== Step 0: Ensuring Wind Generation Data Table Exists ====\n`);
    await runMigration();
    
    // Step 1: Process all 48 periods using the correct BMU mapping
    console.log(`\n==== Step 1: Processing All 48 Settlement Periods ====\n`);
    const curtailmentResult = await processAllPeriods(dateToProcess);
    
    if (curtailmentResult.totalRecords === 0) {
      console.log(`\nNo curtailment records found for ${dateToProcess}. This is normal if there was no wind curtailment on this date.`);
      console.log(`Check Elexon API manually if you believe this is an error.`);
      return;
    }
    
    console.log(`\nProcessed ${curtailmentResult.totalRecords} curtailment records across ${curtailmentResult.totalPeriods} periods`);
    console.log(`Total Energy: ${curtailmentResult.totalVolume.toFixed(2)} MWh`);
    console.log(`Total Payment: £${curtailmentResult.totalPayment.toFixed(2)}`);
    
    // Step 2: Process Bitcoin calculations and all summaries
    console.log(`\n==== Step 2: Processing Bitcoin Calculations and Summaries ====\n`);
    await processFullCascade(dateToProcess);
    
    console.log(`\n=== Complete Data Cascade Process Finished for ${dateToProcess} ===\n`);
    console.log(`To verify the data, run the following SQL commands:`);
    console.log(`1. SELECT * FROM curtailment_records WHERE settlement_date = '${dateToProcess}' LIMIT 10;`);
    console.log(`2. SELECT * FROM daily_summaries WHERE summary_date = '${dateToProcess}';`);
    console.log(`3. SELECT * FROM historical_bitcoin_calculations WHERE calculation_date = '${dateToProcess}' LIMIT 10;`);
    console.log(`\nSee DATA_VERIFICATION.md for more verification commands.`);
  } catch (error) {
    console.error('Error processing complete cascade:', error);
    console.error(`\nRecommended recovery steps:`);
    console.error(`1. Try processing only a few periods to diagnose API issues: npx tsx fix_bmu_mapping_minimal.ts ${process.argv[2] || format(new Date(), 'yyyy-MM-dd')}`);
    console.error(`2. See DATA_VERIFICATION.md for detailed troubleshooting steps`);
    process.exit(1);
  }
}

main();
/**
 * Process 3 Key Periods for March 28, 2025
 * 
 * This script specifically targets 3 key periods that have been successfully
 * processed before: periods 11, 25, and 37.
 */

import { processDate } from './optimized_critical_date_processor';
import * as updateSummariesModule from './update_summaries';
const updateSummaries = updateSummariesModule.updateSummaries;
const updateBitcoinCalculations = updateSummariesModule.updateBitcoinCalculations;

const TARGET_DATE = '2025-03-28';
const KEY_PERIODS = [11, 25, 37];

/**
 * Process the key periods and update all related summary tables
 */
async function processKeyPeriods(): Promise<void> {
  console.log(`\n=== Processing Key Periods for ${TARGET_DATE} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  console.log(`Targeting 3 periods: ${KEY_PERIODS.join(', ')}`);

  // Process each key period one at a time
  for (const period of KEY_PERIODS) {
    console.log(`\nProcessing period ${period}...`);
    
    try {
      await processDate(TARGET_DATE, period, period);
      console.log(`Completed processing period ${period}`);
    } catch (error) {
      console.error(`Error processing period ${period}:`, error);
    }
  }

  // Update all the summary tables
  console.log('\nUpdating summary tables...');
  try {
    await updateSummaries(TARGET_DATE);
    console.log('Summary tables updated successfully');
  } catch (error) {
    console.error('Error updating summary tables:', error);
  }

  // Update Bitcoin calculations
  console.log('\nUpdating Bitcoin calculations...');
  try {
    await updateBitcoinCalculations(TARGET_DATE);
    console.log('Bitcoin calculations updated successfully');
    
    // Update the bitcoin_daily_summaries table
    console.log('\nUpdating Bitcoin daily summary...');
    const dailySummaryModule = await import('./update_bitcoin_daily_summary');
    await dailySummaryModule.updateBitcoinDailySummary(TARGET_DATE);
    console.log('Bitcoin daily summary updated successfully');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
  }

  console.log(`\n=== Processing completed at ${new Date().toISOString()} ===`);
}

// Execute the main function
processKeyPeriods().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
/**
 * Update All Summaries Script
 * 
 * This script updates all summaries and Bitcoin calculations for March 28, 2025
 * after individual periods have been processed.
 */

import * as updateSummariesModule from './update_summaries';
const updateSummaries = updateSummariesModule.updateSummaries;
const updateBitcoinCalculations = updateSummariesModule.updateBitcoinCalculations;
import { updateBitcoinDailySummary } from './update_bitcoin_daily_summary';

const TARGET_DATE = '2025-03-28';

/**
 * Update all summary tables and Bitcoin calculations
 */
async function updateAllSummaries(): Promise<void> {
  console.log(`\n=== Updating All Summaries for ${TARGET_DATE} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);

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
    await updateBitcoinDailySummary(TARGET_DATE);
    console.log('Bitcoin daily summary updated successfully');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
  }

  console.log(`\n=== Summary updates completed at ${new Date().toISOString()} ===`);
}

// Execute the main function
updateAllSummaries().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
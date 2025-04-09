/**
 * Complete Data Reingestion and Update Script
 * 
 * This script performs:
 * 1. Full reingestion of curtailment records from Elexon API
 * 2. Updates all dependent tables (daily, monthly, yearly summaries)
 * 3. Recalculates Bitcoin mining potential data
 * 4. Updates wind generation data from Elexon API
 * 
 * Usage:
 *   npm run tsx reingest_all_data.ts [startDate] [endDate]
 * 
 * Example:
 *   npm run tsx reingest_all_data.ts 2025-01-01 2025-03-31
 */

import { reingestAllCurtailmentData } from './server/scripts/reingest_all_curtailment_data';
import { updateWindGenerationForDates } from './server/scripts/update_wind_generation_for_dates';
import { isValidDateString } from './server/utils/dates';

async function main() {
  try {
    console.log('======================================================');
    console.log('  STARTING COMPLETE DATA REINGESTION AND UPDATE');
    console.log('======================================================\n');
    
    // Extract command line arguments for date range
    const args = process.argv.slice(2);
    const startDate = args[0]; // Format: YYYY-MM-DD
    const endDate = args[1];   // Format: YYYY-MM-DD
    
    // Validate dates
    if (startDate && !isValidDateString(startDate)) {
      console.error(`Invalid start date format: ${startDate}. Use YYYY-MM-DD format.`);
      process.exit(1);
    }
    
    if (endDate && !isValidDateString(endDate)) {
      console.error(`Invalid end date format: ${endDate}. Use YYYY-MM-DD format.`);
      process.exit(1);
    }
    
    // Display date range
    if (startDate && endDate) {
      console.log(`Processing data for date range: ${startDate} to ${endDate}`);
    } else if (startDate) {
      console.log(`Processing data from ${startDate} onwards`);
    } else if (endDate) {
      console.log(`Processing data until ${endDate}`);
    } else {
      console.log('Processing all available data');
    }
    
    const startTime = Date.now();
    
    // Step 1: Reingest all curtailment records
    console.log('\n\n======================================================');
    console.log('  STEP 1: REINGESTING CURTAILMENT RECORDS');
    console.log('======================================================\n');
    
    await reingestAllCurtailmentData(startDate, endDate);
    
    // Step 2: Update wind generation data
    console.log('\n\n======================================================');
    console.log('  STEP 2: UPDATING WIND GENERATION DATA');
    console.log('======================================================\n');
    
    if (startDate && endDate) {
      await updateWindGenerationForDates(startDate, endDate);
    } else {
      console.log('Wind generation data update requires both start and end dates');
      console.log('Skipping wind generation data update');
    }
    
    const endTime = Date.now();
    const executionMinutes = (endTime - startTime) / (1000 * 60);
    
    console.log('\n\n======================================================');
    console.log('  COMPLETE DATA REINGESTION AND UPDATE FINISHED');
    console.log(`  Total execution time: ${executionMinutes.toFixed(1)} minutes`);
    console.log('======================================================\n');
    
  } catch (error) {
    console.error('Error during data reingestion and update process:', error);
    process.exit(1);
  }
}

// Execute main function
main()
  .then(() => {
    console.log('Process completed successfully');
    process.exit(0);
  })
  .catch(error => {
    console.error('Unhandled error during execution:', error);
    process.exit(1);
  });
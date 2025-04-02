/**
 * Process March 28 Key Periods
 * 
 * This script uses the optimized critical date processor to target specific
 * key periods throughout the day for March 28, 2025, giving us a good
 * representative sample across all 24 hours with minimal processing time.
 * 
 * By populating key periods (one morning, one afternoon, one evening period
 * per hour), we can provide a more complete visualization without waiting for
 * all 48 periods to process.
 */

import { processDate } from './optimized_critical_date_processor';

// Target date is fixed to March 28, 2025
const TARGET_DATE = '2025-03-28';

/**
 * Select key periods throughout the day
 * Very targeted selection to ensure a balanced view for visualization
 */
const KEY_PERIODS = [
  // Morning priority periods (hours 6-10)
  11, 15, 19, 
  
  // Midday priority periods (hours 11-15)
  21, 25, 29,
  
  // Evening priority periods (hours 16-20)
  31, 37, 43
];

/**
 * Process a specific set of periods
 */
async function processKeyPeriods(): Promise<void> {
  console.log(`\n=== Processing Key Periods for ${TARGET_DATE} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  console.log(`Targeting ${KEY_PERIODS.length} periods: ${KEY_PERIODS.join(', ')}`);
  
  // Process a smaller batch to see quick results
  const priorityPeriods = [11, 25, 37]; // One from morning, afternoon, evening
  console.log(`\nProcessing priority periods first: ${priorityPeriods.join(', ')}`);
  
  // Process priority periods first
  for (const period of priorityPeriods) {
    console.log(`\nProcessing priority period ${period}...`);
    
    try {
      // Use the processDate function from the optimized processor
      const result = await processDate(TARGET_DATE, period, period);
      
      if (result.success) {
        console.log(`Period ${period} processed successfully: ${result.recordsAdded} records added.`);
      } else {
        console.error(`Period ${period} processing failed.`);
      }
    } catch (error) {
      console.error(`Error processing period ${period}: ${error}`);
    }
    
    // Add a shorter delay between priority periods
    console.log(`Waiting 3 seconds before next period...`);
    await new Promise(resolve => setTimeout(resolve, 3000));
  }
  
  // Update summaries after priority periods
  console.log(`\nUpdating summaries after priority periods...`);
  try {
    const { updateSummaries, updateBitcoinCalculations } = await import('./unified_reconciliation');
    await updateSummaries(TARGET_DATE);
    await updateBitcoinCalculations(TARGET_DATE);
    console.log(`Summaries updated successfully`);
  } catch (error) {
    console.error(`Error updating summaries: ${error}`);
  }
  
  // Filter out the periods we already processed
  const remainingPeriods = KEY_PERIODS.filter(p => !priorityPeriods.includes(p));
  console.log(`\nProcessing remaining ${remainingPeriods.length} periods...`);
  
  // Process the remaining periods
  for (const period of remainingPeriods) {
    console.log(`\nProcessing period ${period}...`);
    
    try {
      // Use the processDate function from the optimized processor
      const result = await processDate(TARGET_DATE, period, period);
      
      if (result.success) {
        console.log(`Period ${period} processed successfully: ${result.recordsAdded} records added.`);
      } else {
        console.error(`Period ${period} processing failed.`);
      }
    } catch (error) {
      console.error(`Error processing period ${period}: ${error}`);
    }
    
    // Add a delay between periods to avoid rate limiting
    console.log(`Waiting 5 seconds before next period...`);
    await new Promise(resolve => setTimeout(resolve, 5000));
  }
  
  console.log(`\n=== Key Periods Processing Complete ===`);
  console.log(`Ended at: ${new Date().toISOString()}`);
}

// Run the process
processKeyPeriods().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
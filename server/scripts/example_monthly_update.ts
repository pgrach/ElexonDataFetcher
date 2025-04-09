/**
 * Example Monthly Update Script
 * 
 * This script demonstrates how to use the reingestion tools to update a specific month's data.
 * It can be used as a template for creating scheduled maintenance scripts.
 */

import { reingestAllCurtailmentData } from './reingest_all_curtailment_data';
import { updateWindGenerationForDates } from './update_wind_generation_for_dates';
import { format, subMonths } from 'date-fns';

/**
 * Updates data for the previous month (or a specified month)
 * 
 * @param targetYear - Year to update (defaults to previous month's year)
 * @param targetMonth - Month to update (defaults to previous month)
 * @param includePreviousMonth - Whether to include the previous month in the update for continuity (default: false)
 */
async function updateMonthlyData(
  targetYear?: number,
  targetMonth?: number,
  includePreviousMonth: boolean = false
): Promise<void> {
  // Determine target month (default to previous month)
  const today = new Date();
  const previousMonth = subMonths(today, 1);
  
  const year = targetYear || previousMonth.getFullYear();
  const month = targetMonth || previousMonth.getMonth() + 1; // JavaScript months are 0-indexed
  
  // Calculate start and end dates
  let startDate: string;
  let endDate: string;
  
  if (includePreviousMonth) {
    // Start from the beginning of the previous month
    const startMonth = month === 1 ? 12 : month - 1;
    const startYear = month === 1 ? year - 1 : year;
    startDate = format(new Date(startYear, startMonth - 1, 1), 'yyyy-MM-dd');
  } else {
    // Start from the beginning of the target month
    startDate = format(new Date(year, month - 1, 1), 'yyyy-MM-dd');
  }
  
  // End date is the last day of the target month
  const nextMonth = month === 12 ? 1 : month + 1;
  const nextYear = month === 12 ? year + 1 : year;
  const lastDay = new Date(nextYear, nextMonth - 1, 0).getDate();
  endDate = format(new Date(year, month - 1, lastDay), 'yyyy-MM-dd');
  
  console.log(`\n=== Updating data for ${year}-${month.toString().padStart(2, '0')} ===`);
  console.log(`Date range: ${startDate} to ${endDate}`);
  
  try {
    // Step 1: Reingest curtailment records
    console.log('\nStep 1: Reingesting curtailment records...');
    await reingestAllCurtailmentData(startDate, endDate);
    
    // Step 2: Update wind generation data
    console.log('\nStep 2: Updating wind generation data...');
    await updateWindGenerationForDates(startDate, endDate);
    
    console.log(`\n=== Successfully updated data for ${year}-${month.toString().padStart(2, '0')} ===`);
  } catch (error) {
    console.error(`\n=== Error updating data for ${year}-${month.toString().padStart(2, '0')} ===`);
    console.error(error);
    throw error;
  }
}

// Only run the script directly if it's the main module
if (require.main === module) {
  (async () => {
    try {
      console.log('Starting monthly data update...');
      
      // Parse command line arguments
      const args = process.argv.slice(2);
      const targetYear = args[0] ? parseInt(args[0], 10) : undefined;
      const targetMonth = args[1] ? parseInt(args[1], 10) : undefined;
      const includePrevious = args[2] === 'true' || args[2] === '1';
      
      if (targetYear && (isNaN(targetYear) || targetYear < 2020 || targetYear > 2030)) {
        console.error('Invalid year. Please provide a year between 2020 and 2030.');
        process.exit(1);
      }
      
      if (targetMonth && (isNaN(targetMonth) || targetMonth < 1 || targetMonth > 12)) {
        console.error('Invalid month. Please provide a month between 1 and 12.');
        process.exit(1);
      }
      
      // Display options
      if (targetYear && targetMonth) {
        console.log(`Updating data for ${targetYear}-${targetMonth.toString().padStart(2, '0')}`);
      } else {
        console.log('Updating data for the previous month');
      }
      
      if (includePrevious) {
        console.log('Including the previous month in the update for continuity');
      }
      
      // Run the update
      await updateMonthlyData(targetYear, targetMonth, includePrevious);
      
      console.log('Monthly data update completed successfully');
      process.exit(0);
    } catch (error) {
      console.error('Error during monthly data update:', error);
      process.exit(1);
    }
  })();
}
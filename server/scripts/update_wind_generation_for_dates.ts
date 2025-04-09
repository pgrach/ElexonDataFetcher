/**
 * Wind Generation Data Update Script
 * 
 * This script processes wind generation data for a specified date range from Elexon's API.
 * This data is useful for analysis alongside curtailment data.
 */

import { processDateRange } from "../services/windGenerationService";
import { isValidDateString } from "../utils/dates";
import { format, addDays, subDays, parseISO } from 'date-fns';

interface WindDataUpdateResult {
  startDate: string;
  endDate: string;
  datesProcessed: number;
  recordsProcessed: number;
  successfulDates: string[];
  failedDates: string[];
  executionTimeMs: number;
}

/**
 * Update wind generation data for a date range
 * 
 * @param startDate - Start date in YYYY-MM-DD format
 * @param endDate - End date in YYYY-MM-DD format
 */
export async function updateWindGenerationForDates(
  startDate: string,
  endDate: string
): Promise<WindDataUpdateResult> {
  console.log(`Starting wind generation data update for ${startDate} to ${endDate}`);
  
  // Validate dates
  if (!isValidDateString(startDate) || !isValidDateString(endDate)) {
    throw new Error('Invalid date format. Use YYYY-MM-DD format.');
  }
  
  const start = new Date(startDate);
  const end = new Date(endDate);
  
  if (start > end) {
    throw new Error('Start date must be before end date');
  }
  
  const result: WindDataUpdateResult = {
    startDate,
    endDate,
    datesProcessed: 0,
    recordsProcessed: 0,
    successfulDates: [],
    failedDates: [],
    executionTimeMs: 0
  };
  
  const startTime = Date.now();
  
  try {
    // Process date range in chunks
    let currentStart = new Date(start);
    const daysInRange = Math.floor((end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24)) + 1;
    
    console.log(`Processing ${daysInRange} days of wind generation data...`);
    
    // Process in smaller chunks if the range is large
    const CHUNK_SIZE = 7; // 7 days at a time
    
    while (currentStart <= end) {
      const chunkEnd = new Date(Math.min(
        addDays(currentStart, CHUNK_SIZE - 1).getTime(),
        end.getTime()
      ));
      
      try {
        // Format dates for API call
        const formattedStart = format(currentStart, 'yyyy-MM-dd');
        const formattedEnd = format(chunkEnd, 'yyyy-MM-dd');
        
        console.log(`Processing chunk from ${formattedStart} to ${formattedEnd}...`);
        
        // Process this date range
        const recordCount = await processDateRange(formattedStart, formattedEnd);
        
        result.recordsProcessed += recordCount;
        result.datesProcessed += Math.floor((chunkEnd.getTime() - currentStart.getTime()) / (1000 * 60 * 60 * 24)) + 1;
        
        // Add all dates in this chunk to successful dates
        let dateToAdd = new Date(currentStart);
        while (dateToAdd <= chunkEnd) {
          result.successfulDates.push(format(dateToAdd, 'yyyy-MM-dd'));
          dateToAdd = addDays(dateToAdd, 1);
        }
        
        console.log(`Successfully processed wind data from ${formattedStart} to ${formattedEnd} (${recordCount} records)`);
      } catch (error) {
        console.error(`Error processing wind data from ${format(currentStart, 'yyyy-MM-dd')} to ${format(chunkEnd, 'yyyy-MM-dd')}:`, error);
        
        // Add all dates in this chunk to failed dates
        let dateToAdd = new Date(currentStart);
        while (dateToAdd <= chunkEnd) {
          result.failedDates.push(format(dateToAdd, 'yyyy-MM-dd'));
          dateToAdd = addDays(dateToAdd, 1);
        }
      }
      
      // Move to next chunk
      currentStart = addDays(chunkEnd, 1);
    }
    
    console.log(`Wind generation data update completed for ${startDate} to ${endDate}`);
  } catch (error) {
    console.error('Error updating wind generation data:', error);
  }
  
  const endTime = Date.now();
  result.executionTimeMs = endTime - startTime;
  
  console.log(`Wind generation data update completed in ${(result.executionTimeMs / 1000).toFixed(1)} seconds`);
  console.log(`Processed ${result.datesProcessed} dates with ${result.recordsProcessed} total records`);
  console.log(`Successful dates: ${result.successfulDates.length}, Failed dates: ${result.failedDates.length}`);
  
  return result;
}

/**
 * Update wind generation data for recent days
 * 
 * @param days - Number of days to update (default: 7)
 */
export async function updateRecentWindGenerationData(days: number = 7): Promise<WindDataUpdateResult> {
  const today = new Date();
  const startDate = format(subDays(today, days - 1), 'yyyy-MM-dd');
  const endDate = format(today, 'yyyy-MM-dd');
  
  return updateWindGenerationForDates(startDate, endDate);
}

// Only run the script directly if it's the main module
if (require.main === module) {
  (async () => {
    try {
      console.log('Starting wind generation data update script...');
      
      // Extract command line arguments for date range
      const args = process.argv.slice(2);
      
      if (args.length >= 2) {
        // Update with start and end dates
        const startDate = args[0];
        const endDate = args[1];
        console.log(`Using date range: ${startDate} to ${endDate}`);
        await updateWindGenerationForDates(startDate, endDate);
      } else if (args.length === 1) {
        // Update with recent days
        const days = parseInt(args[0], 10);
        if (isNaN(days) || days <= 0) {
          console.error('Invalid number of days. Please provide a positive number.');
          process.exit(1);
        }
        console.log(`Updating last ${days} days of wind generation data`);
        await updateRecentWindGenerationData(days);
      } else {
        // Default: update last 7 days
        console.log('No date range specified, updating last 7 days of wind generation data');
        await updateRecentWindGenerationData(7);
      }
      
      console.log('Wind generation data update completed successfully');
      process.exit(0);
    } catch (error) {
      console.error('Error updating wind generation data:', error);
      process.exit(1);
    }
  })();
}
/**
 * Wind Generation Data Update Script
 * 
 * This enhanced update script handles fetching and processing wind generation data
 * from Elexon with sophisticated error handling, retry mechanisms, and detailed reporting.
 */

import { processRecentDays, processSingleDate } from './server/services/windGenerationService';
import { logger } from './server/utils/logger';
import { formatDate } from './server/utils/dates';
import { subDays, parseISO, isValid } from 'date-fns';
import { fileURLToPath } from 'url';
import path from 'path';

// Constants
const MAX_RETRY_ATTEMPTS = 3;
const RETRY_DELAY_BASE_MS = 1000;

// Type definitions for the update result
interface WindDataUpdateResult {
  status: 'completed' | 'partial_update' | 'failed';
  dates: string[];
  updatedDates: string[];
  skippedDates: string[];
  failedDates: string[];
  startTime: string;
  endTime: string;
}

/**
 * Run wind generation data update for the specified number of days
 * 
 * @param days - Number of recent days to update
 * @param force - Force update even if data exists
 * @returns Update result with detailed status information
 */
export async function runWindDataUpdate(days: number = 2, force: boolean = false): Promise<WindDataUpdateResult> {
  const startTime = new Date();
  const result: WindDataUpdateResult = {
    status: 'completed',
    dates: [],
    updatedDates: [],
    skippedDates: [],
    failedDates: [],
    startTime: startTime.toISOString(),
    endTime: ''
  };

  try {
    logger.info(`Starting enhanced wind data update for ${days} days (force=${force})`, {
      module: 'windDataUpdate'
    });

    // Generate the dates to process
    const today = new Date();
    for (let i = 0; i < days; i++) {
      const date = subDays(today, i);
      result.dates.push(formatDate(date));
    }

    logger.info(`Processing ${result.dates.length} dates: ${result.dates.join(', ')}`, {
      module: 'windDataUpdate'
    });

    // Process each date with retries
    for (const date of result.dates) {
      let success = false;
      let attempts = 0;

      while (!success && attempts < MAX_RETRY_ATTEMPTS) {
        attempts++;
        try {
          logger.info(`Processing date ${date} (attempt ${attempts}/${MAX_RETRY_ATTEMPTS})`, {
            module: 'windDataUpdate'
          });

          // Process the date
          const recordsProcessed = await processSingleDate(date);
          
          success = true;
          result.updatedDates.push(date);
          
          logger.info(`Successfully processed ${recordsProcessed} wind generation records for ${date}`, {
            module: 'windDataUpdate'
          });
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          
          if (attempts >= MAX_RETRY_ATTEMPTS) {
            logger.error(`Failed to process date ${date} after ${MAX_RETRY_ATTEMPTS} attempts: ${errorMessage}`, {
              module: 'windDataUpdate'
            });
            
            result.failedDates.push(date);
          } else {
            logger.warning(`Error processing date ${date}, attempt ${attempts}/${MAX_RETRY_ATTEMPTS}: ${errorMessage}`, {
              module: 'windDataUpdate'
            });
            
            // Exponential backoff
            const delayMs = RETRY_DELAY_BASE_MS * Math.pow(2, attempts - 1);
            logger.info(`Retrying in ${delayMs}ms...`, { module: 'windDataUpdate' });
            await new Promise(resolve => setTimeout(resolve, delayMs));
          }
        }
      }
    }

    // Determine final status
    if (result.failedDates.length === result.dates.length) {
      result.status = 'failed';
    } else if (result.failedDates.length > 0) {
      result.status = 'partial_update';
    } else {
      result.status = 'completed';
    }

    // Set end time
    const endTime = new Date();
    result.endTime = endTime.toISOString();

    // Log summary
    const durationMs = endTime.getTime() - startTime.getTime();
    logger.info(`Wind data update completed with status: ${result.status} in ${durationMs}ms. Updated: ${result.updatedDates.length}, Failed: ${result.failedDates.length}`, {
      module: 'windDataUpdate'
    });

    return result;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Unexpected error in wind data update: ${errorMessage}`, {
      module: 'windDataUpdate'
    });

    const endTime = new Date();
    result.endTime = endTime.toISOString();
    result.status = 'failed';
    
    return result;
  }
}

// Main function if the script is run directly
const checkIfMainModule = () => {
  try {
    const currentUrl = import.meta.url;
    const currentPath = fileURLToPath(currentUrl);
    return process.argv[1] === currentPath;
  } catch (error) {
    return false;
  }
};

// Run the update if this is the main module
if (checkIfMainModule()) {
  // Parse command-line arguments
  const args = process.argv.slice(2);
  const daysArg = args.find(arg => arg.startsWith('--days='));
  const forceArg = args.includes('--force');
  
  // Default to 2 days if not specified
  const days = daysArg ? parseInt(daysArg.split('=')[1], 10) : 2;
  
  // Run the update
  runWindDataUpdate(days, forceArg)
    .then(result => {
      console.log('Update completed with status:', result.status);
      console.log('Updated dates:', result.updatedDates);
      console.log('Failed dates:', result.failedDates);
      process.exit(0);
    })
    .catch(error => {
      console.error('Update failed with error:', error);
      process.exit(1);
    });
}
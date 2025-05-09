/**
 * Wind Generation Data Updater Service
 * 
 * This service regularly fetches and updates wind generation data from Elexon.
 * It runs daily updates and can process historical data on demand.
 * 
 * Features:
 * - Scheduled daily updates at configurable times
 * - Historical data processing on service startup
 * - Robust error handling and retry mechanisms
 * - Integration with system-wide logging
 */

import schedule from 'node-schedule';
import { processRecentDays, processSingleDate, processDateRange, getLatestDataDate } from './windGenerationService';
import { logger } from '../utils/logger';
import { formatDate } from '../utils/dates';
import { subDays, format } from 'date-fns';
import { runWindDataUpdate } from '../scripts/data/updaters/windGenerationDataUpdater';

// Scheduling constants
const DEFAULT_UPDATE_HOUR = 1;  // 1 AM daily
const DEFAULT_UPDATE_MINUTE = 30;
const HISTORICAL_DAYS_TO_PROCESS = 60; // When starting from scratch, process 60 days
const MAX_RETRY_ATTEMPTS = 3;

// Service state
let isRunning = false;
let lastRunTime: Date | null = null;
let lastUpdateStatus: 'success' | 'partial' | 'failed' | null = null;
let updateJob: schedule.Job | null = null;

/**
 * Main update function - fetches and processes latest wind generation data
 * Enhanced with comprehensive error handling and detailed logging
 */
async function updateWindData(days: number = 2, force: boolean = false): Promise<boolean> {
  if (isRunning) {
    logger.warning('Wind data update already in progress, skipping. Last run: ' + 
      (lastRunTime ? lastRunTime.toISOString() : 'never'), { 
      module: 'windDataUpdater'
    });
    return false;
  }
  
  isRunning = true;
  lastRunTime = new Date();
  
  try {
    logger.info(`Starting enhanced wind generation data update for last ${days} days (force=${force})`, { 
      module: 'windDataUpdater'
    });
    
    // Use the more robust wind data update script
    const result = await runWindDataUpdate(days, force);
    
    // Detailed logging of results
    logger.info(`Wind generation data update completed with status: ${result.status}. ` +
      `Total: ${result.dates.length}, Updated: ${result.updatedDates.length}, ` +
      `Skipped: ${result.skippedDates.length}, Failed: ${result.failedDates.length}`, { 
      module: 'windDataUpdater'
    });
    
    // Set status for service health monitoring
    if (result.status === 'completed') {
      lastUpdateStatus = 'success';
    } else if (result.status === 'partial_update') {
      lastUpdateStatus = 'partial';
      
      // Log specific failures for troubleshooting
      if (result.failedDates.length > 0) {
        logger.warning(`Failed to update wind data for dates: ${result.failedDates.join(', ')}`, {
          module: 'windDataUpdater'
        });
      }
    } else {
      lastUpdateStatus = 'failed';
      logger.error(`Wind data update failed completely. Failed dates: ${result.failedDates.join(', ')}`, {
        module: 'windDataUpdater'
      });
    }
    
    return result.status !== 'failed';
  } catch (error) {
    lastUpdateStatus = 'failed';
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Unexpected error in wind generation data update: ${errorMessage}`, { 
      module: 'windDataUpdater'
    });
    return false;
  } finally {
    isRunning = false;
  }
}

/**
 * Process historical wind data to ensure the database is populated
 * Enhanced with retry logic and comprehensive logging
 */
async function processHistoricalData(): Promise<void> {
  try {
    logger.info('Checking for existing wind generation data', { module: 'windDataUpdater' });
    
    const latestDate = await getLatestDataDate();
    if (!latestDate) {
      // No data exists, process the last 60 days
      logger.info(`No existing wind data found, processing last ${HISTORICAL_DAYS_TO_PROCESS} days`, { 
        module: 'windDataUpdater'
      });
      
      const endDate = new Date();
      const startDate = subDays(endDate, HISTORICAL_DAYS_TO_PROCESS);
      
      const formattedStartDate = format(startDate, 'yyyy-MM-dd');
      const formattedEndDate = format(endDate, 'yyyy-MM-dd');
      
      logger.info(`Processing historical data from ${formattedStartDate} to ${formattedEndDate}`, {
        module: 'windDataUpdater'
      });
      
      // Process historical data in chunks to avoid timeout
      const chunkSize = 7; // Process a week at a time
      let currentStart = new Date(formattedStartDate);
      const end = new Date(formattedEndDate);
      
      while (currentStart <= end) {
        const chunkEnd = new Date(currentStart);
        chunkEnd.setDate(chunkEnd.getDate() + chunkSize - 1);
        
        // Cap at the overall end date
        if (chunkEnd > end) {
          chunkEnd.setTime(end.getTime());
        }
        
        const currentStartFormatted = format(currentStart, 'yyyy-MM-dd');
        const chunkEndFormatted = format(chunkEnd, 'yyyy-MM-dd');
        
        logger.info(`Processing chunk from ${currentStartFormatted} to ${chunkEndFormatted}`, {
          module: 'windDataUpdater'
        });
        
        try {
          // Try up to MAX_RETRY_ATTEMPTS times
          let success = false;
          let attempt = 0;
          
          while (!success && attempt < MAX_RETRY_ATTEMPTS) {
            attempt++;
            
            try {
              await processDateRange(currentStartFormatted, chunkEndFormatted);
              success = true;
            } catch (chunkError) {
              if (attempt >= MAX_RETRY_ATTEMPTS) {
                throw chunkError;
              }
              
              // Log and retry with backoff
              const errorMessage = chunkError instanceof Error ? chunkError.message : String(chunkError);
              logger.warning(`Error processing chunk ${currentStartFormatted} to ${chunkEndFormatted}, attempt ${attempt}/${MAX_RETRY_ATTEMPTS}: ${errorMessage}`, {
                module: 'windDataUpdater'
              });
              
              // Exponential backoff
              await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, attempt)));
            }
          }
        } catch (finalError) {
          const errorMessage = finalError instanceof Error ? finalError.message : String(finalError);
          logger.error(`Failed to process chunk ${currentStartFormatted} to ${chunkEndFormatted} after ${MAX_RETRY_ATTEMPTS} attempts: ${errorMessage}`, {
            module: 'windDataUpdater'
          });
        }
        
        // Move to next chunk
        currentStart.setDate(currentStart.getDate() + chunkSize);
      }
    } else {
      // Some data exists, make sure we're up to date
      logger.info(`Wind data found up to ${latestDate}, updating to current date`, { 
        module: 'windDataUpdater'
      });
      
      // Process from the day after latest date to today
      const latestDateObj = new Date(latestDate);
      latestDateObj.setDate(latestDateObj.getDate() + 1);
      const today = new Date();
      
      if (latestDateObj <= today) {
        const formattedStartDate = format(latestDateObj, 'yyyy-MM-dd');
        const formattedEndDate = format(today, 'yyyy-MM-dd');
        
        logger.info(`Processing missing data from ${formattedStartDate} to ${formattedEndDate}`, {
          module: 'windDataUpdater'
        });
        
        // Use the enhanced update function with force=true to ensure all data is updated
        await updateWindData(Math.ceil((today.getTime() - latestDateObj.getTime()) / (1000 * 60 * 60 * 24)) + 1, true);
      } else {
        logger.info('Wind generation data is already up to date', { module: 'windDataUpdater' });
      }
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Error processing historical wind generation data: ${errorMessage}`, { 
      module: 'windDataUpdater'
    });
  }
}

/**
 * Start the wind data update service
 * 
 * @param hour - Hour to run daily update (0-23)
 * @param minute - Minute to run daily update (0-59)
 */
export function startWindDataUpdateService(hour = DEFAULT_UPDATE_HOUR, minute = DEFAULT_UPDATE_MINUTE): void {
  logger.info(`Starting wind generation data update service, scheduled for ${hour}:${minute} daily`, {
    module: 'windDataUpdater'
  });
  
  // Cancel existing job if it exists
  if (updateJob) {
    updateJob.cancel();
  }
  
  // Schedule daily update job
  updateJob = schedule.scheduleJob(`${minute} ${hour} * * *`, async () => {
    logger.info('Running scheduled wind generation data update', { module: 'windDataUpdater' });
    await updateWindData(2); // Process last 2 days to ensure complete data
  });
  
  // Process historical data right away
  processHistoricalData();
}

/**
 * Stop the wind data update service
 */
export function stopWindDataUpdateService(): void {
  if (updateJob) {
    updateJob.cancel();
    updateJob = null;
    logger.info('Wind generation data update service stopped', { module: 'windDataUpdater' });
  }
}

/**
 * Get the current status of the update service with detailed state information
 */
export function getWindDataServiceStatus(): {
  isRunning: boolean;
  lastRunTime: string | null;
  lastUpdateStatus: 'success' | 'partial' | 'failed' | null;
  nextScheduledRun: string | null;
} {
  return {
    isRunning,
    lastRunTime: lastRunTime ? lastRunTime.toISOString() : null,
    lastUpdateStatus,
    nextScheduledRun: updateJob ? updateJob.nextInvocation().toISOString() : null
  };
}

/**
 * Manually trigger an update with optional force update parameter
 * 
 * @param days - Number of days to update (default: 2)
 * @param force - Force update even if data exists (default: false)
 */
export async function manualUpdate(days: number = 2, force: boolean = false): Promise<boolean> {
  return updateWindData(days, force);
}

/**
 * Process wind generation data for a specific date
 * This function is used for synchronizing with the curtailment data pipeline
 * 
 * @param date - Date in YYYY-MM-DD format
 * @param force - Force update even if data exists (default: false)
 * @returns Promise resolving to boolean indicating success
 */
export async function processWindDataForDate(date: string, force: boolean = false): Promise<boolean> {
  if (!isRunning) {
    try {
      logger.info(`Processing wind generation data for specific date: ${date}`, { 
        module: 'windDataUpdater'
      });
      
      // Import required function
      const { processSingleDate } = await import('./windGenerationService');
      
      // Process the specific date
      const recordsProcessed = await processSingleDate(date);
      
      logger.info(`Completed wind generation data processing for ${date}. Records processed: ${recordsProcessed}`, {
        module: 'windDataUpdater'
      });
      
      return recordsProcessed > 0;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      logger.error(`Error processing wind generation data for ${date}: ${errorMessage}`, {
        module: 'windDataUpdater'
      });
      return false;
    }
  } else {
    logger.warning(`Wind data update already in progress, cannot process date ${date}`, {
      module: 'windDataUpdater'
    });
    return false;
  }
}
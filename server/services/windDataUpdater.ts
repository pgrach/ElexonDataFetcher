/**
 * Wind Generation Data Updater Service
 * 
 * This service regularly fetches and updates wind generation data from Elexon.
 * It runs daily updates and can process historical data on demand.
 */

import schedule from 'node-schedule';
import { processRecentDays, processSingleDate, processDateRange, getLatestDataDate } from './windGenerationService';
import { logger } from '../utils/logger';
import { formatDate } from '../utils/dates';
import { subDays, format } from 'date-fns';

// Scheduling constants
const DEFAULT_UPDATE_HOUR = 1;  // 1 AM daily
const DEFAULT_UPDATE_MINUTE = 30;
const HISTORICAL_DAYS_TO_PROCESS = 60; // When starting from scratch, process 60 days

// Service state
let isRunning = false;
let lastRunTime: Date | null = null;
let updateJob: schedule.Job | null = null;

/**
 * Main update function - fetches and processes latest wind generation data
 */
async function updateWindData(days: number = 2): Promise<boolean> {
  if (isRunning) {
    logger.warning('Wind data update already in progress, skipping', { 
      module: 'windDataUpdater',
      lastRunTime: lastRunTime ? lastRunTime.toISOString() : 'never'
    });
    return false;
  }
  
  isRunning = true;
  lastRunTime = new Date();
  
  try {
    logger.info(`Starting wind generation data update for last ${days} days`, { 
      module: 'windDataUpdater'
    });
    
    const recordsProcessed = await processRecentDays(days);
    
    logger.info(`Wind generation data update completed, processed ${recordsProcessed} records`, { 
      module: 'windDataUpdater'
    });
    
    return true;
  } catch (error) {
    logger.error('Error updating wind generation data', { 
      module: 'windDataUpdater',
      error: error instanceof Error ? error.message : String(error)
    });
    return false;
  } finally {
    isRunning = false;
  }
}

/**
 * Process historical wind data to ensure the database is populated
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
      
      await processDateRange(formattedStartDate, formattedEndDate);
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
        
        await processDateRange(formattedStartDate, formattedEndDate);
      } else {
        logger.info('Wind generation data is already up to date', { module: 'windDataUpdater' });
      }
    }
  } catch (error) {
    logger.error('Error processing historical wind generation data', { 
      module: 'windDataUpdater',
      error: error instanceof Error ? error.message : String(error)
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
 * Get the current status of the update service
 */
export function getWindDataServiceStatus(): {
  isRunning: boolean;
  lastRunTime: string | null;
  nextScheduledRun: string | null;
} {
  return {
    isRunning,
    lastRunTime: lastRunTime ? lastRunTime.toISOString() : null,
    nextScheduledRun: updateJob ? updateJob.nextInvocation().toISOString() : null
  };
}

/**
 * Manually trigger an update
 * 
 * @param days - Number of days to update (default: 2)
 */
export async function manualUpdate(days: number = 2): Promise<boolean> {
  return updateWindData(days);
}
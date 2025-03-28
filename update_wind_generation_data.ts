/**
 * Wind Generation Data Update Script
 * 
 * This script provides a simple way to update wind generation data from Elexon's API.
 */

import { processRecentDays } from './server/services/windGenerationService';
import { getRecentDates } from './server/utils/dates';
import { logger } from './server/utils/logger';

/**
 * Simple interface for update result
 */
interface UpdateResult {
  status: 'completed' | 'partial_update' | 'failed';
  dates: string[];
  updatedDates: string[];
  skippedDates: string[];
  failedDates: string[];
  startTime: string;
  endTime: string;
  executionTimeMs: number;
}

/**
 * Process wind generation data updates for recent days
 * 
 * @param days - Number of days to process (default: 2)
 * @param force - Force processing even if data exists (default: false)
 */
export async function runWindDataUpdate(days: number = 2, force: boolean = false): Promise<UpdateResult> {
  // Create result object
  const result: UpdateResult = {
    status: 'completed',
    dates: getRecentDates(days),
    updatedDates: [],
    skippedDates: [],
    failedDates: [],
    startTime: new Date().toISOString(),
    endTime: '',
    executionTimeMs: 0
  };
  
  try {
    logger.info(`Starting wind generation data update for ${days} days`, {
      module: 'windDataUpdate'
    });
    
    // Process recent days
    const recordsProcessed = await processRecentDays(days);
    
    result.updatedDates = [...result.dates];
    
    result.endTime = new Date().toISOString();
    result.executionTimeMs = new Date(result.endTime).getTime() - new Date(result.startTime).getTime();
    
    logger.info(`Wind generation data update completed with ${recordsProcessed} records processed`, {
      module: 'windDataUpdate'
    });
  } catch (error) {
    result.status = 'failed';
    result.failedDates = [...result.dates];
    result.endTime = new Date().toISOString();
    result.executionTimeMs = new Date(result.endTime).getTime() - new Date(result.startTime).getTime();
    
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Wind data update failed: ${errorMessage}`, {
      module: 'windDataUpdate'
    });
  }
  
  return result;
}
/**
 * Wind Generation Data Update Script
 * 
 * This script provides a standardized way to update wind generation data from Elexon's B1630 API.
 * It can be run directly from the command line or called programmatically from the data update service.
 * 
 * Features:
 * - Checks and updates wind generation data for specified days
 * - Robust error handling and retry logic
 * - Checkpoint system for resuming interrupted runs
 * - Comprehensive logging
 * 
 * Usage:
 *   npx tsx update_wind_generation_data.ts [days=2] [force=false]
 * 
 * Options:
 *   days - Number of recent days to update (default: 2)
 *   force - 'true' to force update even if data exists (default: false)
 */

import { format, subDays, parseISO } from 'date-fns';
import fs from 'fs';
import path from 'path';
import { processRecentDays, processSingleDate, hasWindDataForDate } from './server/services/windGenerationService';
import { logger } from './server/utils/logger';

// Configuration
const RECENT_DAYS_TO_UPDATE = parseInt(process.argv[2] || '2', 10);
const FORCE_UPDATE = process.argv[3] === 'true';
const MAX_RETRY_ATTEMPTS = 3;
const CHECKPOINT_FILE = './wind_data_update_checkpoint.json';
const LOG_DIR = './logs';
const LOG_FILE = `${LOG_DIR}/windDataUpdater_${format(new Date(), 'yyyy-MM-dd')}.log`;

// Ensure log directory exists
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

// Set up logging
function log(message: string, level: 'info' | 'error' | 'warning' | 'success' = 'info'): void {
  const timestamp = new Date().toISOString();
  const levelStr = level.toUpperCase();
  const formatted = `[${timestamp}] [${levelStr}] ${message}`;
  
  // Console output with colors
  let consoleMessage = formatted;
  if (level === 'error') {
    consoleMessage = `\x1b[31m${formatted}\x1b[0m`; // Red
  } else if (level === 'warning') {
    consoleMessage = `\x1b[33m${formatted}\x1b[0m`; // Yellow
  } else if (level === 'success') {
    consoleMessage = `\x1b[32m${formatted}\x1b[0m`; // Green
  }
  
  try {
    console.log(consoleMessage);
  } catch (err) {
    // Handle stdout errors (like EPIPE)
  }
  
  // Log to file
  try {
    fs.appendFileSync(LOG_FILE, formatted + '\n');
  } catch (err) {
    // Try to report file write errors to console
    try {
      console.error(`Error writing to log file: ${err}`);
    } catch (_) {
      // Last resort silence
    }
  }

  // Also log to system logger
  try {
    logger.info(message, { module: 'windDataUpdate' });
  } catch (err) {
    // Ignore system logger errors
  }
}

// Sleep utility
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Retry wrapper for operations
async function withRetry<T>(
  operation: () => Promise<T>,
  description: string,
  maxAttempts: number = MAX_RETRY_ATTEMPTS
): Promise<T> {
  let lastError: Error | null = null;
  
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      if (attempt > 1) {
        log(`Retry attempt ${attempt}/${maxAttempts} for: ${description}`, 'info');
        await sleep(1000 * Math.pow(2, attempt - 1)); // Exponential backoff
      }
      
      return await operation();
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
      const errorMsg = lastError.message;
      
      // Log the error
      log(`Error on attempt ${attempt}/${maxAttempts} for ${description}: ${errorMsg}`, 'error');
      
      // Only retry if not the last attempt
      if (attempt < maxAttempts) {
        // Exponential backoff with jitter
        const baseDelay = 1000 * Math.pow(2, attempt - 1);
        const jitter = Math.floor(Math.random() * 1000);
        const delay = baseDelay + jitter;
        
        log(`Waiting ${Math.round(delay/1000)}s before retry...`, 'info');
        await sleep(delay);
      }
    }
  }
  
  throw lastError || new Error(`Failed after ${maxAttempts} attempts: ${description}`);
}

// Checkpoint management
interface Checkpoint {
  lastRun: string;
  dates: string[];
  processedDates: string[];
  lastProcessedDate: string | null;
  status: 'running' | 'completed' | 'failed';
  startTime: string;
  endTime: string | null;
}

function saveCheckpoint(checkpoint: Checkpoint): void {
  try {
    fs.writeFileSync(
      CHECKPOINT_FILE,
      JSON.stringify(checkpoint, null, 2)
    );
  } catch (error) {
    log(`Error saving checkpoint: ${error}`, 'error');
  }
}

function loadCheckpoint(): Checkpoint | null {
  try {
    if (fs.existsSync(CHECKPOINT_FILE)) {
      const data = fs.readFileSync(CHECKPOINT_FILE, 'utf8');
      return JSON.parse(data) as Checkpoint;
    }
  } catch (error) {
    log(`Error loading checkpoint: ${error}`, 'warning');
  }
  return null;
}

// Main update function
export async function runWindDataUpdate(days: number = RECENT_DAYS_TO_UPDATE, force: boolean = FORCE_UPDATE): Promise<{
  dates: string[];
  updatedDates: string[];
  skippedDates: string[];
  failedDates: string[];
  status: 'completed' | 'partial_update' | 'failed';
}> {
  log(`=== Starting Wind Generation Data Update ===`, 'info');
  log(`Date: ${new Date().toISOString()}`, 'info');
  log(`Updating the last ${days} days of wind generation data...`, 'info');
  log(`Force update: ${force}`, 'info');
  
  // Set up checkpoint
  let checkpoint = loadCheckpoint();
  let isResume = false;
  
  // If there's a valid checkpoint that's still running, try to resume
  if (checkpoint && checkpoint.status === 'running' &&
      checkpoint.dates.length > 0 && 
      checkpoint.lastRun === format(new Date(), 'yyyy-MM-dd')) {
    log(`Resuming from previous checkpoint. Last processed date: ${checkpoint.lastProcessedDate || 'none'}`, 'info');
    isResume = true;
  } else {
    // Start fresh
    const today = new Date();
    const dates: string[] = [];
    
    for (let i = 0; i < days; i++) {
      const date = subDays(today, i);
      dates.push(format(date, 'yyyy-MM-dd'));
    }
    
    log(`Dates to update: ${dates.join(', ')}`, 'info');
    
    checkpoint = {
      lastRun: format(new Date(), 'yyyy-MM-dd'),
      dates,
      processedDates: [],
      lastProcessedDate: null,
      status: 'running',
      startTime: new Date().toISOString(),
      endTime: null
    };
    
    saveCheckpoint(checkpoint);
  }
  
  // Track updated, skipped, and failed dates
  const updatedDates: string[] = [];
  const skippedDates: string[] = [];
  const failedDates: string[] = [];
  
  // Process each date
  for (const date of checkpoint.dates) {
    // Skip already processed dates (unless in resume mode)
    if (!isResume && checkpoint.processedDates.includes(date)) {
      log(`Skipping already processed date: ${date}`, 'info');
      skippedDates.push(date);
      continue;
    }
    
    // Reset resume flag after we find the last processed date
    if (isResume && checkpoint.lastProcessedDate === date) {
      isResume = false;
      continue;
    }
    
    // If still in resume mode, skip this date
    if (isResume) {
      log(`Skipping date in resume mode: ${date}`, 'info');
      skippedDates.push(date);
      continue;
    }
    
    log(`Processing date: ${date}`, 'info');
    
    try {
      // Check if data already exists for this date
      const hasData = await withRetry(
        async () => await hasWindDataForDate(date),
        `Check if wind data exists for ${date}`
      );
      
      if (hasData && !force) {
        log(`Wind generation data already exists for ${date}. Skipping...`, 'info');
        skippedDates.push(date);
      } else {
        // Process the date
        const message = hasData ? 'Updating' : 'Fetching new';
        log(`${message} wind generation data for ${date}...`, 'info');
        
        const recordsProcessed = await withRetry(
          async () => await processSingleDate(date),
          `Process wind generation data for ${date}`
        );
        
        log(`Successfully processed ${recordsProcessed} records for ${date}`, 'success');
        updatedDates.push(date);
      }
      
      // Update checkpoint
      checkpoint.processedDates.push(date);
      checkpoint.lastProcessedDate = date;
      saveCheckpoint(checkpoint);
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      log(`Failed to process ${date}: ${errorMessage}`, 'error');
      failedDates.push(date);
      
      // Update checkpoint even on failure
      checkpoint.lastProcessedDate = date;
      saveCheckpoint(checkpoint);
    }
  }
  
  // Determine final status
  let status: 'completed' | 'partial_update' | 'failed';
  if (failedDates.length === 0) {
    status = 'completed';
    log(`✅ Wind generation data update completed successfully`, 'success');
  } else if (updatedDates.length > 0) {
    status = 'partial_update';
    log(`⚠️ Wind generation data updated partially. ${failedDates.length} dates failed.`, 'warning');
  } else {
    status = 'failed';
    log(`❌ Wind generation data update failed for all dates.`, 'error');
  }
  
  // Update and save final checkpoint
  checkpoint.status = status === 'failed' ? 'failed' : 'completed';
  checkpoint.endTime = new Date().toISOString();
  saveCheckpoint(checkpoint);
  
  // Print summary
  log(`\n=== Wind Generation Data Update Summary ===`, 'info');
  log(`Total dates: ${checkpoint.dates.length}`, 'info');
  log(`Updated: ${updatedDates.length}`, 'info');
  log(`Skipped: ${skippedDates.length}`, 'info');
  log(`Failed: ${failedDates.length}`, 'info');
  
  return {
    dates: checkpoint.dates,
    updatedDates,
    skippedDates,
    failedDates,
    status
  };
}

// Run main function if called directly
if (require.main === module) {
  runWindDataUpdate().catch(error => {
    log(`Uncaught error in wind data update: ${error}`, 'error');
    process.exit(1);
  });
}

// Export for programmatic use
export { runWindDataUpdate };
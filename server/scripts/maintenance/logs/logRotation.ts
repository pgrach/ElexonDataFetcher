/**
 * Log Rotation Script
 * 
 * This script manages log files to prevent them from accumulating indefinitely.
 * It archives older logs and maintains a more organized logs directory.
 * 
 * Features:
 * - Archives logs older than a specified number of days
 * - Groups logs by type and date for better organization
 * - Can be run manually or scheduled to run periodically
 */

import fs from 'fs/promises';
import path from 'path';
import { createWriteStream, createReadStream, existsSync, mkdirSync } from 'fs';
import { createGzip } from 'zlib';
import { pipeline } from 'stream/promises';
import { format, subDays } from 'date-fns';
import { logger } from '../../../utils/logger';

// Configuration
const LOG_DIR = path.join(process.cwd(), 'logs');
const ARCHIVE_DIR = path.join(LOG_DIR, 'archives');
const LOG_AGE_DAYS = 30; // Archive logs older than 30 days
const DRY_RUN = false; // Set to true to see what would be archived without actually doing it

/**
 * Ensures the archive directory exists
 */
async function ensureArchiveDirectory(): Promise<void> {
  try {
    if (!existsSync(ARCHIVE_DIR)) {
      mkdirSync(ARCHIVE_DIR, { recursive: true });
      logger.info(`Created archive directory: ${ARCHIVE_DIR}`, { module: 'logRotation' });
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Failed to create archive directory: ${errorMessage}`, { module: 'logRotation' });
    throw error;
  }
}

/**
 * Compresses a log file and moves it to the archive directory
 * 
 * @param logFile The path to the log file
 * @param logType The type of log (e.g., 'app', 'request', 'cache')
 * @param logDate The date of the log in YYYY-MM-DD format
 */
async function compressAndArchiveLog(logFile: string, logType: string, logDate: string): Promise<void> {
  try {
    // Create year-month directory structure if it doesn't exist
    const [year, month] = logDate.split('-');
    const archiveYearMonthDir = path.join(ARCHIVE_DIR, year, month);
    
    if (!existsSync(archiveYearMonthDir)) {
      mkdirSync(archiveYearMonthDir, { recursive: true });
    }
    
    // Create type-specific subdirectory
    const archiveTypeDir = path.join(archiveYearMonthDir, logType);
    if (!existsSync(archiveTypeDir)) {
      mkdirSync(archiveTypeDir, { recursive: true });
    }
    
    const sourceFilePath = path.join(LOG_DIR, logFile);
    const archiveFilePath = path.join(archiveTypeDir, `${logFile}.gz`);
    
    if (!DRY_RUN) {
      const gzip = createGzip();
      const source = createReadStream(sourceFilePath);
      const destination = createWriteStream(archiveFilePath);
      
      await pipeline(source, gzip, destination);
      await fs.unlink(sourceFilePath);
      
      logger.info(`Archived log file: ${logFile} to ${archiveFilePath}`, { module: 'logRotation' });
    } else {
      logger.info(`[DRY RUN] Would archive: ${logFile} to ${archiveFilePath}`, { module: 'logRotation' });
    }
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Failed to archive log file ${logFile}: ${errorMessage}`, { module: 'logRotation' });
  }
}

/**
 * Extract log type from a log file name
 */
function extractLogInfo(fileName: string): { type: string; date: string } | null {
  // Common log patterns: logType_YYYY-MM-DD.log or logType_YYYY-MM-DD_additional-info.log
  const match = fileName.match(/^([a-zA-Z]+)_(\d{4}-\d{2}-\d{2})/);
  
  if (match) {
    return { 
      type: match[1], 
      date: match[2]
    };
  }
  
  return null;
}

/**
 * Main function to rotate logs
 */
export async function rotateLogs(): Promise<{
  totalFiles: number;
  archivedFiles: number;
  errors: number;
}> {
  let archivedCount = 0;
  let errorCount = 0;
  
  try {
    await ensureArchiveDirectory();
    
    const cutoffDate = subDays(new Date(), LOG_AGE_DAYS);
    const cutoffDateStr = format(cutoffDate, 'yyyy-MM-dd');
    
    logger.info(`Starting log rotation. Archiving logs older than ${cutoffDateStr}`, { module: 'logRotation' });
    
    const files = await fs.readdir(LOG_DIR);
    const logFiles = files.filter(file => file.endsWith('.log'));
    
    logger.info(`Found ${logFiles.length} log files to process`, { module: 'logRotation' });
    
    for (const file of logFiles) {
      const logInfo = extractLogInfo(file);
      
      if (!logInfo) {
        logger.warning(`Could not parse log file name: ${file}, skipping`, { module: 'logRotation' });
        continue;
      }
      
      if (logInfo.date < cutoffDateStr) {
        try {
          await compressAndArchiveLog(file, logInfo.type, logInfo.date);
          archivedCount++;
        } catch (err) {
          errorCount++;
        }
      }
    }
    
    logger.info(`Log rotation completed. Archived ${archivedCount} of ${logFiles.length} files with ${errorCount} errors.`, {
      module: 'logRotation'
    });
    
    return {
      totalFiles: logFiles.length,
      archivedFiles: archivedCount,
      errors: errorCount
    };
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Log rotation failed: ${errorMessage}`, { module: 'logRotation' });
    return {
      totalFiles: 0,
      archivedFiles: 0,
      errors: 1
    };
  }
}

// Allow running as a standalone script
if (require.main === module) {
  rotateLogs()
    .then(result => {
      console.log('Log rotation completed:', result);
      process.exit(0);
    })
    .catch(error => {
      console.error('Log rotation failed:', error);
      process.exit(1);
    });
}
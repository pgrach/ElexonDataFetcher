/**
 * Wind Generation Data API Routes
 * 
 * This router provides endpoints for retrieving and managing wind generation data.
 * It includes endpoints for retrieving wind generation data by date, checking data status,
 * and manually triggering updates for system administrators.
 */

import express, { Request, Response } from 'express';
import { 
  getWindGenerationDataForDate, 
  processSingleDate, 
  processDateRange,
  processRecentDays,
  getLatestDataDate,
  hasWindDataForDate
} from '../services/windGenerationService';
import { isValidDateString, isValidYear } from '../utils/dates';
import { ValidationError } from '../utils/errors';
import { logger } from '../utils/logger';
import { getWindDataServiceStatus, manualUpdate } from '../services/windDataUpdateService';
import { db } from '../../db';
import { sql } from 'drizzle-orm';

const router = express.Router();

/**
 * Get wind generation data for a specific date
 * 
 * @route GET /api/wind-generation/date/:date
 * @param {string} date.path.required - The settlement date in YYYY-MM-DD format
 */
router.get('/date/:date', async (req: Request, res: Response) => {
  try {
    const { date } = req.params;
    
    if (!isValidDateString(date)) {
      throw new ValidationError('Invalid date format. Use YYYY-MM-DD format.');
    }
    
    const data = await getWindGenerationDataForDate(date);
    
    if (data.length === 0) {
      return res.status(404).json({
        message: `No wind generation data found for ${date}`,
        date
      });
    }
    
    res.json({
      date,
      periods: data.length,
      data
    });
  } catch (error) {
    logger.error('Error fetching wind generation data', {
      module: 'windGenerationRoutes',
      error: error instanceof Error ? error.message : String(error)
    });
    
    res.status(error instanceof ValidationError ? 400 : 500).json({
      error: error instanceof Error ? error.message : 'An unexpected error occurred',
    });
  }
});

/**
 * Check if wind generation data exists for a specific date
 * 
 * @route GET /api/wind-generation/check/:date
 * @param {string} date.path.required - The settlement date in YYYY-MM-DD format
 */
router.get('/check/:date', async (req: Request, res: Response) => {
  try {
    const { date } = req.params;
    
    if (!isValidDateString(date)) {
      throw new ValidationError('Invalid date format. Use YYYY-MM-DD format.');
    }
    
    const exists = await hasWindDataForDate(date);
    
    res.json({
      date,
      exists
    });
  } catch (error) {
    logger.error('Error checking wind generation data', {
      module: 'windGenerationRoutes',
      error: error instanceof Error ? error.message : String(error)
    });
    
    res.status(error instanceof ValidationError ? 400 : 500).json({
      error: error instanceof Error ? error.message : 'An unexpected error occurred',
    });
  }
});

/**
 * Get status of the wind generation data service
 * 
 * @route GET /api/wind-generation/status
 */
router.get('/status', async (req: Request, res: Response) => {
  try {
    const serviceStatus = getWindDataServiceStatus();
    const latestDate = await getLatestDataDate();
    
    res.json({
      serviceStatus,
      dataStatus: {
        latestDate
      }
    });
  } catch (error) {
    logger.error('Error getting wind generation service status', {
      module: 'windGenerationRoutes',
      error: error instanceof Error ? error.message : String(error)
    });
    
    res.status(500).json({
      error: error instanceof Error ? error.message : 'An unexpected error occurred',
    });
  }
});

/**
 * Manually trigger wind generation data update
 * 
 * @route POST /api/wind-generation/update/recent
 * @param {number} days.query - Number of days to update (default: 2)
 */
router.post('/update/recent', async (req: Request, res: Response) => {
  try {
    const days = req.query.days ? parseInt(req.query.days as string, 10) : 2;
    
    if (isNaN(days) || days < 1 || days > 30) {
      throw new ValidationError('Days parameter must be a number between 1 and 30');
    }
    
    logger.info(`Manual update triggered for last ${days} days`, {
      module: 'windGenerationRoutes'
    });
    
    // Start update in background
    manualUpdate(days)
      .then(result => {
        logger.info(`Manual update completed with result: ${result}`, {
          module: 'windGenerationRoutes'
        });
      })
      .catch(error => {
        logger.error('Error in manual update', {
          module: 'windGenerationRoutes',
          error: error instanceof Error ? error.message : String(error)
        });
      });
    
    res.json({
      message: `Started updating wind generation data for the last ${days} days`,
      status: 'processing'
    });
  } catch (error) {
    logger.error('Error triggering wind generation update', {
      module: 'windGenerationRoutes',
      error: error instanceof Error ? error.message : String(error)
    });
    
    res.status(error instanceof ValidationError ? 400 : 500).json({
      error: error instanceof Error ? error.message : 'An unexpected error occurred',
    });
  }
});

/**
 * Process wind generation data for a specific date
 * 
 * @route POST /api/wind-generation/process/date/:date
 * @param {string} date.path.required - The settlement date in YYYY-MM-DD format
 */
router.post('/process/date/:date', async (req: Request, res: Response) => {
  try {
    const { date } = req.params;
    
    if (!isValidDateString(date)) {
      throw new ValidationError('Invalid date format. Use YYYY-MM-DD format.');
    }
    
    logger.info(`Processing wind generation data for ${date}`, {
      module: 'windGenerationRoutes'
    });
    
    // Start processing in background
    processSingleDate(date)
      .then(recordsProcessed => {
        logger.info(`Processed ${recordsProcessed} wind generation records for ${date}`, {
          module: 'windGenerationRoutes'
        });
      })
      .catch(error => {
        logger.error(`Error processing wind generation data for ${date}`, {
          module: 'windGenerationRoutes',
          error: error instanceof Error ? error.message : String(error)
        });
      });
    
    res.json({
      message: `Started processing wind generation data for ${date}`,
      status: 'processing'
    });
  } catch (error) {
    logger.error('Error processing wind generation data', {
      module: 'windGenerationRoutes',
      error: error instanceof Error ? error.message : String(error)
    });
    
    res.status(error instanceof ValidationError ? 400 : 500).json({
      error: error instanceof Error ? error.message : 'An unexpected error occurred',
    });
  }
});

/**
 * Process wind generation data for a date range
 * 
 * @route POST /api/wind-generation/process/range
 * @param {string} startDate.query.required - Start date in YYYY-MM-DD format
 * @param {string} endDate.query.required - End date in YYYY-MM-DD format
 */
router.post('/process/range', async (req: Request, res: Response) => {
  try {
    const { startDate, endDate } = req.query;
    
    if (!startDate || !endDate || 
        !isValidDateString(startDate as string) || 
        !isValidDateString(endDate as string)) {
      throw new ValidationError('Invalid date format. Both startDate and endDate must be in YYYY-MM-DD format.');
    }
    
    const startDateStr = startDate as string;
    const endDateStr = endDate as string;
    
    logger.info(`Processing wind generation data from ${startDateStr} to ${endDateStr}`, {
      module: 'windGenerationRoutes'
    });
    
    // Start processing in background
    processDateRange(startDateStr, endDateStr)
      .then(recordsProcessed => {
        logger.info(`Processed ${recordsProcessed} wind generation records from ${startDateStr} to ${endDateStr}`, {
          module: 'windGenerationRoutes'
        });
      })
      .catch(error => {
        logger.error(`Error processing wind generation data from ${startDateStr} to ${endDateStr}`, {
          module: 'windGenerationRoutes',
          error: error instanceof Error ? error.message : String(error)
        });
      });
    
    res.json({
      message: `Started processing wind generation data from ${startDateStr} to ${endDateStr}`,
      status: 'processing'
    });
  } catch (error) {
    logger.error('Error processing wind generation data range', {
      module: 'windGenerationRoutes',
      error: error instanceof Error ? error.message : String(error)
    });
    
    res.status(error instanceof ValidationError ? 400 : 500).json({
      error: error instanceof Error ? error.message : 'An unexpected error occurred',
    });
  }
});

/**
 * Process wind generation data for an entire year
 * 
 * @route POST /api/wind-generation/process/year/:year
 * @param {string} year.path.required - Year in YYYY format
 * @param {boolean} force.query - Force reprocessing even if data exists
 */
router.post('/process/year/:year', async (req: Request, res: Response) => {
  try {
    const { year } = req.params;
    const force = req.query.force === 'true';
    
    if (!isValidYear(year)) {
      throw new ValidationError('Invalid year format. Use YYYY format.');
    }
    
    const startDate = `${year}-01-01`;
    const endDate = `${year}-12-31`;
    
    logger.info(`Processing wind generation data for entire year ${year}${force ? ' (force)' : ''}`, {
      module: 'windGenerationRoutes'
    });
    
    // Check if we already have data for this year
    const existingRecords = await db.execute(sql`
      SELECT COUNT(*) as count
      FROM wind_generation_data
      WHERE EXTRACT(YEAR FROM settlement_date) = ${parseInt(year, 10)}
    `);

    const recordCount = existingRecords[0]?.count ? parseInt(existingRecords[0].count as string, 10) : 0;
    
    if (recordCount > 0 && !force) {
      return res.status(409).json({
        message: `Wind generation data for ${year} already exists (${recordCount} records). Use force=true to reprocess.`,
        year,
        recordCount,
        status: 'skipped'
      });
    }
    
    // Start processing in background (using 3-month chunks to manage API rate limits)
    const quarters = [
      { start: `${year}-01-01`, end: `${year}-03-31` },
      { start: `${year}-04-01`, end: `${year}-06-30` },
      { start: `${year}-07-01`, end: `${year}-09-30` },
      { start: `${year}-10-01`, end: `${year}-12-31` }
    ];
    
    let totalRecords = 0;
    
    // Process each quarter with a delay between them
    const processQuarters = async () => {
      for (const [index, quarter] of quarters.entries()) {
        try {
          logger.info(`Processing Q${index + 1} ${year}: ${quarter.start} to ${quarter.end}`, {
            module: 'windGenerationRoutes'
          });
          
          const records = await processDateRange(quarter.start, quarter.end);
          totalRecords += records;
          
          logger.info(`Processed ${records} records for Q${index + 1} ${year}`, {
            module: 'windGenerationRoutes'
          });
          
          // Add delay between quarters to avoid rate limiting
          if (index < quarters.length - 1) {
            await new Promise(resolve => setTimeout(resolve, 3000));
          }
        } catch (error) {
          logger.error(`Error processing Q${index + 1} ${year}`, {
            module: 'windGenerationRoutes',
            error: error instanceof Error ? error.message : String(error)
          });
        }
      }
      
      logger.info(`Completed processing year ${year}, total records: ${totalRecords}`, {
        module: 'windGenerationRoutes'
      });
    };
    
    // Start the background processing
    processQuarters().catch(error => {
      logger.error(`Error in year processing for ${year}`, {
        module: 'windGenerationRoutes',
        error: error instanceof Error ? error.message : String(error)
      });
    });
    
    res.json({
      message: `Started processing wind generation data for year ${year}${force ? ' (forced)' : ''}`,
      year,
      status: 'processing'
    });
  } catch (error) {
    logger.error('Error processing wind generation data for year', {
      module: 'windGenerationRoutes',
      error: error instanceof Error ? error.message : String(error)
    });
    
    res.status(error instanceof ValidationError ? 400 : 500).json({
      error: error instanceof Error ? error.message : 'An unexpected error occurred',
    });
  }
});

export default router;
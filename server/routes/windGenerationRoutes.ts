/**
 * Wind Generation Data API Routes
 * 
 * This router provides endpoints for retrieving and managing wind generation data.
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
import { isValidDateString } from '../utils/dates';
import { ValidationError } from '../utils/errors';
import { logger } from '../utils/logger';
import { getWindDataServiceStatus, manualUpdate } from '../services/windDataUpdater';

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

export default router;
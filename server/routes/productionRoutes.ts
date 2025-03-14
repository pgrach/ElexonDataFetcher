/**
 * Production Data Routes
 * 
 * This router provides endpoints for analyzing production data,
 * including Physical Notification (PN) data and curtailment percentages.
 */

import express, { Request, Response } from 'express';
import { isValidDateString } from '../utils/dates';
import { ValidationError } from '../utils/errors';
import { logger } from '../utils/logger';
import { 
  getPNDataForPeriod, 
  calculateFarmCurtailmentPercentage,
  calculateLeadPartyCurtailmentPercentage
} from '../services/pnDataService';

const router = express.Router();

/**
 * Get Physical Notification (PN) data for a specific date and period
 * 
 * @route GET /api/production/pn-data/:date/:period
 * @param {string} date.path.required - The settlement date in YYYY-MM-DD format
 * @param {number} period.path.required - The settlement period (1-48)
 * @param {string} farmId.query - Optional farm ID to filter results
 */
router.get('/pn-data/:date/:period', async (req: Request, res: Response) => {
  try {
    const { date, period } = req.params;
    const { farmId } = req.query;
    
    // Validate parameters
    if (!isValidDateString(date)) {
      throw new ValidationError('Invalid date format. Use YYYY-MM-DD format.');
    }
    
    const periodNum = parseInt(period, 10);
    if (isNaN(periodNum) || periodNum < 1 || periodNum > 48) {
      throw new ValidationError('Period must be a number between 1 and 48.');
    }
    
    const pnData = await getPNDataForPeriod(
      date, 
      periodNum,
      farmId ? String(farmId) : undefined
    );
    
    res.json({
      date,
      period: periodNum,
      count: pnData.length,
      data: pnData
    });
  } catch (error) {
    logger.error('Error fetching PN data', {
      module: 'productionRoutes',
      error: error instanceof Error ? error.message : String(error),
    });
    
    res.status(error instanceof ValidationError ? 400 : 500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

/**
 * Calculate curtailment percentage for a specific farm on a specific date
 * 
 * @route GET /api/production/curtailment-percentage/farm/:farmId/:date
 * @param {string} farmId.path.required - The farm ID (BMU)
 * @param {string} date.path.required - The settlement date in YYYY-MM-DD format
 */
router.get('/curtailment-percentage/farm/:farmId/:date', async (req: Request, res: Response) => {
  try {
    const { farmId, date } = req.params;
    
    // Validate parameters
    if (!farmId) {
      throw new ValidationError('Farm ID is required');
    }
    
    if (!isValidDateString(date)) {
      throw new ValidationError('Invalid date format. Use YYYY-MM-DD format.');
    }
    
    const curtailmentStats = await calculateFarmCurtailmentPercentage(farmId, date);
    
    res.json(curtailmentStats);
  } catch (error) {
    logger.error('Error calculating farm curtailment percentage', {
      module: 'productionRoutes',
      error: error instanceof Error ? error.message : String(error),
    });
    
    res.status(error instanceof ValidationError ? 400 : 500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

/**
 * Calculate curtailment percentage for all farms of a lead party on a specific date
 * 
 * @route GET /api/production/curtailment-percentage/lead-party/:leadPartyName/:date
 * @param {string} leadPartyName.path.required - The lead party name
 * @param {string} date.path.required - The settlement date in YYYY-MM-DD format
 */
router.get('/curtailment-percentage/lead-party/:leadPartyName/:date', async (req: Request, res: Response) => {
  try {
    const { leadPartyName, date } = req.params;
    
    // Validate parameters
    if (!leadPartyName) {
      throw new ValidationError('Lead party name is required');
    }
    
    if (!isValidDateString(date)) {
      throw new ValidationError('Invalid date format. Use YYYY-MM-DD format.');
    }
    
    const curtailmentStats = await calculateLeadPartyCurtailmentPercentage(
      decodeURIComponent(leadPartyName), 
      date
    );
    
    res.json(curtailmentStats);
  } catch (error) {
    logger.error('Error calculating lead party curtailment percentage', {
      module: 'productionRoutes',
      error: error instanceof Error ? error.message : String(error),
    });
    
    res.status(error instanceof ValidationError ? 400 : 500).json({
      error: error instanceof Error ? error.message : String(error),
    });
  }
});

export default router;
/**
 * Curtailment Analytics Routes
 * 
 * API endpoints for advanced curtailment analytics including
 * curtailment percentages, efficiency metrics, and comparison tools.
 */

import express, { Request, Response } from 'express';
import {
  getFarmCurtailmentPercentage,
  getLeadPartyCurtailmentPercentage,
  getTopCurtailedFarmsByPercentage
} from '../services/curtailmentAnalyticsService';
import { isValidDateString } from '../utils/dates';

const router = express.Router();

/**
 * @route GET /api/curtailment-analytics/farm/:farmId
 * @description Get curtailment percentage for a specific farm
 * @param farmId - The farm ID to analyze
 * @param period - day, month, or year
 * @param value - The specific date, month (YYYY-MM), or year (YYYY)
 */
router.get('/farm/:farmId', async (req: Request, res: Response) => {
  try {
    const { farmId } = req.params;
    const period = req.query.period as 'day' | 'month' | 'year' || 'day';
    const value = req.query.value as string || new Date().toISOString().split('T')[0];
    
    // Validate parameters
    if (!farmId) {
      return res.status(400).json({ error: 'Farm ID is required' });
    }
    
    if (!['day', 'month', 'year'].includes(period)) {
      return res.status(400).json({ error: 'Invalid period. Must be day, month, or year' });
    }
    
    // Get curtailment percentage
    const result = await getFarmCurtailmentPercentage(farmId, period, value);
    
    return res.json(result);
  } catch (error: any) {
    console.error('Error in farm curtailment percentage route:', error);
    return res.status(500).json({ error: 'Failed to get farm curtailment data', message: error.message });
  }
});

/**
 * @route GET /api/curtailment-analytics/lead-party/:leadPartyName
 * @description Get curtailment percentage for a lead party (all farms)
 * @param leadPartyName - The lead party to analyze
 * @param period - day, month, or year
 * @param value - The specific date, month (YYYY-MM), or year (YYYY)
 */
router.get('/lead-party/:leadPartyName', async (req: Request, res: Response) => {
  try {
    const { leadPartyName } = req.params;
    const period = req.query.period as 'day' | 'month' | 'year' || 'day';
    const value = req.query.value as string || new Date().toISOString().split('T')[0];
    
    // Validate parameters
    if (!leadPartyName) {
      return res.status(400).json({ error: 'Lead party name is required' });
    }
    
    if (!['day', 'month', 'year'].includes(period)) {
      return res.status(400).json({ error: 'Invalid period. Must be day, month, or year' });
    }
    
    // Get curtailment percentage
    const result = await getLeadPartyCurtailmentPercentage(leadPartyName, period, value);
    
    return res.json(result);
  } catch (error: any) {
    console.error('Error in lead party curtailment percentage route:', error);
    return res.status(500).json({ error: 'Failed to get lead party curtailment data', message: error.message });
  }
});

/**
 * @route GET /api/curtailment-analytics/top-farms
 * @description Get farms with highest curtailment percentages
 * @param period - day, month, or year
 * @param value - The specific date, month (YYYY-MM), or year (YYYY)
 * @param limit - Maximum number of farms to return (default: 10)
 */
router.get('/top-farms', async (req: Request, res: Response) => {
  try {
    const period = req.query.period as 'day' | 'month' | 'year' || 'day';
    const value = req.query.value as string || new Date().toISOString().split('T')[0];
    const limit = parseInt(req.query.limit as string || '10');
    
    // Validate parameters
    if (!['day', 'month', 'year'].includes(period)) {
      return res.status(400).json({ error: 'Invalid period. Must be day, month, or year' });
    }
    
    // Get top curtailed farms
    const result = await getTopCurtailedFarmsByPercentage(period, value, limit);
    
    return res.json(result);
  } catch (error: any) {
    console.error('Error in top curtailed farms route:', error);
    return res.status(500).json({ error: 'Failed to get top curtailed farms', message: error.message });
  }
});

export default router;
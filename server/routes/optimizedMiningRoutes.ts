/**
 * Optimized Mining Routes
 * 
 * These routes provide the same functionality as the existing miningPotentialRoutes
 * but use direct table queries instead of materialized views for better maintainability.
 */

import express, { Request, Response } from "express";
import { format, parseISO, isValid } from "date-fns";
import axios from "axios";
import { 
  getDailyMiningPotential,
  getMonthlyMiningPotential,
  getYearlyMiningPotential,
  getFarmStatistics
} from "../services/optimizedMiningService";

// Minerstat API helper function
async function fetchFromMinerstat() {
  try {
    const response = await axios.get('https://api.minerstat.com/v2/coins?list=BTC');
    const btcData = response.data[0];

    if (!btcData || typeof btcData.difficulty !== 'number' || typeof btcData.price !== 'number') {
      throw new Error('Invalid response format from Minerstat API');
    }

    // Convert USD to GBP (using a fixed rate - in production this should be fetched from a forex API)
    const usdToGbpRate = 0.79; // Example fixed rate
    const priceInGbp = btcData.price * usdToGbpRate;

    console.log('Minerstat API response:', {
      difficulty: btcData.difficulty,
      priceUsd: btcData.price,
      priceGbp: priceInGbp
    });

    return {
      difficulty: btcData.difficulty,
      price: priceInGbp // Return price in GBP
    };
  } catch (error: any) {
    console.error('Error fetching from Minerstat:', error.message);
    if (error.response) {
      console.error('API Response:', {
        status: error.response.status,
        data: error.response.data
      });
    }
    throw error;
  }
}

const router = express.Router();

// Daily mining potential endpoint
router.get('/daily', async (req: Request, res: Response) => {
  try {
    const requestDate = req.query.date ? parseISO(req.query.date as string) : new Date();
    const minerModel = req.query.minerModel as string || 'S19J_PRO';
    const farmId = req.query.farmId as string;
    
    // Validate date
    if (!isValid(requestDate)) {
      return res.status(400).json({
        error: 'Invalid date format',
        message: 'Please provide a valid date in ISO format (YYYY-MM-DD)'
      });
    }
    
    const formattedDate = format(requestDate, 'yyyy-MM-dd');
    
    console.log('Daily mining potential request:', {
      date: formattedDate,
      minerModel,
      farmId
    });
    
    // Get current price from Minerstat
    let currentPrice;
    try {
      const { price } = await fetchFromMinerstat();
      currentPrice = price;
    } catch (error) {
      console.error('Failed to fetch current price:', error);
      currentPrice = null;
    }
    
    // Get daily mining potential data using optimized service
    const potentialData = await getDailyMiningPotential(formattedDate, minerModel, farmId);
    
    res.json({
      date: formattedDate,
      bitcoinMined: Number(potentialData.totalBitcoinMined),
      valueAtCurrentPrice: Number(potentialData.totalBitcoinMined) * (currentPrice || 0),
      curtailedEnergy: Number(potentialData.totalCurtailedEnergy),
      difficulty: potentialData.difficulty,
      currentPrice
    });
  } catch (error) {
    console.error('Error in daily mining potential endpoint:', error);
    res.status(500).json({
      error: 'Failed to calculate daily mining potential',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Monthly mining potential endpoint
router.get('/monthly/:yearMonth', async (req: Request, res: Response) => {
  try {
    const { yearMonth } = req.params;
    const minerModel = req.query.minerModel as string || 'S19J_PRO';
    const farmId = req.query.farmId as string;
    
    // Validate yearMonth format (YYYY-MM)
    if (!yearMonth.match(/^\d{4}-\d{2}$/)) {
      return res.status(400).json({
        error: 'Invalid format',
        message: 'Please provide a valid year-month in YYYY-MM format'
      });
    }
    
    console.log('Monthly mining potential request:', {
      yearMonth,
      minerModel,
      farmId
    });
    
    // Get current price from Minerstat
    let currentPrice;
    try {
      const { price } = await fetchFromMinerstat();
      currentPrice = price;
    } catch (error) {
      console.error('Failed to fetch current price:', error);
      currentPrice = null;
    }
    
    // Get monthly mining potential data
    const potentialData = await getMonthlyMiningPotential(yearMonth, minerModel, farmId);
    
    res.json({
      month: yearMonth,
      bitcoinMined: Number(potentialData.totalBitcoinMined),
      valueAtCurrentPrice: Number(potentialData.totalBitcoinMined) * (currentPrice || 0),
      curtailedEnergy: Number(potentialData.totalCurtailedEnergy),
      averageDifficulty: potentialData.averageDifficulty,
      currentPrice
    });
  } catch (error) {
    console.error('Error in monthly mining potential endpoint:', error);
    res.status(500).json({
      error: 'Failed to calculate monthly mining potential',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Yearly mining potential endpoint
router.get('/yearly/:year', async (req: Request, res: Response) => {
  try {
    const { year } = req.params;
    const minerModel = req.query.minerModel as string || 'S19J_PRO';
    const farmId = req.query.farmId as string;
    
    // Validate year format
    if (!year.match(/^\d{4}$/)) {
      return res.status(400).json({
        error: 'Invalid year format',
        message: 'Please provide a valid year in YYYY format'
      });
    }
    
    console.log('Yearly mining potential request:', {
      year,
      minerModel,
      farmId
    });
    
    // Get current price from Minerstat
    let currentPrice;
    try {
      const { price } = await fetchFromMinerstat();
      currentPrice = price;
    } catch (error) {
      console.error('Failed to fetch current price:', error);
      currentPrice = null;
    }
    
    // Get yearly mining potential data
    const potentialData = await getYearlyMiningPotential(year, minerModel, farmId);
    
    res.json({
      year,
      bitcoinMined: Number(potentialData.totalBitcoinMined),
      valueAtCurrentPrice: Number(potentialData.totalBitcoinMined) * (currentPrice || 0),
      curtailedEnergy: Number(potentialData.totalCurtailedEnergy),
      totalPayment: potentialData.totalPayment,
      averageDifficulty: potentialData.averageDifficulty,
      currentPrice
    });
  } catch (error) {
    console.error('Error in yearly mining potential endpoint:', error);
    res.status(500).json({
      error: 'Failed to calculate yearly mining potential',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Farm-specific statistics endpoint
router.get('/farm/:farmId', async (req: Request, res: Response) => {
  try {
    const { farmId } = req.params;
    const period = (req.query.period as 'day' | 'month' | 'year') || 'month';
    const value = req.query.value as string || format(new Date(), period === 'day' ? 'yyyy-MM-dd' : period === 'month' ? 'yyyy-MM' : 'yyyy');
    
    // Validate period type
    if (!['day', 'month', 'year'].includes(period)) {
      return res.status(400).json({
        error: 'Invalid period type',
        message: 'Period must be one of: day, month, year'
      });
    }
    
    // Validate value format based on period
    const formatValid = 
      (period === 'day' && value.match(/^\d{4}-\d{2}-\d{2}$/)) ||
      (period === 'month' && value.match(/^\d{4}-\d{2}$/)) ||
      (period === 'year' && value.match(/^\d{4}$/));
      
    if (!formatValid) {
      return res.status(400).json({
        error: 'Invalid value format',
        message: `For period type '${period}', value must be in format: ${period === 'day' ? 'YYYY-MM-DD' : period === 'month' ? 'YYYY-MM' : 'YYYY'}`
      });
    }
    
    // Get farm statistics
    const stats = await getFarmStatistics(farmId, period, value);
    
    res.json(stats);
  } catch (error) {
    console.error('Error in farm statistics endpoint:', error);
    res.status(500).json({
      error: 'Failed to retrieve farm statistics',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

export default router;
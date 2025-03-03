/**
 * Mining Potential Routes
 * 
 * Endpoints for querying optimized mining potential data using materialized views.
 * These endpoints provide the same functionality as the existing curtailment routes
 * but with improved performance by using pre-calculated data.
 */

import express, { Request, Response } from "express";
import { format, parseISO, isValid } from "date-fns";
import axios from "axios";
import { 
  getDailyMiningPotential, 
  getYearlyMiningPotential 
} from "../services/miningPotentialService";

// Minerstat API helper function (copied from curtailmentRoutes.ts)
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
    
    // Get daily mining potential data
    const potentialData = await getDailyMiningPotential(formattedDate, minerModel, farmId);
    
    res.json({
      date: formattedDate,
      bitcoinMined: Number(potentialData.totalBitcoinMined),
      valueAtCurrentPrice: Number(potentialData.totalBitcoinMined) * (currentPrice || 0),
      curtailedEnergy: Number(potentialData.totalCurtailedEnergy),
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

export default router;
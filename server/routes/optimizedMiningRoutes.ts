/**
 * Optimized Mining Routes
 * 
 * These routes provide the same functionality as the existing miningPotentialRoutes
 * but use direct table queries instead of materialized views for better maintainability.
 */

import express, { Request, Response } from "express";
import { format, parseISO, isValid } from "date-fns";
import axios from "axios";
import { inArray } from "drizzle-orm";
import { 
  getDailyMiningPotential,
  getMonthlyMiningPotential,
  getYearlyMiningPotential,
  getFarmStatistics,
  getTopCurtailedFarms,
  getAvailableFarms,
  getFarmOpportunityComparison
} from "../services/optimizedMiningService";

import { priceCache, difficultyCache } from '../utils/cache';

// Minerstat API helper function
async function fetchFromMinerstat() {
  try {
    // First check if we have cached values
    const cachedPrice = priceCache.get('current');
    const cachedDifficulty = difficultyCache.get('current');
    
    // If both values are in cache, return them
    if (cachedPrice !== undefined && cachedDifficulty !== undefined) {
      console.log('Using cached Minerstat data:', {
        difficulty: cachedDifficulty,
        priceGbp: cachedPrice,
        source: 'cache'
      });
      
      return {
        difficulty: cachedDifficulty,
        price: cachedPrice
      };
    }
    
    // Otherwise, fetch from API
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
      priceGbp: priceInGbp,
      source: 'api'
    });

    // Store values in cache
    priceCache.set('current', priceInGbp);
    difficultyCache.set('current', btcData.difficulty);

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
    
    // Check if we have any cached values to fall back to
    const cachedPrice = priceCache.get('current');
    const cachedDifficulty = difficultyCache.get('current');
    
    if (cachedPrice !== undefined && cachedDifficulty !== undefined) {
      console.log('Falling back to cached values after API error');
      return {
        difficulty: cachedDifficulty,
        price: cachedPrice
      };
    }
    
    // Last resort fallback values
    return {
      difficulty: 110568428300952, // Current difficulty as fallback
      price: 60000 // Example GBP price as fallback
    };
  }
}

/**
 * Helper function to fetch current Bitcoin price from Minerstat API
 * with error handling and consistent return type
 */
async function fetchCurrentPrice(): Promise<number | null> {
  try {
    const { price } = await fetchFromMinerstat();
    return price;
  } catch (error) {
    console.error('Failed to fetch current price:', error);
    return null;
  }
}

const router = express.Router();

// Get all available farms endpoint
router.get('/farms', async (req: Request, res: Response) => {
  try {
    // Extract date parameter if provided (e.g., ?date=2025-03-10)
    const date = req.query.date as string | undefined;
    // Extract timeframe parameter (e.g., ?timeframe=daily)
    const timeframe = req.query.timeframe as 'daily' | 'monthly' | 'yearly' | undefined || 'daily';
    
    console.log(`Farms request with timeframe: ${timeframe}, date: ${date || 'not specified'}`);
    
    const farms = await getAvailableFarms(date, timeframe);
    res.json(farms);
  } catch (error) {
    console.error('Error getting available farms:', error);
    res.status(500).json({ error: 'Failed to get available farms' });
  }
});

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
    const currentPrice = await fetchCurrentPrice();
    
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
    const leadParty = req.query.leadParty as string;
    let farmId = req.query.farmId as string;
    
    // Validate yearMonth format (YYYY-MM)
    if (!yearMonth.match(/^\d{4}-\d{2}$/)) {
      return res.status(400).json({
        error: 'Invalid format',
        message: 'Please provide a valid year-month in YYYY-MM format'
      });
    }
    
    // Handle leadParty parameter (for compatibility with the frontend)
    if (leadParty && !farmId) {
      // If leadParty is provided but farmId is not, try to find the corresponding farmId
      // First, get all farms for this lead party
      const { db } = await import("../../db");
      const { curtailmentRecords } = await import("../../db/schema");
      const { eq } = await import("drizzle-orm");
      
      const farms = await db
        .select({
          farmId: curtailmentRecords.farmId
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.leadPartyName, leadParty))
        .groupBy(curtailmentRecords.farmId);

      console.log(`Found ${farms.length} farms for lead party ${leadParty}`);
      
      if (farms.length > 0) {
        // Use the farmIds for filtering
        farmId = farms[0].farmId; // Just use the first farm for simplicity
      }
    }
    
    console.log('Monthly mining potential request:', {
      yearMonth,
      minerModel,
      leadParty,
      farmId
    });
    
    // Get current price from Minerstat
    const currentPrice = await fetchCurrentPrice();
    
    // If a lead party is specified, handle it specifically
    if (leadParty) {
      // Import dependencies needed for this section
      const { db } = await import("../../db");
      const { curtailmentRecords, historicalBitcoinCalculations } = await import("../../db/schema");
      const { eq, and, inArray, sql } = await import("drizzle-orm");
      
      // Get date range for the month
      const [year, month] = yearMonth.split('-').map(n => parseInt(n, 10));
      const startDate = new Date(year, month - 1, 1);
      const endDate = new Date(year, month, 0); // Last day of month
      const formattedStartDate = format(startDate, 'yyyy-MM-dd');
      const formattedEndDate = format(endDate, 'yyyy-MM-dd');
      
      // First get all farmIds that match the leadParty
      const farms = await db
        .select({
          farmId: curtailmentRecords.farmId
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.leadPartyName, leadParty))
        .groupBy(curtailmentRecords.farmId);
      
      if (farms.length === 0) {
        return res.json({
          month: yearMonth,
          bitcoinMined: 0,
          valueAtCurrentPrice: 0,
          curtailedEnergy: 0,
          difficulty: 0, // Changed from averageDifficulty
          currentPrice
        });
      }
      
      const farmIds = farms.map(f => f.farmId);
      
      // Query Bitcoin calculations for the specified farms
      const bitcoinData = await db
        .select({
          totalBitcoinMined: sql<string>`SUM(bitcoin_mined)`,
          avgDifficulty: sql<string>`AVG(difficulty)`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            sql`settlement_date BETWEEN ${formattedStartDate} AND ${formattedEndDate}`,
            eq(historicalBitcoinCalculations.minerModel, minerModel),
            inArray(historicalBitcoinCalculations.farmId, farmIds)
          )
        );
      
      // Query curtailment data for the specified lead party
      const curtailmentData = await db
        .select({
          totalCurtailedEnergy: sql<string>`SUM(ABS(volume))`
        })
        .from(curtailmentRecords)
        .where(
          and(
            sql`settlement_date BETWEEN ${formattedStartDate} AND ${formattedEndDate}`,
            eq(curtailmentRecords.leadPartyName, leadParty)
          )
        );
      
      return res.json({
        month: yearMonth,
        bitcoinMined: Number(bitcoinData[0]?.totalBitcoinMined || 0),
        valueAtCurrentPrice: Number(bitcoinData[0]?.totalBitcoinMined || 0) * (currentPrice || 0),
        curtailedEnergy: Number(curtailmentData[0]?.totalCurtailedEnergy || 0),
        difficulty: Number(bitcoinData[0]?.avgDifficulty || 0), // Changed from averageDifficulty
        currentPrice
      });
    }
    
    // Get monthly mining potential data for non-lead party requests
    const potentialData = await getMonthlyMiningPotential(yearMonth, minerModel, farmId);
    
    res.json({
      month: yearMonth,
      bitcoinMined: Number(potentialData.totalBitcoinMined),
      valueAtCurrentPrice: Number(potentialData.totalBitcoinMined) * (currentPrice || 0),
      curtailedEnergy: Number(potentialData.totalCurtailedEnergy),
      difficulty: potentialData.difficulty, // Changed from averageDifficulty
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
    const leadParty = req.query.leadParty as string;
    let farmId = req.query.farmId as string;
    
    // Validate year format
    if (!year.match(/^\d{4}$/)) {
      return res.status(400).json({
        error: 'Invalid year format',
        message: 'Please provide a valid year in YYYY format'
      });
    }
    
    // Get current price from Minerstat first, so it's available throughout the function
    const currentPrice = await fetchCurrentPrice();
    
    // When a leadParty is specified, we need to handle it directly here
    // instead of relying on getYearlyMiningPotential to process it
    if (leadParty) {
      // Import required dependencies
      const { db } = await import("../../db");
      const { curtailmentRecords, historicalBitcoinCalculations } = await import("../../db/schema");
      const { eq, inArray, and, sql } = await import("drizzle-orm");
      
      // Get all farms for this lead party
      const farms = await db
        .select({
          farmId: curtailmentRecords.farmId
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.leadPartyName, leadParty))
        .groupBy(curtailmentRecords.farmId);

      const farmIds = farms.map(f => f.farmId);
      console.log(`Found ${farms.length} farms for lead party ${leadParty}: ${farmIds.join(', ')}`);
      
      if (farms.length === 0) {
        return res.json({
          year,
          bitcoinMined: 0,
          valueAtCurrentPrice: 0,
          curtailedEnergy: 0,
          totalPayment: 0,
          difficulty: 0, // Changed from averageDifficulty
          currentPrice
        });
      }
      
      // Directly query Bitcoin calculations for all farms in this lead party
      const bitcoinResults = await db
        .select({
          totalBitcoinMined: sql<number>`SUM(bitcoin_mined)`,
          avgDifficulty: sql<number>`AVG(difficulty)` // Average difficulty for the year
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            sql`EXTRACT(YEAR FROM settlement_date) = ${parseInt(year, 10)}`,
            eq(historicalBitcoinCalculations.minerModel, minerModel),
            inArray(historicalBitcoinCalculations.farmId, farmIds)
          )
        );
      
      // Query for curtailment data for all farms
      const curtailmentResults = await db
        .select({
          totalCurtailedEnergy: sql<number>`SUM(ABS(volume))`,
          totalPayment: sql<number>`SUM(payment)`
        })
        .from(curtailmentRecords)
        .where(
          and(
            sql`EXTRACT(YEAR FROM settlement_date) = ${parseInt(year, 10)}`,
            inArray(curtailmentRecords.farmId, farmIds)
          )
        );
      
      console.log(`Lead party Bitcoin results: ${JSON.stringify(bitcoinResults)}`);
      console.log(`Lead party curtailment results: ${JSON.stringify(curtailmentResults)}`);
      
      return res.json({
        year,
        bitcoinMined: Number(bitcoinResults[0]?.totalBitcoinMined || 0),
        valueAtCurrentPrice: Number(bitcoinResults[0]?.totalBitcoinMined || 0) * (currentPrice || 0),
        curtailedEnergy: Number(curtailmentResults[0]?.totalCurtailedEnergy || 0),
        totalPayment: Number(curtailmentResults[0]?.totalPayment || 0),
        difficulty: Number(bitcoinResults[0]?.avgDifficulty || 0), // Changed from averageDifficulty
        currentPrice
      });
    }
    
    console.log('Yearly mining potential request:', {
      year,
      minerModel,
      leadParty,
      farmId
    });
    
    // We already handled leadParty above, so no need to check again here
    
    // Get yearly mining potential data for a single farm or all farms
    const potentialData = await getYearlyMiningPotential(year, minerModel, farmId);
    
    res.json({
      year,
      bitcoinMined: Number(potentialData.bitcoinMined || potentialData.totalBitcoinMined),
      valueAtCurrentPrice: Number(potentialData.bitcoinMined || potentialData.totalBitcoinMined) * (currentPrice || 0),
      curtailedEnergy: Number(potentialData.curtailedEnergy || potentialData.totalCurtailedEnergy),
      totalPayment: Number(potentialData.totalPayment),
      difficulty: Number(potentialData.difficulty), // Changed from averageDifficulty
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

// Farm opportunity comparison endpoint
router.get('/farm-opportunity-comparison/:farmId', async (req: Request, res: Response) => {
  try {
    const { farmId } = req.params;
    const period = (req.query.period as 'day' | 'month') || 'day';
    let value = req.query.value as string;
    const minerModel = req.query.minerModel as string || 'S19J_PRO';
    
    // Validate farmId
    if (!farmId) {
      return res.status(400).json({
        error: 'Missing farmId',
        message: 'Please provide a valid farm ID'
      });
    }
    
    // Set default value based on period type
    if (!value) {
      if (period === 'day') {
        value = format(new Date(), 'yyyy-MM-dd');
      } else if (period === 'month') {
        value = format(new Date(), 'yyyy-MM');
      }
    }
    
    // Validate value format based on period
    if (period === 'day' && !value.match(/^\d{4}-\d{2}-\d{2}$/)) {
      return res.status(400).json({
        error: 'Invalid date format',
        message: 'Please provide a valid date in YYYY-MM-DD format'
      });
    } else if (period === 'month' && !value.match(/^\d{4}-\d{2}$/)) {
      return res.status(400).json({
        error: 'Invalid month format',
        message: 'Please provide a valid month in YYYY-MM format'
      });
    }
    
    console.log(`Farm opportunity comparison request for ${farmId}:`, {
      period,
      value,
      minerModel
    });
    
    // Get opportunity comparison data
    const data = await getFarmOpportunityComparison(period, value, farmId, minerModel);
    
    res.json(data);
  } catch (error) {
    console.error('Error in farm opportunity comparison endpoint:', error);
    res.status(500).json({
      error: 'Failed to get farm opportunity comparison data',
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
    const useLeadParty = req.query.useLeadParty === 'true';
    
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
    
    // If we want to use all farms under the same lead party
    if (useLeadParty) {
      try {
        // First, get the lead party for this farm
        const { db } = await import("../../db");
        const { curtailmentRecords, historicalBitcoinCalculations } = await import("../../db/schema");
        const { eq, inArray, and, sql } = await import("drizzle-orm");
        
        const farmResult = await db
          .select({
            leadPartyName: curtailmentRecords.leadPartyName
          })
          .from(curtailmentRecords)
          .where(eq(curtailmentRecords.farmId, farmId))
          .limit(1);
          
        if (farmResult.length === 0) {
          return res.status(404).json({
            error: 'Farm not found',
            message: `No records found for farm: ${farmId}`
          });
        }
          
        const leadPartyName = farmResult[0].leadPartyName;
        console.log(`Using lead party "${leadPartyName}" for farm ${farmId}`);
        
        // Then get all farms for this lead party
        const allFarms = await db
          .select({
            farmId: curtailmentRecords.farmId
          })
          .from(curtailmentRecords)
          .where(eq(curtailmentRecords.leadPartyName, leadPartyName))
          .groupBy(curtailmentRecords.farmId);
          
        const farmIds = allFarms.map(f => f.farmId);
        console.log(`Found ${farmIds.length} farms for lead party "${leadPartyName}": ${farmIds.join(', ')}`);
          
        // Process statistics for the farm group
        let bitcoinDateCondition;
        let curtailmentDateCondition;
        
        if (period === 'day') {
          // For a specific day
          bitcoinDateCondition = eq(historicalBitcoinCalculations.settlementDate, value);
          curtailmentDateCondition = eq(curtailmentRecords.settlementDate, value);
        } else if (period === 'month') {
          // For a specific month (YYYY-MM)
          const [year, month] = value.split('-').map(n => parseInt(n, 10));
          const startDate = new Date(year, month - 1, 1);
          const endDate = new Date(year, month, 0); // Last day of month
          const formattedStartDate = format(startDate, 'yyyy-MM-dd');
          const formattedEndDate = format(endDate, 'yyyy-MM-dd');
          
          bitcoinDateCondition = sql`${historicalBitcoinCalculations.settlementDate} BETWEEN ${formattedStartDate} AND ${formattedEndDate}`;
          curtailmentDateCondition = sql`${curtailmentRecords.settlementDate} BETWEEN ${formattedStartDate} AND ${formattedEndDate}`;
        } else if (period === 'year') {
          // For a specific year
          bitcoinDateCondition = sql`EXTRACT(YEAR FROM ${historicalBitcoinCalculations.settlementDate}) = ${parseInt(value, 10)}`;
          curtailmentDateCondition = sql`EXTRACT(YEAR FROM ${curtailmentRecords.settlementDate}) = ${parseInt(value, 10)}`;
        } else {
          throw new Error(`Invalid period type: ${period}`);
        }
        
        // Get statistics by miner model for all farms in the group
        const results = await db
          .select({
            minerModel: historicalBitcoinCalculations.minerModel,
            totalBitcoinMined: sql<number>`SUM(bitcoin_mined)`,
            periodCount: sql<number>`COUNT(DISTINCT settlement_date)`,
            averageDifficulty: sql<number>`AVG(difficulty)`
          })
          .from(historicalBitcoinCalculations)
          .where(
            and(
              bitcoinDateCondition,
              inArray(historicalBitcoinCalculations.farmId, farmIds)
            )
          )
          .groupBy(historicalBitcoinCalculations.minerModel);
        
        // Get curtailment data for the lead party
        const curtailmentData = await db
          .select({
            totalCurtailedEnergy: sql<number>`SUM(ABS(volume))`,
            periodCount: sql<number>`COUNT(DISTINCT settlement_date)`,
            totalPayment: sql<number>`SUM(payment)`
          })
          .from(curtailmentRecords)
          .where(
            and(
              curtailmentDateCondition,
              eq(curtailmentRecords.leadPartyName, leadPartyName)
            )
          );
        
        // Return statistics for the farm group
        const stats = {
          leadParty: leadPartyName,
          farmCount: farmIds.length,
          farms: farmIds,
          period,
          value,
          totalCurtailedEnergy: Number(curtailmentData[0]?.totalCurtailedEnergy || 0),
          totalPayment: Number(curtailmentData[0]?.totalPayment || 0),
          totalPeriods: Number(curtailmentData[0]?.periodCount || 0),
          minerModels: results.map(r => ({
            model: r.minerModel,
            bitcoinMined: Number(r.totalBitcoinMined || 0),
            difficulty: Number(r.averageDifficulty || 0) // Changed from averageDifficulty
          }))
        };
        
        return res.json(stats);
      } catch (error) {
        console.error(`Error getting lead party statistics: ${error instanceof Error ? error.message : 'Unknown error'}`);
        // Fall back to single farm if there's an error
      }
    }
    
    // Get statistics for a single farm
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

// Top curtailed farms endpoint
router.get('/top-farms', async (req: Request, res: Response) => {
  try {
    // Get query parameters
    const period = (req.query.period as 'day' | 'month' | 'year') || 'day';
    let value = req.query.value as string;
    const minerModel = req.query.minerModel as string || 'S19J_PRO';
    const limit = req.query.limit ? parseInt(req.query.limit as string, 10) : 5;
    
    // If no value is provided, use current date/month/year based on period
    if (!value) {
      const now = new Date();
      value = format(
        now, 
        period === 'day' ? 'yyyy-MM-dd' : 
        period === 'month' ? 'yyyy-MM' : 'yyyy'
      );
    }
    
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
        message: `For period type '${period}', value must be in format: ${
          period === 'day' ? 'YYYY-MM-DD' : 
          period === 'month' ? 'YYYY-MM' : 'YYYY'
        }`
      });
    }
    
    // Get top farms data
    const topFarms = await getTopCurtailedFarms(period, value, minerModel, limit);
    
    // Return response
    res.json(topFarms);
  } catch (error) {
    console.error('Error in top farms endpoint:', error);
    res.status(500).json({
      error: 'Failed to fetch top curtailed farms',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

export default router;
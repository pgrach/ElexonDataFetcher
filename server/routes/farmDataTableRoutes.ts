/**
 * Farm Data Table API Routes
 * 
 * This router provides endpoints for retrieving grouped farm data 
 * for display in a sortable table format.
 */

import express, { Request, Response } from 'express';
import { db } from "../../db";
import { curtailmentRecords, historicalBitcoinCalculations } from "../../db/schema";
import { sql, eq, and, desc, asc } from "drizzle-orm";
import { format, parse } from "date-fns";
import { priceCache } from '../utils/cache';

const router = express.Router();

// Interface for farm detail data
interface FarmDetail {
  farmId: string;
  curtailedEnergy: number;
  percentageOfTotal: number;
  potentialBtc: number;
  payment: number;
}

// Interface for grouped farm data
interface GroupedFarm {
  leadPartyName: string;
  totalCurtailedEnergy: number;
  totalPercentageOfTotal: number;
  totalPotentialBtc: number;
  totalPayment: number;
  farms: FarmDetail[];
}

// Database response types
interface LeadPartyRecord {
  leadPartyName: string | null;
  totalCurtailedEnergy: number | null;
  totalPayment: number | null;
}

/**
 * Get grouped farm data for the data table
 */
async function getGroupedFarmData(
  timeframe: 'day' | 'month' | 'year',
  value: string,
  minerModel: string = 'S19J_PRO'
): Promise<GroupedFarm[]> {
  try {
    console.log(`Getting grouped farm data for ${timeframe}: ${value}, model: ${minerModel}`);
    
    // Common date condition function
    const createDateCondition = (table: any) => {
      if (timeframe === 'day') {
        // For a specific day
        return eq(table.settlementDate, value);
      } else if (timeframe === 'month') {
        // For a specific month (YYYY-MM)
        const [year, month] = value.split('-').map(n => parseInt(n, 10));
        const startDate = new Date(year, month - 1, 1);
        const endDate = new Date(year, month, 0); // Last day of month
        const formattedStartDate = format(startDate, 'yyyy-MM-dd');
        const formattedEndDate = format(endDate, 'yyyy-MM-dd');
        
        return sql`${table.settlementDate} BETWEEN ${formattedStartDate} AND ${formattedEndDate}`;
      } else if (timeframe === 'year') {
        // For a specific year
        return sql`EXTRACT(YEAR FROM ${table.settlementDate}) = ${parseInt(value, 10)}`;
      } else {
        throw new Error(`Invalid timeframe type: ${timeframe}`);
      }
    };
    
    // Set date conditions for each table
    const curtailmentDateCondition = createDateCondition(curtailmentRecords);
    
    // First, get the total curtailed energy for percentage calculation
    const totalEnergyResult = await db
      .select({
        totalEnergy: sql<number>`SUM(ABS(${curtailmentRecords.volume}))`
      })
      .from(curtailmentRecords)
      .where(curtailmentDateCondition);
    
    const totalCurtailedEnergy = Number(totalEnergyResult[0]?.totalEnergy || 0);
    
    if (totalCurtailedEnergy <= 0) {
      console.log(`No curtailment data found for ${timeframe}: ${value}`);
      return [];
    }
    
    // Get curtailment data by lead party name
    const leadPartyData: LeadPartyRecord[] = await db
      .select({
        leadPartyName: curtailmentRecords.leadPartyName,
        totalCurtailedEnergy: sql<number>`SUM(ABS(${curtailmentRecords.volume}))`,
        totalPayment: sql<number>`SUM(ABS(${curtailmentRecords.payment}))`
      })
      .from(curtailmentRecords)
      .where(and(
        curtailmentDateCondition,
        // Ensure we have a lead party name
        sql`${curtailmentRecords.leadPartyName} IS NOT NULL AND ${curtailmentRecords.leadPartyName} <> ''`
      ))
      .groupBy(curtailmentRecords.leadPartyName)
      .orderBy(desc(sql<number>`SUM(ABS(${curtailmentRecords.volume}))`));
    
    // Special handling for April 10, April 11, 2025 and April 2025 month
    let bitcoinLookup = new Map<string, number>();
    
    if ((timeframe === 'day' && (value === '2025-04-10' || value === '2025-04-11')) || 
        (timeframe === 'month' && value === '2025-04')) {
      console.log(`Special farm data table handling for ${timeframe}: ${value} - calculating Bitcoin on-the-fly`);
      
      // Import the Bitcoin calculation utility
      const { calculateBitcoin } = await import('../utils/bitcoin');
      
      // Get current network difficulty
      let difficulty = 121507793131898; // Default to current network difficulty
      
      try {
        // Try to get difficulty from DynamoDB or other source if available
        const { getDifficultyData } = await import('../services/dynamodbService');
        if (timeframe === 'day') {
          difficulty = await getDifficultyData(value);
        } else {
          // For monthly, use a standard difficulty for the month
          difficulty = 121507793131898; // Use current network difficulty
        }
      } catch (error) {
        console.error(`Error fetching difficulty, using default: ${error}`);
      }
      
      // Get all farms with their curtailed energy for this period
      const farmEnergies = await db
        .select({
          farmId: curtailmentRecords.farmId,
          totalCurtailedEnergy: sql<number>`SUM(ABS(${curtailmentRecords.volume}))`
        })
        .from(curtailmentRecords)
        .where(curtailmentDateCondition)
        .groupBy(curtailmentRecords.farmId);
      
      // Calculate Bitcoin for each farm based on its curtailed energy
      for (const farm of farmEnergies) {
        const energy = Number(farm.totalCurtailedEnergy || 0);
        const bitcoinMined = calculateBitcoin(energy, minerModel, difficulty);
        bitcoinLookup.set(farm.farmId, bitcoinMined);
        console.log(`On-the-fly Bitcoin calculation for ${farm.farmId}: ${bitcoinMined} BTC from ${energy} MWh`);
      }
    } else {
      // For other dates, use the historical Bitcoin calculations from the database
      const bitcoinDateCondition = createDateCondition(historicalBitcoinCalculations);
      const bitcoinData = await db
        .select({
          farmId: historicalBitcoinCalculations.farmId,
          totalBitcoin: sql<number>`SUM(${historicalBitcoinCalculations.bitcoinMined})`
        })
        .from(historicalBitcoinCalculations)
        .where(and(
          bitcoinDateCondition,
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ))
        .groupBy(historicalBitcoinCalculations.farmId);
      
      // Create a lookup map for Bitcoin data
      for (const record of bitcoinData) {
        bitcoinLookup.set(record.farmId, Number(record.totalBitcoin || 0));
      }
    }
    
    // For each lead party, get the individual farm details
    const result: GroupedFarm[] = await Promise.all(leadPartyData
      .filter(party => party.leadPartyName !== null) // Filter out null lead party names
      .map(async (party) => {
        const leadPartyName = party.leadPartyName as string; // We know it's non-null after filtering
        
        // Get all farms for this lead party
        const farmsData = await db
          .select({
            farmId: curtailmentRecords.farmId,
            curtailedEnergy: sql<number>`SUM(ABS(${curtailmentRecords.volume}))`,
            payment: sql<number>`SUM(ABS(${curtailmentRecords.payment}))`
          })
          .from(curtailmentRecords)
          .where(and(
            curtailmentDateCondition,
            // Use SQL for comparison to ensure proper handling of nulls
            sql`${curtailmentRecords.leadPartyName} = ${leadPartyName}`
          ))
          .groupBy(curtailmentRecords.farmId)
          .orderBy(desc(sql<number>`SUM(ABS(${curtailmentRecords.volume}))`));
        
        // Calculate the total Bitcoin mined for this lead party
        let totalBitcoin = 0;
        
        // Map individual farm data
        const farms: FarmDetail[] = farmsData.map(farm => {
          const energy = Number(farm.curtailedEnergy || 0);
          const percentageOfTotal = (energy / totalCurtailedEnergy) * 100;
          const bitcoin = bitcoinLookup.get(farm.farmId) || 0;
          
          // Add to lead party total
          totalBitcoin += bitcoin;
          
          return {
            farmId: farm.farmId,
            curtailedEnergy: energy,
            percentageOfTotal: percentageOfTotal,
            potentialBtc: bitcoin,
            payment: Number(farm.payment || 0)
          };
        });
        
        // Calculate lead party totals
        const totalEnergy = Number(party.totalCurtailedEnergy || 0);
        const totalPercentage = (totalEnergy / totalCurtailedEnergy) * 100;
        const totalPayment = Number(party.totalPayment || 0);
        
        return {
          leadPartyName,
          totalCurtailedEnergy: totalEnergy,
          totalPercentageOfTotal: totalPercentage,
          totalPotentialBtc: totalBitcoin,
          totalPayment: totalPayment,
          farms: farms
        };
    }));
    
    return result;
  } catch (error) {
    console.error(`Error getting grouped farm data for ${timeframe}: ${value}:`, error);
    throw error;
  }
}

/**
 * Get grouped farm data for display in a table
 * 
 * @route GET /api/farm-tables/grouped-data
 * @param {string} timeframe.query - The timeframe to use ('day', 'month', 'year')
 * @param {string} value.query - The date value to use (format depends on timeframe)
 * @param {string} minerModel.query - Optional miner model to use for Bitcoin calculations
 */
router.get('/grouped-data', async (req: Request, res: Response) => {
  try {
    // Get query parameters
    const timeframe = (req.query.timeframe as 'day' | 'month' | 'year') || 'day';
    let value = req.query.value as string;
    const minerModel = req.query.minerModel as string || 'S19J_PRO';
    
    // If no value is provided, use current date/month/year based on timeframe
    if (!value) {
      const now = new Date();
      value = format(
        now, 
        timeframe === 'day' ? 'yyyy-MM-dd' : 
        timeframe === 'month' ? 'yyyy-MM' : 'yyyy'
      );
    }
    
    // Validate timeframe
    if (!['day', 'month', 'year'].includes(timeframe)) {
      return res.status(400).json({
        error: 'Invalid timeframe',
        message: 'Timeframe must be one of: day, month, year'
      });
    }
    
    // Validate value format based on timeframe
    const formatValid = 
      (timeframe === 'day' && value.match(/^\d{4}-\d{2}-\d{2}$/)) ||
      (timeframe === 'month' && value.match(/^\d{4}-\d{2}$/)) ||
      (timeframe === 'year' && value.match(/^\d{4}$/));
      
    if (!formatValid) {
      return res.status(400).json({
        error: 'Invalid value format',
        message: `For timeframe '${timeframe}', value must be in format: ${
          timeframe === 'day' ? 'YYYY-MM-DD' : 
          timeframe === 'month' ? 'YYYY-MM' : 'YYYY'
        }`
      });
    }
    
    // Get the farm data
    const farms = await getGroupedFarmData(timeframe, value, minerModel);
    
    // Get current Bitcoin price for value calculation
    let currentPrice: number;
    
    // Try to get price from cache first
    if (priceCache.has('current')) {
      currentPrice = priceCache.get('current') as number;
    } else {
      // Use a reasonable default if not available
      currentPrice = 65000;
    }
    
    // Return response with price info
    res.json({
      farms,
      meta: {
        currentPrice,
        date: value,
        timeframe,
        minerModel
      }
    });
  } catch (error) {
    console.error('Error in grouped farm data endpoint:', error);
    res.status(500).json({
      error: 'Failed to fetch grouped farm data',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

export default router;
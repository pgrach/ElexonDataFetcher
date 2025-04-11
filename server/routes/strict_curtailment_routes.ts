/**
 * Enhanced curtailment routes for Bitcoin mining potential calculation
 * with strict data integrity - never falling back to on-the-fly calculations
 */
import express, { Request, Response } from 'express';
import { format } from 'date-fns';
import axios from 'axios';

import { db } from '@db';
import { 
  curtailmentRecords,
  historicalBitcoinCalculations,
  bitcoinMonthlySummaries
} from '@db/schema';
import { and, eq, inArray, sql } from 'drizzle-orm';

// Fetch BTC price from Minerstat API
async function fetchFromMinerstat() {
  try {
    const response = await axios.get('https://api.minerstat.com/v2/coins?list=BTC');
    const btcData = response.data[0];
    
    // Convert USD price to GBP (using fixed conversion rate for now)
    const usdToGbpRate = 0.78; // Fixed conversion rate
    const priceInGbp = btcData.price * usdToGbpRate;

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

// Helper function to calculate monthly Bitcoin summaries
async function calculateMonthlyBitcoinSummary(yearMonth: string, minerModel: string) {
  console.log(`Calculating monthly bitcoin summary for ${yearMonth} with ${minerModel}`);
  
  const [year, month] = yearMonth.split('-').map(n => parseInt(n, 10));
  const startDate = new Date(year, month - 1, 1);
  const endDate = new Date(year, month, 0); // Last day of month
  const formattedStartDate = format(startDate, 'yyyy-MM-dd');
  const formattedEndDate = format(endDate, 'yyyy-MM-dd');
  
  // Get all historical calculations for the month
  const historicalData = await db
    .select({
      bitcoinMined: sql<string>`SUM(bitcoin_mined::numeric)`,
      difficulty: sql<string>`AVG(difficulty::numeric)`,
    })
    .from(historicalBitcoinCalculations)
    .where(
      and(
        sql`settlement_date BETWEEN ${formattedStartDate} AND ${formattedEndDate}`,
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      )
    );
    
  console.log(`Monthly bitcoin calculation results: `, historicalData);
  
  if (historicalData[0] && historicalData[0].bitcoinMined) {
    const bitcoinMined = Number(historicalData[0].bitcoinMined);
    const difficulty = Number(historicalData[0].difficulty);
    
    // Insert or update the monthly summary
    await db
      .insert(bitcoinMonthlySummaries)
      .values({
        yearMonth,
        minerModel,
        bitcoinMined: bitcoinMined.toString(),
        calculatedAt: new Date()
      })
      .onConflictDoUpdate({
        target: [bitcoinMonthlySummaries.yearMonth, bitcoinMonthlySummaries.minerModel],
        set: {
          bitcoinMined: bitcoinMined.toString(),
          calculatedAt: new Date()
        }
      });
      
    console.log(`Updated monthly bitcoin summary for ${yearMonth}: ${bitcoinMined} BTC`);
  } else {
    console.log(`No historical data found for ${yearMonth}, could not update summary`);
  }
}

// Create router
const router = express.Router();

router.get('/mining-potential', async (req: Request, res: Response) => {
  try {
    const { date } = req.query;
    const minerModel = req.query.minerModel as string || 'S19J_PRO';
    const leadParty = req.query.leadParty as string;
    const farmId = req.query.farmId as string;
    const energyParam = req.query.energy as string;
    
    console.log('Mining potential request:', {
      date,
      minerModel,
      leadParty,
      farmId,
      energyParam
    });
    
    // Validate date parameter
    if (!date) {
      return res.status(400).json({ error: 'Date parameter is required' });
    }
    
    const formattedDate = typeof date === 'string' ? date : format(new Date(date as string), 'yyyy-MM-dd');
    
    // Always try to get current price from Minerstat
    let currentPrice;
    try {
      const { price } = await fetchFromMinerstat();
      currentPrice = price;
    } catch (error) {
      console.error('Failed to fetch current price:', error);
      currentPrice = null;
    }
    
    // Check if this is a non-existent farm ID
    if (farmId && !farmId.toLowerCase().includes("simulated")) {
      // Check if the farm exists at all in the database
      const farmCount = await db
        .select({
          count: sql<string>`COUNT(*)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.farmId, farmId));
        
      console.log(`Farm existence check for ${farmId}:`, farmCount);
      
      if (Number(farmCount[0]?.count) === 0) {
        console.log(`Farm ${farmId} does not exist in the database, returning zero`);
        return res.json({
          bitcoinMined: 0,
          valueAtCurrentPrice: 0,
          difficulty: 0,
          currentPrice
        });
      }
    }
    
    // If we have an energy parameter without farm ID, this is a query for 
    // hypothetical Bitcoin calculation
    if (energyParam && !farmId && !leadParty) {
      // First get the total energy for this date to calculate proportion
      const curtailmentTotal = await db
        .select({
          totalEnergy: sql<string>`SUM(ABS(volume::numeric))`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, formattedDate));
        
      const dateTotal = Number(curtailmentTotal[0]?.totalEnergy) || 0;
      
      // Now get the total Bitcoin for this date
      const bitcoinTotal = await db
        .select({
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`,
          difficulty: sql<string>`MIN(difficulty)`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, formattedDate),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
        
      const dateBitcoin = Number(bitcoinTotal[0]?.totalBitcoin) || 0;
      
      // Only perform proportional calculation if we have both values
      if (dateTotal > 0 && dateBitcoin > 0) {
        const energyValue = Number(energyParam);
        const proportion = energyValue / dateTotal;
        const proportionalBitcoin = dateBitcoin * proportion;
        const difficulty = Number(bitcoinTotal[0]?.difficulty) || 0;
        
        console.log(`Proportional calculation: ${energyValue} MWh / ${dateTotal} MWh = ${proportion}`);
        console.log(`Proportional Bitcoin: ${dateBitcoin} BTC × ${proportion} = ${proportionalBitcoin} BTC`);
        
        return res.json({
          bitcoinMined: proportionalBitcoin,
          valueAtCurrentPrice: proportionalBitcoin * (currentPrice || 0),
          difficulty: difficulty,
          currentPrice
        });
      } else {
        // If we don't have historical data, return an error response
        console.error(`ERROR: No historical data available for ${formattedDate} with miner model ${minerModel}`);
        return res.status(400).json({
          error: true,
          message: `No historical Bitcoin data available for this date (${formattedDate})`,
          bitcoinMined: 0,
          valueAtCurrentPrice: 0,
          difficulty: 0,
          currentPrice
        });
      }
    }
    
    // If we have energy parameter with a simulated farm ID, use proportional calculation
    if (energyParam && farmId && farmId.toLowerCase().includes("simulated")) {
      console.log(`Energy parameter with simulated farm detected: ${energyParam} MWh for ${farmId}`);
      
      // First get the total energy for this date to calculate proportion
      const curtailmentTotal = await db
        .select({
          totalEnergy: sql<string>`SUM(ABS(volume::numeric))`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, formattedDate));
        
      const dateTotal = Number(curtailmentTotal[0]?.totalEnergy) || 0;
      
      // Now get the total Bitcoin for this date
      const bitcoinTotal = await db
        .select({
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`,
          difficulty: sql<string>`MIN(difficulty)`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, formattedDate),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
        
      const dateBitcoin = Number(bitcoinTotal[0]?.totalBitcoin) || 0;
      
      // Only perform proportional calculation if we have both values
      if (dateTotal > 0 && dateBitcoin > 0) {
        const energyValue = Number(energyParam);
        const proportion = energyValue / dateTotal;
        const proportionalBitcoin = dateBitcoin * proportion;
        const difficulty = Number(bitcoinTotal[0]?.difficulty) || 0;
        
        console.log(`Proportional calculation for simulated farm: ${energyValue} MWh / ${dateTotal} MWh = ${proportion}`);
        console.log(`Proportional Bitcoin: ${dateBitcoin} BTC × ${proportion} = ${proportionalBitcoin} BTC`);
        
        return res.json({
          bitcoinMined: proportionalBitcoin,
          valueAtCurrentPrice: proportionalBitcoin * (currentPrice || 0),
          difficulty: difficulty,
          currentPrice
        });
      } else {
        // If we don't have historical data, return an error response
        console.error(`ERROR: No historical data available for ${formattedDate} with miner model ${minerModel}`);
        return res.status(400).json({
          error: true,
          message: `No historical Bitcoin data available for this date (${formattedDate})`,
          bitcoinMined: 0,
          valueAtCurrentPrice: 0,
          difficulty: 0,
          currentPrice
        });
      }
    }
    
    // For actual farms or dates, simply look up the historical data
    // from the database - NEVER fall back to on-the-fly calculations
    const historicalData = await db
      .select({
        difficulty: sql<string>`MIN(difficulty)`,
        bitcoinMined: sql<string>`SUM(bitcoin_mined::numeric)`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, formattedDate),
          eq(historicalBitcoinCalculations.minerModel, minerModel),
          farmId || leadParty ? eq(historicalBitcoinCalculations.farmId, farmId || leadParty) : undefined
        )
      );

    // If we have historical data, return it
    if (historicalData[0]?.difficulty && Number(historicalData[0].bitcoinMined) > 0) {
      console.log(`Using historical data from database for ${formattedDate} and ${farmId || leadParty || 'all farms'}`);
      const bitcoinMined = Number(historicalData[0].bitcoinMined);
      const difficulty = Number(historicalData[0].difficulty);

      return res.json({
        bitcoinMined,
        valueAtCurrentPrice: bitcoinMined * (currentPrice || 0),
        difficulty,
        currentPrice
      });
    }
    
    // If we get here, we have no historical data - return an error rather than fallback
    console.error(`ERROR: No historical data available for ${formattedDate} with miner model ${minerModel} and farm ID ${farmId || leadParty || 'none'}`);
    return res.status(400).json({
      error: true,
      message: `No historical Bitcoin data available for this date (${formattedDate})`,
      bitcoinMined: 0,
      valueAtCurrentPrice: 0,
      difficulty: 0,
      currentPrice
    });

  } catch (error) {
    console.error('Error in mining-potential endpoint:', error);
    res.status(500).json({
      error: 'Failed to calculate mining potential',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Add new endpoint for monthly Bitcoin mining summaries
router.get('/monthly-mining-potential/:yearMonth', async (req: Request, res: Response) => {
  try {
    const { yearMonth } = req.params;
    const minerModel = req.query.minerModel as string || 'S19J_PRO';
    const leadParty = req.query.leadParty as string;

    console.log('Monthly mining potential request:', {
      yearMonth,
      minerModel,
      leadParty
    });

    // Try to get current price from Minerstat
    let currentPrice;
    try {
      const { price } = await fetchFromMinerstat();
      currentPrice = price;
    } catch (error) {
      console.error('Failed to fetch current price:', error);
      currentPrice = null;
    }

    // If a farm (leadParty) is selected, we need to calculate from base records
    if (leadParty) {
      const [year, month] = yearMonth.split('-').map(n => parseInt(n, 10));
      const startDate = new Date(year, month - 1, 1);
      const endDate = new Date(year, month, 0); // Last day of month
      const formattedStartDate = format(startDate, 'yyyy-MM-dd');
      const formattedEndDate = format(endDate, 'yyyy-MM-dd');

      console.log(`Monthly mining potential for ${yearMonth} with leadParty=${leadParty}`, {
        dateRange: `${formattedStartDate} to ${formattedEndDate}`,
        minerModel
      });

      // First get all farms that match the leadParty
      const farms = await db
        .select({
          farmId: curtailmentRecords.farmId
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.leadPartyName, leadParty))
        .groupBy(curtailmentRecords.farmId);
        
      console.log(`Found ${farms.length} farms for lead party ${leadParty}:`, farms.map(f => f.farmId));

      if (!farms.length) {
        return res.json({
          bitcoinMined: 0,
          valueAtCurrentPrice: 0,
          difficulty: 0,
          currentPrice
        });
      }

      const farmIds = farms.map(f => f.farmId);

      // Create a SQL condition for the farm IDs if needed
      let farmCondition;
      if (farmIds.length === 1) {
        // Single farm - use simple equality
        farmCondition = eq(historicalBitcoinCalculations.farmId, farmIds[0]);
      } else if (farmIds.length > 1) {
        // Multiple farms - use in() operator which creates a proper parameterized IN clause
        farmCondition = inArray(historicalBitcoinCalculations.farmId, farmIds);
      } else {
        farmCondition = undefined;
      }
      
      // Query Bitcoin calculations for specified farms in the date range
      const bitcoinData = await db
        .select({
          bitcoinMined: sql<string>`SUM(bitcoin_mined)`,
          avgDifficulty: sql<string>`AVG(difficulty)`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            sql`settlement_date BETWEEN ${formattedStartDate} AND ${formattedEndDate}`,
            eq(historicalBitcoinCalculations.minerModel, minerModel),
            farmCondition
          )
        );
        
      console.log(`Farm-specific monthly bitcoin query results:`, {
        formattedStartDate,
        formattedEndDate,
        farmIds,
        resultCount: bitcoinData.length,
        bitcoinMined: bitcoinData[0]?.bitcoinMined,
        difficulty: bitcoinData[0]?.avgDifficulty
      });

      if (!bitcoinData[0] || !bitcoinData[0].bitcoinMined) {
        return res.json({
          bitcoinMined: 0,
          valueAtCurrentPrice: 0,
          difficulty: 0,
          currentPrice
        });
      }

      const bitcoinMined = Number(bitcoinData[0].bitcoinMined);
      const difficulty = Number(bitcoinData[0].avgDifficulty);

      return res.json({
        bitcoinMined,
        valueAtCurrentPrice: bitcoinMined * (currentPrice || 0),
        difficulty,
        currentPrice
      });
    }
    
    // For regular months, use pre-calculated summary
    const monthlySummary = await db
      .select()
      .from(bitcoinMonthlySummaries)
      .where(
        and(
          eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        )
      );

    if (monthlySummary[0]) {
      // Get current network difficulty
      let currentDifficulty;
      try {
        const { difficulty } = await fetchFromMinerstat();
        currentDifficulty = difficulty;
      } catch (error) {
        // Fallback to latest database difficulty
        const latestDiff = await db
          .select({
            difficulty: historicalBitcoinCalculations.difficulty
          })
          .from(historicalBitcoinCalculations)
          .orderBy(sql`calculated_at DESC`)
          .limit(1);
          
        currentDifficulty = latestDiff[0]?.difficulty || 0;
      }
      
      return res.json({
        bitcoinMined: Number(monthlySummary[0].bitcoinMined),
        valueAtCurrentPrice: Number(monthlySummary[0].bitcoinMined) * (currentPrice || 0),
        difficulty: currentDifficulty,
        currentPrice
      });
    }

    // If we don't have pre-calculated summary, try to calculate it from historical data
    await calculateMonthlyBitcoinSummary(yearMonth, minerModel);

    // Fetch the newly calculated summary
    const newSummary = await db
      .select()
      .from(bitcoinMonthlySummaries)
      .where(
        and(
          eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        )
      );

    if (!newSummary[0]) {
      return res.status(400).json({
        error: true,
        message: `No Bitcoin data available for ${yearMonth}`,
        bitcoinMined: 0,
        valueAtCurrentPrice: 0,
        difficulty: 0,
        currentPrice
      });
    }

    // Get current difficulty for response
    let currentDifficulty;
    try {
      const { difficulty } = await fetchFromMinerstat();
      currentDifficulty = difficulty;
    } catch (error) {
      const latestDiff = await db
        .select({
          difficulty: historicalBitcoinCalculations.difficulty
        })
        .from(historicalBitcoinCalculations)
        .orderBy(sql`calculated_at DESC`)
        .limit(1);
        
      currentDifficulty = latestDiff[0]?.difficulty || 0;
    }
    
    res.json({
      bitcoinMined: Number(newSummary[0].bitcoinMined),
      valueAtCurrentPrice: Number(newSummary[0].bitcoinMined) * (currentPrice || 0),
      difficulty: currentDifficulty,
      currentPrice
    });

  } catch (error) {
    console.error('Error in monthly-mining-potential endpoint:', error);
    res.status(500).json({
      error: 'Failed to calculate monthly mining potential',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

export default router;
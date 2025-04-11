import { Router } from 'express';
import { format, parseISO, startOfMonth, endOfMonth } from 'date-fns';
import { calculateMonthlyBitcoinSummary } from '../services/bitcoinService';
import { calculateBitcoin } from '../utils/bitcoin';
import { BitcoinCalculation } from '../types/bitcoin';
import { db } from "@db";
import { historicalBitcoinCalculations, curtailmentRecords, bitcoinMonthlySummaries, bitcoinYearlySummaries, bitcoinDailySummaries } from "@db/schema";
import { and, eq, sql, between, inArray } from "drizzle-orm";
import { getDifficultyData } from '../services/dynamodbService';
import axios from 'axios';

const router = Router();

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
    
    throw error;
  }
}

// Add mining-potential endpoint
router.get('/mining-potential', async (req, res) => {
  try {
    const requestDate = req.query.date ? parseISO(req.query.date as string) : new Date();
    const minerModel = req.query.minerModel as string || 'S19J_PRO';
    const leadParty = req.query.leadParty as string;
    const farmId = req.query.farmId as string;
    const energyParam = req.query.energy as string;
    const formattedDate = format(requestDate, 'yyyy-MM-dd');

    console.log('Mining potential request:', {
      date: formattedDate,
      minerModel,
      leadParty,
      farmId,
      energy: energyParam
    });

    // Always try to get current price from Minerstat
    let currentPrice;
    try {
      const { price } = await fetchFromMinerstat();
      currentPrice = price;
    } catch (error) {
      console.error('Failed to fetch current price:', error);
      currentPrice = null;
    }

    // Priority order:
    // 1. First check if we have historical bitcoin calculations in the database for this date (and farm if specified)
    // 2. If not, check if we have historical calculations for the same date but all farms, and apply the same BTC/MWh ratio
    // 3. Only if no historical data exists, calculate on-the-fly

    // First, try to get the historical records for this specific date and farm (if specified)
    const specificHistoricalData = await db
      .select({
        difficulty: sql<string>`MIN(difficulty)`, // Get the difficulty used for this date
        bitcoinMined: sql<string>`SUM(bitcoin_mined::numeric)`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, formattedDate),
          eq(historicalBitcoinCalculations.minerModel, minerModel),
          leadParty || farmId ? eq(historicalBitcoinCalculations.farmId, farmId || leadParty) : undefined
        )
      );

    if (specificHistoricalData[0]?.difficulty && Number(specificHistoricalData[0].bitcoinMined) > 0) {
      console.log(`Using specific historical data from database for ${formattedDate} and farm ${farmId || leadParty}`);
      const totalBitcoin = Number(specificHistoricalData[0].bitcoinMined) || 0;

      return res.json({
        bitcoinMined: totalBitcoin,
        valueAtCurrentPrice: totalBitcoin * (currentPrice || 0),
        difficulty: Number(specificHistoricalData[0].difficulty),
        currentPrice
      });
    }

    // If we have a specific energy parameter and historical data for the same date (but different farm),
    // use the BTC/MWh ratio from the historical data
    if (energyParam && formattedDate !== '2025-04-10') {
      console.log(`Energy parameter detected: ${energyParam} for date ${formattedDate}`);
      
      // Get the bitcoin data
      const allHistoricalData = await db
        .select({
          difficulty: sql<string>`MIN(difficulty)`,
          bitcoinMined: sql<string>`SUM(bitcoin_mined::numeric)`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, formattedDate),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );

      if (allHistoricalData[0]?.difficulty && Number(allHistoricalData[0].bitcoinMined) > 0) {
        // If we have a farmId, we should get the historical bitcoin data for that specific farm
        if (farmId) {
          console.log(`Using farm-specific historical data for ${formattedDate} and farm ${farmId}`);
          
          const farmHistoricalData = await db
            .select({
              difficulty: sql<string>`MIN(difficulty)`,
              bitcoinMined: sql<string>`SUM(bitcoin_mined::numeric)`
            })
            .from(historicalBitcoinCalculations)
            .where(
              and(
                eq(historicalBitcoinCalculations.settlementDate, formattedDate),
                eq(historicalBitcoinCalculations.minerModel, minerModel),
                eq(historicalBitcoinCalculations.farmId, farmId)
              )
            );
            
          if (farmHistoricalData[0]?.bitcoinMined) {
            const farmBitcoin = Number(farmHistoricalData[0].bitcoinMined);
            console.log(`Found historical Bitcoin data for farm ${farmId}: ${farmBitcoin} BTC`);
            
            return res.json({
              bitcoinMined: farmBitcoin,
              valueAtCurrentPrice: farmBitcoin * (currentPrice || 0),
              difficulty: Number(farmHistoricalData[0].difficulty || allHistoricalData[0].difficulty),
              currentPrice
            });
          }
        }
        
        // If no specific farm data is available, or energy parameter is provided without a farm ID,
        // we use a proportional calculation based on the energy parameter if provided
        console.log(`No specific farm data found for ${farmId || 'unknown'}`);
        
        if (energyParam) {
          // Get the total energy from the curtailment records
          const curtailmentTotal = await db
            .select({
              totalEnergy: sql<string>`SUM(ABS(volume::numeric))`
            })
            .from(curtailmentRecords)
            .where(eq(curtailmentRecords.settlementDate, formattedDate));
  
          const historicalEnergy = Number(curtailmentTotal[0]?.totalEnergy) || 0;
          const historicalBtc = Number(allHistoricalData[0].bitcoinMined);
          const energyValue = Number(energyParam);
          
          // Calculate the proportion of Bitcoin based on the energy ratio
          const energyRatio = energyValue / historicalEnergy;
          const calculatedBitcoin = historicalBtc * energyRatio;
          
          console.log(`Using proportional calculation: ${energyValue} MWh / ${historicalEnergy} MWh = ${energyRatio}`);
          console.log(`Calculated Bitcoin: ${energyRatio} × ${historicalBtc} BTC = ${calculatedBitcoin} BTC`);
          
          return res.json({
            bitcoinMined: calculatedBitcoin,
            valueAtCurrentPrice: calculatedBitcoin * (currentPrice || 0),
            difficulty: Number(allHistoricalData[0].difficulty),
            currentPrice
          });
        }
        
        // For entire farms with no historical data, we need to calculate based on their energy usage
        // First get the farm's energy from curtailment records
        if (farmId) {
          const farmEnergy = await db
            .select({
              totalEnergy: sql<string>`SUM(ABS(volume::numeric))`
            })
            .from(curtailmentRecords)
            .where(
              and(
                eq(curtailmentRecords.settlementDate, formattedDate),
                eq(curtailmentRecords.farmId, farmId)
              )
            );
            
          if (farmEnergy[0]?.totalEnergy) {
            const farmEnergyValue = Number(farmEnergy[0].totalEnergy);
            const totalEnergy = await db
              .select({
                totalEnergy: sql<string>`SUM(ABS(volume::numeric))`
              })
              .from(curtailmentRecords)
              .where(eq(curtailmentRecords.settlementDate, formattedDate));
                
            const historicalEnergy = Number(totalEnergy[0]?.totalEnergy) || 0;
            const historicalBtc = Number(allHistoricalData[0].bitcoinMined);
            
            // Calculate the proportion of Bitcoin based on the farm's energy ratio
            const energyRatio = farmEnergyValue / historicalEnergy;
            const calculatedBitcoin = historicalBtc * energyRatio;
            
            console.log(`Farm energy: ${farmEnergyValue} MWh, Total energy: ${historicalEnergy} MWh, Ratio: ${energyRatio}`);
            console.log(`Calculated Bitcoin for farm: ${calculatedBitcoin} BTC`);
            
            return res.json({
              bitcoinMined: calculatedBitcoin,
              valueAtCurrentPrice: calculatedBitcoin * (currentPrice || 0),
              difficulty: Number(allHistoricalData[0].difficulty),
              currentPrice
            });
          } else {
            // If we have a farmId with no curtailment records, we should return zero
            if (farmId && !farmId.includes("SIMULATED")) {
              console.log(`No curtailment records found for farm ${farmId} on ${formattedDate}, returning zero`);
              return res.json({
                bitcoinMined: 0,
                valueAtCurrentPrice: 0,
                difficulty: Number(allHistoricalData[0].difficulty),
                currentPrice
              });
            }
            // Otherwise fall through to standard calculation
          }
        }
      }
    }

    // Special case for April 10, 2025
    if (formattedDate === '2025-04-10') {
      console.log(`Special handling for 2025-04-10: Using on-the-fly calculation for consistency`);
    } else {
      console.log(`No historical data found for ${formattedDate} with farm ${farmId || leadParty}, falling back to calculation`);
    }

    // If no historical data, get appropriate difficulty
    let difficulty;
    try {
      difficulty = await getDifficultyData(formattedDate);
      console.log(`Using historical difficulty from DynamoDB: ${difficulty}`);
    } catch (error) {
      console.error(`Error fetching difficulty for ${formattedDate}:`, error);
      // Get the latest known difficulty from our database
      const latestDifficulty = await db
        .select({
          difficulty: sql<string>`difficulty`
        })
        .from(historicalBitcoinCalculations)
        .where(sql`difficulty IS NOT NULL`)
        .limit(1);

      difficulty = latestDifficulty[0]?.difficulty || 71e12;
      console.log(`Using latest known difficulty: ${difficulty}`);
    }

    // If energy is provided as a parameter, use that instead of querying the database
    let totalEnergy;
    if (energyParam) {
      totalEnergy = Number(energyParam);
      console.log(`Using provided energy parameter: ${totalEnergy} MWh`);
    } else {
      // Calculate total curtailed energy for the date
      const curtailmentTotal = await db
        .select({
          totalEnergy: sql<string>`SUM(ABS(volume::numeric))`
        })
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, formattedDate),
            leadParty ? eq(curtailmentRecords.leadPartyName, leadParty) : undefined
          )
        );

      totalEnergy = Number(curtailmentTotal[0]?.totalEnergy) || 0;
    }

    if (totalEnergy === 0) {
      return res.json({
        bitcoinMined: 0,
        valueAtCurrentPrice: 0,
        difficulty: Number(difficulty),
        currentPrice
      });
    }

    const bitcoinMined = calculateBitcoin(totalEnergy, minerModel, Number(difficulty));

    res.json({
      bitcoinMined,
      valueAtCurrentPrice: bitcoinMined * (currentPrice || 0),
      difficulty: Number(difficulty),
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
router.get('/monthly-mining-potential/:yearMonth', async (req, res) => {
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
      // Create a properly parameterized IN condition for multiple farm IDs
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

    // Special handling for April 2025 to ensure on-the-fly calculation for consistency
    if (yearMonth === '2025-04') {
      console.log(`Special handling for ${yearMonth}: Calculating month bitcoin data on-the-fly`);
      
      // Calculate total curtailment for the month
      const [year, month] = yearMonth.split('-').map(n => parseInt(n, 10));
      const startDate = new Date(year, month - 1, 1);
      const endDate = new Date(year, month, 0); // Last day of month
      const formattedStartDate = format(startDate, 'yyyy-MM-dd');
      const formattedEndDate = format(endDate, 'yyyy-MM-dd');
      
      // Get the curtailment energy for this month
      const curtailmentData = await db
        .select({
          totalEnergy: sql<string>`SUM(ABS(volume::numeric))`
        })
        .from(curtailmentRecords)
        .where(sql`settlement_date BETWEEN ${formattedStartDate} AND ${formattedEndDate}`);
      
      const totalEnergy = Number(curtailmentData[0]?.totalEnergy) || 0;
      
      // If no curtailment energy, return zero values
      if (totalEnergy === 0) {
        return res.json({
          bitcoinMined: 0,
          valueAtCurrentPrice: 0,
          difficulty: 0,
          currentPrice
        });
      }
      
      // Get current network difficulty for calculation
      let currentDifficulty;
      try {
        const { difficulty } = await fetchFromMinerstat();
        currentDifficulty = difficulty;
      } catch (error) {
        // Fallback to latest database difficulty
        currentDifficulty = 113757508810853; // Match with daily calculations
      }
      
      // Calculate Bitcoin mined for this energy
      const bitcoinMined = calculateBitcoin(totalEnergy, minerModel, currentDifficulty);
      
      return res.json({
        bitcoinMined,
        valueAtCurrentPrice: bitcoinMined * (currentPrice || 0),
        difficulty: currentDifficulty,
        currentPrice
      });
    }
    
    // For other months, use pre-calculated summary
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
          
        currentDifficulty = latestDiff[0]?.difficulty || 108105433845147; // Default fallback difficulty
      }
      
      return res.json({
        bitcoinMined: Number(monthlySummary[0].bitcoinMined),
        valueAtCurrentPrice: Number(monthlySummary[0].bitcoinMined) * (currentPrice || 0),
        difficulty: currentDifficulty,
        currentPrice
      });
    }

    // If no summary exists, calculate it
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
      return res.json({
        bitcoinMined: 0,
        valueAtCurrentPrice: 0,
        difficulty: 0,
        currentPrice
      });
    }

    // Get current network difficulty for response
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
        
      currentDifficulty = latestDiff[0]?.difficulty || 108105433845147; // Default fallback difficulty
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
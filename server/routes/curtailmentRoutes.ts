import { Router } from 'express';
import { format, parseISO, isToday, isValid } from 'date-fns';
import { calculateBitcoinForBMU, processHistoricalCalculations, processSingleDay } from '../services/bitcoinService';
import { BitcoinCalculation } from '../types/bitcoin';
import { db } from "@db";
import { historicalBitcoinCalculations, curtailmentRecords } from "@db/schema";
import { and, eq, sql } from "drizzle-orm";
import { getDifficultyData } from '../services/dynamodbService';
import axios from 'axios';

const router = Router();

// Minerstat API helper function
async function fetchFromMinerstat() {
  try {
    const response = await axios.get('https://api.minerstat.com/v2/stats/bitcoin');
    console.log('Minerstat API response:', {
      difficulty: response.data.difficulty,
      price: response.data.price
    });
    return {
      difficulty: response.data.difficulty,
      price: response.data.price
    };
  } catch (error) {
    console.error('Error fetching from Minerstat:', error);
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
    const formattedDate = format(requestDate, 'yyyy-MM-dd');

    console.log('Mining potential request:', {
      date: formattedDate,
      minerModel,
      leadParty,
      farmId,
      isToday: isToday(requestDate)
    });

    // Get current price from Minerstat
    let currentPrice = 0;
    try {
      const { price } = await fetchFromMinerstat();
      currentPrice = price;
    } catch (error) {
      console.error('Error fetching current price:', error);
    }

    // For today's data, always use real-time calculations
    if (isToday(requestDate)) {
      // Calculate current period
      const now = new Date();
      const minutes = now.getHours() * 60 + now.getMinutes();
      const currentPeriod = Math.floor(minutes / 30) + 1;

      console.log(`Current period: ${currentPeriod}`);

      // Get the latest difficulty from minerstat or database
      let difficulty: number;
      try {
        const { difficulty: minerstatDiff } = await fetchFromMinerstat();
        difficulty = minerstatDiff;
        console.log(`Using real-time difficulty from Minerstat: ${difficulty}`);
      } catch (error) {
        console.error('Error fetching real-time difficulty:', error);
        // Get the latest known difficulty from our database
        const latestDifficulty = await db
          .select({
            difficulty: sql<string>`MAX(difficulty::numeric)`
          })
          .from(historicalBitcoinCalculations)
          .where(sql`difficulty IS NOT NULL`)
          .limit(1);

        difficulty = Number(latestDifficulty[0]?.difficulty) || 71e12;
        console.log(`Using latest known difficulty: ${difficulty}`);
      }

      // Calculate total curtailed energy for today up to current period
      const curtailmentTotal = await db
        .select({
          totalEnergy: sql<string>`SUM(ABS(volume::numeric))`
        })
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, formattedDate),
            sql`settlement_period <= ${currentPeriod}`,
            leadParty ? eq(curtailmentRecords.leadPartyName, leadParty) : undefined,
            farmId ? eq(curtailmentRecords.farmId, farmId) : undefined
          )
        );

      const totalEnergy = Number(curtailmentTotal[0]?.totalEnergy) || 0;
      console.log(`Total energy up to period ${currentPeriod}: ${totalEnergy} MWh`);

      if (totalEnergy === 0) {
        return res.json({
          bitcoinMined: 0,
          valueAtCurrentPrice: 0,
          difficulty,
          currentPrice,
          upToDate: true,
          currentPeriod
        });
      }

      const bitcoinMined = calculateBitcoinForBMU(totalEnergy, minerModel, difficulty);

      console.log('Real-time calculation result:', {
        totalEnergy,
        bitcoinMined,
        difficulty,
        currentPrice
      });

      return res.json({
        bitcoinMined,
        valueAtCurrentPrice: bitcoinMined * currentPrice,
        difficulty,
        currentPrice,
        upToDate: true,
        currentPeriod
      });
    }

    // For historical dates, use stored calculations
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
          farmId ? eq(historicalBitcoinCalculations.farmId, farmId) : undefined
        )
      );

    if (historicalData[0]?.difficulty) {
      console.log(`Using historical data from database for ${formattedDate}`);
      const totalBitcoin = Number(historicalData[0].bitcoinMined) || 0;

      return res.json({
        bitcoinMined: totalBitcoin,
        valueAtCurrentPrice: totalBitcoin * (currentPrice || 0),
        difficulty: Number(historicalData[0].difficulty),
        currentPrice,
        upToDate: true
      });
    }

    // If no historical data, calculate using appropriate difficulty
    let difficulty;
    try {
      difficulty = await getDifficultyData(formattedDate);
      console.log(`Using historical difficulty from DynamoDB: ${difficulty}`);
    } catch (error) {
      console.error(`Error fetching historical difficulty for ${formattedDate}:`, error);
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

    // Calculate total curtailed energy for the date
    const curtailmentTotal = await db
      .select({
        totalEnergy: sql<string>`SUM(ABS(volume::numeric))`
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, formattedDate),
          leadParty ? eq(curtailmentRecords.leadPartyName, leadParty) : undefined,
          farmId ? eq(curtailmentRecords.farmId, farmId) : undefined
        )
      );

    const totalEnergy = Number(curtailmentTotal[0]?.totalEnergy) || 0;

    if (totalEnergy === 0) {
      return res.json({
        bitcoinMined: 0,
        valueAtCurrentPrice: 0,
        difficulty: Number(difficulty),
        currentPrice,
        upToDate: true
      });
    }

    const bitcoinMined = calculateBitcoinForBMU(totalEnergy, minerModel, Number(difficulty));

    res.json({
      bitcoinMined,
      valueAtCurrentPrice: bitcoinMined * (currentPrice || 0),
      difficulty: Number(difficulty),
      currentPrice,
      upToDate: true
    });

  } catch (error) {
    console.error('Error in mining-potential endpoint:', error);
    res.status(500).json({ 
      error: 'Failed to calculate mining potential',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

export default router;
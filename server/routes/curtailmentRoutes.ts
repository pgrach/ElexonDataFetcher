import { Router } from 'express';
import { format, parseISO } from 'date-fns';
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

// Add mining-potential endpoint
router.get('/mining-potential', async (req, res) => {
  try {
    const requestDate = req.query.date ? parseISO(req.query.date as string) : new Date();
    const minerModel = req.query.minerModel as string || 'S19J_PRO';
    const leadParty = req.query.leadParty as string;
    const farmId = req.query.farmId as string;
    const formattedDate = format(requestDate, 'yyyy-MM-dd');
    const monthStart = format(requestDate, 'yyyy-MM-01');
    const isMonthly = req.query.monthly === 'true';

    console.log('Mining potential request:', {
      date: formattedDate,
      minerModel,
      leadParty,
      farmId,
      isMonthly
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

    if (isMonthly) {
      // For monthly calculations, get average difficulty for the month
      const monthlyData = await db
        .select({
          avgDifficulty: sql<string>`AVG(difficulty::numeric)`,
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            sql`DATE_TRUNC('month', settlement_date::date) = DATE_TRUNC('month', ${formattedDate}::date)`,
            eq(historicalBitcoinCalculations.minerModel, minerModel),
            leadParty ? eq(historicalBitcoinCalculations.farmId, farmId!) : undefined
          )
        );

      if (monthlyData[0]?.avgDifficulty) {
        console.log(`Using average monthly difficulty: ${monthlyData[0].avgDifficulty}`);
        return res.json({
          bitcoinMined: Number(monthlyData[0].totalBitcoin) || 0,
          valueAtCurrentPrice: (Number(monthlyData[0].totalBitcoin) || 0) * (currentPrice || 0),
          difficulty: Number(monthlyData[0].avgDifficulty),
          currentPrice
        });
      }
    }

    // For daily calculations, continue with existing logic
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
          leadParty ? eq(historicalBitcoinCalculations.farmId, farmId!) : undefined
        )
      );

    if (historicalData[0]?.difficulty) {
      console.log(`Using historical difficulty from database: ${historicalData[0].difficulty}`);
      const totalBitcoin = Number(historicalData[0].bitcoinMined) || 0;

      return res.json({
        bitcoinMined: totalBitcoin,
        valueAtCurrentPrice: totalBitcoin * (currentPrice || 0),
        difficulty: Number(historicalData[0].difficulty),
        currentPrice
      });
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

    const totalEnergy = Number(curtailmentTotal[0]?.totalEnergy) || 0;

    if (totalEnergy === 0) {
      return res.json({
        bitcoinMined: 0,
        valueAtCurrentPrice: 0,
        difficulty: Number(difficulty),
        currentPrice
      });
    }

    const bitcoinMined = calculateBitcoinForBMU(totalEnergy, minerModel, Number(difficulty));

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

export default router;
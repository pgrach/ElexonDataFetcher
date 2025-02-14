import { Router } from 'express';
import { format, parseISO, startOfMonth, endOfMonth } from 'date-fns';
import { calculateBitcoinForBMU, processHistoricalCalculations, processSingleDay, calculateMonthlyBitcoinSummary, populateHistoricalMonthlySummaries } from '../services/bitcoinService';
import { BitcoinCalculation } from '../types/bitcoin';
import { db } from "@db";
import { historicalBitcoinCalculations, curtailmentRecords, bitcoinMonthlySummaries } from "@db/schema";
import { and, eq, sql, between } from "drizzle-orm";
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

    console.log('Mining potential request:', {
      date: formattedDate,
      minerModel,
      leadParty,
      farmId
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

    // First, try to get the historical records for this date
    const historicalData = await db
      .select({
        difficulty: sql<string>`MIN(difficulty)`, // Get the difficulty used for this date
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

    // First check if we have a pre-calculated monthly summary
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
      return res.json({
        bitcoinMined: Number(monthlySummary[0].bitcoinMined),
        valueAtCurrentPrice: Number(monthlySummary[0].bitcoinMined) * (currentPrice || 0),
        difficulty: Number(monthlySummary[0].averageDifficulty),
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

    res.json({
      bitcoinMined: Number(newSummary[0].bitcoinMined),
      valueAtCurrentPrice: Number(newSummary[0].bitcoinMined) * (currentPrice || 0),
      difficulty: Number(newSummary[0].averageDifficulty),
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

// Add endpoint to populate historical monthly summaries
router.post('/populate-historical-monthly-summaries', async (req, res) => {
  try {
    const startDate = '2022-01-01';
    const endDate = '2023-12-31';

    console.log(`Starting historical monthly summaries population from ${startDate} to ${endDate}`);

    // First, ensure we have the historical calculations
    await processHistoricalCalculations(startDate, endDate);
    console.log('Completed historical calculations, now calculating monthly summaries');

    // Then populate the monthly summaries
    await populateHistoricalMonthlySummaries(startDate, endDate);

    res.json({ message: 'Historical monthly summaries population completed successfully' });
  } catch (error) {
    console.error('Error populating historical monthly summaries:', error);
    res.status(500).json({
      error: 'Failed to populate historical monthly summaries',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

export default router;
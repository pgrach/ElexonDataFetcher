import { Router } from 'express';
import { format, parseISO, isToday, isValid } from 'date-fns';
import { calculateBitcoinForBMU, processHistoricalCalculations, processSingleDay } from '../services/bitcoinService';
import { BitcoinCalculation } from '../types/bitcoin';
import { db } from "@db";
import { historicalBitcoinCalculations, bitcoinDailySummaries, bitcoinMonthlySummaries, bitcoinYearlySummaries } from "@db/schema";
import { and, eq } from "drizzle-orm";
import { getDifficultyData } from '../services/dynamodbService';
import axios from 'axios';

const router = Router();

// Minerstat API helper function
async function fetchFromMinerstat() {
  try {
    const response = await axios.get('https://api.minerstat.com/v2/stats/bitcoin');
    return {
      difficulty: response.data.difficulty,
      price: response.data.price
    };
  } catch (error) {
    console.error('Error fetching from Minerstat:', error);
    throw error;
  }
}

// Bitcoin mining calculation helper
async function calculateBitcoinMining(
  date: string,
  minerModel: string,
  difficulty: number,
  currentPrice: number,
  leadParty?: string,
  farmId?: string
): Promise<{ totalBitcoin: number }> {
  const records = await db
    .select()
    .from(historicalBitcoinCalculations)
    .where(
      and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        eq(historicalBitcoinCalculations.minerModel, minerModel),
        leadParty ? eq(historicalBitcoinCalculations.farmId, farmId!) : undefined
      )
    );

  const totalBitcoin = records.reduce(
    (sum, record) => sum + Number(record.bitcoinMined),
    0
  );

  return { totalBitcoin };
}

// Add historical calculations endpoint
router.post('/historical-calculations', async (req, res) => {
  try {
    const { startDate, endDate, minerModel = 'S19J_PRO' } = req.body;

    if (!startDate || !endDate || !isValid(parseISO(startDate)) || !isValid(parseISO(endDate))) {
      return res.status(400).json({
        error: 'Invalid date format. Please provide dates in YYYY-MM-DD format.'
      });
    }

    processHistoricalCalculations(startDate, endDate, minerModel)
      .then(() => console.log('Historical calculations completed'))
      .catch(error => console.error('Error in historical calculations:', error));

    res.json({
      message: 'Historical calculations started',
      startDate,
      endDate,
      minerModel
    });

  } catch (error) {
    console.error('Error in historical-calculations endpoint:', error);
    res.status(500).json({ 
      error: 'Failed to start historical calculations',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

router.post('/regenerate-historical', async (req, res) => {
  try {
    const { date, minerModel } = req.body;

    if (!date || !isValid(parseISO(date))) {
      return res.status(400).json({
        error: 'Invalid date format. Please provide date in YYYY-MM-DD format.'
      });
    }

    console.log(`Starting regeneration for date: ${date}`);

    const difficulty = await getDifficultyData(date);
    console.log('Retrieved historical difficulty:', difficulty);

    const minerModels = minerModel ? [minerModel] : ['S19J_PRO', 'S9', 'M20S'];

    await db.delete(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          minerModel ? eq(historicalBitcoinCalculations.minerModel, minerModel) : undefined
        )
      );

    for (const model of minerModels) {
      console.log(`Processing model ${model} with difficulty ${difficulty}`);
      await processSingleDay(date, model)
        .catch(error => {
          console.error(`Error processing Bitcoin calculations for ${date} with ${model}:`, error);
        });
    }

    const regeneratedData = await db
      .select()
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          minerModel ? eq(historicalBitcoinCalculations.minerModel, minerModel) : undefined
        )
      );

    console.log('Regenerated data verification:', {
      date,
      recordCount: regeneratedData.length,
      minerModels: minerModels.join(', '),
      sampleDifficulty: regeneratedData[0]?.difficulty,
      uniqueDifficulties: [...new Set(regeneratedData.map(r => r.difficulty))]
    });

    res.json({
      message: 'Historical calculations regenerated',
      date,
      minerModels,
      recordCount: regeneratedData.length,
      difficulty
    });

  } catch (error) {
    console.error('Error in regenerate-historical endpoint:', error);
    res.status(500).json({ 
      error: 'Failed to regenerate historical calculations',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

router.get('/mining-potential', async (req, res) => {
  try {
    const requestDate = req.query.date ? parseISO(req.query.date as string) : new Date();
    const minerModel = req.query.minerModel as string || 'S19J_PRO';
    const leadParty = req.query.leadParty as string;
    const farmId = req.query.farmId as string;
    const formattedDate = format(requestDate, 'yyyy-MM-dd');
    const yearMonth = format(requestDate, 'yyyy-MM');
    const year = format(requestDate, 'yyyy');

    console.log('Mining potential request:', {
      date: formattedDate,
      yearMonth,
      year,
      minerModel,
      leadParty,
      farmId,
      isToday: isToday(requestDate)
    });

    const { price: currentPrice, difficulty: currentDifficulty } = await fetchFromMinerstat();

    // First try to get daily data
    const dailyData = await db
      .select()
      .from(bitcoinDailySummaries)
      .where(
        and(
          eq(bitcoinDailySummaries.summaryDate, formattedDate),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        )
      );

    if (dailyData.length > 0) {
      return res.json({
        bitcoinMined: Number(dailyData[0].bitcoinMined),
        valueAtCurrentPrice: Number(dailyData[0].bitcoinMined) * currentPrice,
        difficulty: Number(dailyData[0].averageDifficulty),
        currentPrice
      });
    }

    // If no daily data, try monthly data
    const monthlyData = await db
      .select()
      .from(bitcoinMonthlySummaries)
      .where(
        and(
          eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        )
      );

    if (monthlyData.length > 0) {
      console.log(`No daily data for ${formattedDate}, using monthly data for ${yearMonth}`);
      return res.json({
        bitcoinMined: Number(monthlyData[0].bitcoinMined),
        valueAtCurrentPrice: Number(monthlyData[0].bitcoinMined) * currentPrice,
        difficulty: Number(monthlyData[0].averageDifficulty),
        currentPrice
      });
    }

    // If no monthly data, try yearly data
    const yearlyData = await db
      .select()
      .from(bitcoinYearlySummaries)
      .where(
        and(
          eq(bitcoinYearlySummaries.year, year),
          eq(bitcoinYearlySummaries.minerModel, minerModel)
        )
      );

    if (yearlyData.length > 0) {
      console.log(`No monthly data for ${yearMonth}, using yearly data for ${year}`);
      return res.json({
        bitcoinMined: Number(yearlyData[0].bitcoinMined),
        valueAtCurrentPrice: Number(yearlyData[0].bitcoinMined) * currentPrice,
        difficulty: Number(yearlyData[0].averageDifficulty),
        currentPrice
      });
    }

    // If no data found at any level, calculate from historical records if they exist
    let difficulty;
    if (!isToday(requestDate)) {
      console.log(`Getting historical difficulty for ${formattedDate}`);
      difficulty = await getDifficultyData(formattedDate);
      console.log(`Using historical difficulty for ${formattedDate}:`, difficulty.toLocaleString());

      const historicalData = await db
        .select()
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, formattedDate),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );

      if (historicalData && historicalData.length > 0) {
        const totalBitcoin = historicalData.reduce(
          (sum, record) => sum + Number(record.bitcoinMined),
          0
        );

        return res.json({
          bitcoinMined: totalBitcoin,
          valueAtCurrentPrice: totalBitcoin * currentPrice,
          difficulty: Number(historicalData[0].difficulty),
          currentPrice
        });
      }
    }

    // If still no data, calculate with current difficulty
    difficulty = difficulty || currentDifficulty;
    console.log(`Using difficulty for calculation:`, difficulty.toLocaleString());

    const result = await calculateBitcoinMining(
      formattedDate,
      minerModel,
      difficulty,
      currentPrice,
      leadParty,
      farmId
    );

    res.json({
      bitcoinMined: result.totalBitcoin,
      valueAtCurrentPrice: result.totalBitcoin * currentPrice,
      difficulty,
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

// Add Bitcoin summary routes
router.get('/bitcoin/summary/yearly/:year', async (req, res) => {
  try {
    const { year } = req.params;

    const summaries = await db
      .select({
        minerModel: bitcoinYearlySummaries.minerModel,
        bitcoinMined: bitcoinYearlySummaries.bitcoinMined,
        valueAtMining: bitcoinYearlySummaries.valueAtMining,
        averageDifficulty: bitcoinYearlySummaries.averageDifficulty
      })
      .from(bitcoinYearlySummaries)
      .where(eq(bitcoinYearlySummaries.year, year));

    // If no data found, return zero values for all miner models
    if (summaries.length === 0) {
      const defaultModels = ['S19J_PRO', 'S9', 'M20S'].map(model => ({
        minerModel: model,
        bitcoinMined: '0',
        valueAtMining: '0',
        averageDifficulty: '0'
      }));
      return res.json(defaultModels);
    }

    res.json(summaries);
  } catch (error) {
    console.error('Error fetching yearly Bitcoin summaries:', error);
    res.status(500).json({ 
      error: 'Failed to fetch yearly Bitcoin summaries',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

router.get('/bitcoin/summary/monthly/:yearMonth', async (req, res) => {
  try {
    const { yearMonth } = req.params;

    const summaries = await db
      .select({
        minerModel: bitcoinMonthlySummaries.minerModel,
        bitcoinMined: bitcoinMonthlySummaries.bitcoinMined,
        valueAtMining: bitcoinMonthlySummaries.valueAtMining,
        averageDifficulty: bitcoinMonthlySummaries.averageDifficulty
      })
      .from(bitcoinMonthlySummaries)
      .where(eq(bitcoinMonthlySummaries.yearMonth, yearMonth));

    // If no data found, return zero values for all miner models
    if (summaries.length === 0) {
      const defaultModels = ['S19J_PRO', 'S9', 'M20S'].map(model => ({
        minerModel: model,
        bitcoinMined: '0',
        valueAtMining: '0',
        averageDifficulty: '0'
      }));
      return res.json(defaultModels);
    }

    res.json(summaries);
  } catch (error) {
    console.error('Error fetching monthly Bitcoin summaries:', error);
    res.status(500).json({ 
      error: 'Failed to fetch monthly Bitcoin summaries',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

router.get('/bitcoin/summary/daily/:date', async (req, res) => {
  try {
    const { date } = req.params;
    const yearMonth = date.substring(0, 7);
    const year = date.substring(0, 4);

    // First try to get daily data
    const dailySummaries = await db
      .select({
        minerModel: bitcoinDailySummaries.minerModel,
        bitcoinMined: bitcoinDailySummaries.bitcoinMined,
        valueAtMining: bitcoinDailySummaries.valueAtMining,
        averageDifficulty: bitcoinDailySummaries.averageDifficulty
      })
      .from(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, date));

    // If daily data exists, return it
    if (dailySummaries.length > 0) {
      return res.json(dailySummaries);
    }

    // If no daily data, try to get monthly data
    const monthlySummaries = await db
      .select({
        minerModel: bitcoinMonthlySummaries.minerModel,
        bitcoinMined: bitcoinMonthlySummaries.bitcoinMined,
        valueAtMining: bitcoinMonthlySummaries.valueAtMining,
        averageDifficulty: bitcoinMonthlySummaries.averageDifficulty
      })
      .from(bitcoinMonthlySummaries)
      .where(eq(bitcoinMonthlySummaries.yearMonth, yearMonth));

    // If monthly data exists, return it
    if (monthlySummaries.length > 0) {
      console.log(`No daily data for ${date}, returning monthly summaries for ${yearMonth}`);
      return res.json(monthlySummaries);
    }

    // If no monthly data, try to get yearly data
    const yearlySummaries = await db
      .select({
        minerModel: bitcoinYearlySummaries.minerModel,
        bitcoinMined: bitcoinYearlySummaries.bitcoinMined,
        valueAtMining: bitcoinYearlySummaries.valueAtMining,
        averageDifficulty: bitcoinYearlySummaries.averageDifficulty
      })
      .from(bitcoinYearlySummaries)
      .where(eq(bitcoinYearlySummaries.year, year));

    // If yearly data exists, return it
    if (yearlySummaries.length > 0) {
      console.log(`No monthly data for ${yearMonth}, returning yearly summaries for ${year}`);
      return res.json(yearlySummaries);
    }

    // If no data found at any level, return zero values for all miner models
    console.log(`No data found at any level for ${date}, returning zeros`);
    const defaultModels = ['S19J_PRO', 'S9', 'M20S'].map(model => ({
      minerModel: model,
      bitcoinMined: '0',
      valueAtMining: '0',
      averageDifficulty: '0'
    }));
    return res.json(defaultModels);

  } catch (error) {
    console.error('Error fetching daily Bitcoin summaries:', error);
    res.status(500).json({ 
      error: 'Failed to fetch daily Bitcoin summaries',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

export default router;
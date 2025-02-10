import { Router } from 'express';
import { format, parseISO, isToday, isValid } from 'date-fns';
import { calculateBitcoinForBMU, processHistoricalCalculations, processSingleDay } from '../services/bitcoinService';
import { BitcoinCalculation } from '../types/bitcoin';
import { db } from "@db";
import { historicalBitcoinCalculations, bitcoinDailySummaries, bitcoinMonthlySummaries, bitcoinYearlySummaries, curtailmentRecords } from "@db/schema";
import { and, eq, sql } from "drizzle-orm";
import { getDifficultyData } from '../services/dynamodbService';
import axios from 'axios';

const router = Router();

// Minerstat API helper function
async function fetchFromMinerstat() {
  try {
    const response = await axios.get('https://api.minerstat.com/v2/stats/bitcoin');
    if (!response.data || !response.data.difficulty || !response.data.price) {
      console.error('Invalid Minerstat response:', response.data);
      throw new Error('Invalid Minerstat response structure');
    }
    return {
      difficulty: response.data.difficulty,
      price: response.data.price
    };
  } catch (error) {
    console.error('Error fetching from Minerstat:', error);
    throw error;
  }
}

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

    let difficulty: number;
    let currentPrice: number;

    try {
      const minerstatData = await fetchFromMinerstat();
      currentPrice = minerstatData.price;

      if (isToday(requestDate)) {
        difficulty = minerstatData.difficulty;
        console.log(`Using current difficulty for today:`, difficulty.toLocaleString());

        // For today's date, calculate total curtailment volume
        const todaysCurtailment = await db
          .select({
            totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`
          })
          .from(curtailmentRecords)
          .where(eq(curtailmentRecords.settlementDate, formattedDate));

        const curtailedMWh = parseFloat(todaysCurtailment[0]?.totalVolume || '0');
        console.log(`Today's total curtailment: ${curtailedMWh} MWh`);

        if (curtailedMWh > 0) {
          const bitcoinMined = calculateBitcoinForBMU(curtailedMWh, minerModel, difficulty);
          return res.json({
            bitcoinMined,
            valueAtCurrentPrice: bitcoinMined * currentPrice,
            difficulty,
            currentPrice
          });
        }
      } else {
        console.log(`Getting historical difficulty for ${formattedDate}`);
        difficulty = await getDifficultyData(formattedDate);
        console.log(`Using historical difficulty:`, difficulty.toLocaleString());
      }

      // For historical dates or if no curtailment found today
      const result = await calculateBitcoinMining(
        formattedDate,
        minerModel,
        difficulty,
        currentPrice,
        leadParty,
        farmId
      );

      console.log('Mining potential response:', {
        date: formattedDate,
        bitcoinMined: result.totalBitcoin,
        valueAtCurrentPrice: result.totalBitcoin * currentPrice,
        difficulty
      });

      res.json({
        bitcoinMined: result.totalBitcoin,
        valueAtCurrentPrice: result.totalBitcoin * currentPrice,
        difficulty,
        currentPrice
      });

    } catch (error) {
      console.error('Error fetching difficulty/price data:', error);
      throw error;
    }

  } catch (error) {
    console.error('Error in mining-potential endpoint:', error);
    res.status(500).json({ 
      error: 'Failed to calculate mining potential',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Helper function for Bitcoin mining calculations
async function calculateBitcoinMining(
  date: string,
  minerModel: string,
  difficulty: number,
  currentPrice: number,
  leadParty?: string,
  farmId?: string
): Promise<{ totalBitcoin: number }> {
  console.log('Calculating Bitcoin mining for:', { date, minerModel, difficulty, leadParty, farmId });

  // For historical dates, first check if we have pre-calculated values
  const query = and(
    eq(historicalBitcoinCalculations.settlementDate, date),
    eq(historicalBitcoinCalculations.minerModel, minerModel)
  );

  // Only add farmId filter if both leadParty and farmId are provided
  if (leadParty && farmId) {
    query.push(eq(historicalBitcoinCalculations.farmId, farmId));
  }

  const records = await db
    .select()
    .from(historicalBitcoinCalculations)
    .where(query);

  console.log(`Found ${records.length} records for ${date}`);

  if (records.length > 0) {
    const totalBitcoin = records.reduce(
      (sum, record) => sum + Number(record.bitcoinMined),
      0
    );
    console.log('Calculated total Bitcoin from historical records:', totalBitcoin);
    return { totalBitcoin };
  }

  // If no historical records found, calculate based on curtailment data
  const curtailmentData = await db
    .select({
      totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));

  const curtailedMWh = parseFloat(curtailmentData[0]?.totalVolume || '0');
  console.log(`Total curtailment for ${date}: ${curtailedMWh} MWh`);

  if (curtailedMWh > 0) {
    const bitcoinMined = calculateBitcoinForBMU(curtailedMWh, minerModel, difficulty);
    console.log('Calculated total Bitcoin from curtailment:', bitcoinMined);
    return { totalBitcoin: bitcoinMined };
  }

  return { totalBitcoin: 0 };
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

    const summaries = await db
      .select({
        minerModel: bitcoinDailySummaries.minerModel,
        bitcoinMined: bitcoinDailySummaries.bitcoinMined,
        valueAtMining: bitcoinDailySummaries.valueAtMining,
        averageDifficulty: bitcoinDailySummaries.averageDifficulty
      })
      .from(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, date));

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
    console.error('Error fetching daily Bitcoin summaries:', error);
    res.status(500).json({ 
      error: 'Failed to fetch daily Bitcoin summaries',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

export default router;
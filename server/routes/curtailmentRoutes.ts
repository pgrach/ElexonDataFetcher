import { Router } from 'express';
import { format, parseISO, isToday, isValid } from 'date-fns';
import { calculateBitcoinForBMU, processHistoricalCalculations, processSingleDay } from '../services/bitcoinService';
import { BitcoinCalculation } from '../types/bitcoin';
import { db } from "@db";
import { historicalBitcoinCalculations, bitcoinDailySummaries, bitcoinMonthlySummaries, bitcoinYearlySummaries } from "@db/schema";
import { and, eq, sql } from "drizzle-orm";
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

    console.log('Mining potential request:', {
      date: formattedDate,
      minerModel,
      leadParty,
      farmId,
      isToday: isToday(requestDate)
    });

    const { price: currentPrice, difficulty: currentDifficulty } = await fetchFromMinerstat();
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
            eq(historicalBitcoinCalculations.minerModel, minerModel),
            leadParty ? eq(historicalBitcoinCalculations.farmId, farmId!) : undefined
          )
        );

      console.log('Historical data from DB:', {
        found: historicalData.length > 0,
        date: formattedDate,
        firstRecord: historicalData[0],
        difficulty: difficulty.toLocaleString()
      });

      if (historicalData && historicalData.length > 0) {
        const totalBitcoin = historicalData.reduce(
          (sum, record) => sum + Number(record.bitcoinMined),
          0
        );

        return res.json({
          bitcoinMined: totalBitcoin,
          valueAtCurrentPrice: totalBitcoin * currentPrice,
          difficulty: Number(historicalData[0].difficulty),
          currentPrice,
          price: currentPrice 
        });
      }
    } else {
      difficulty = currentDifficulty;
      console.log(`Using current difficulty for today:`, difficulty.toLocaleString());
    }

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
      currentPrice,
      price: currentPrice 
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
    console.log(`Fetching yearly summary for ${year}`);

    // Query historical calculations directly for the year
    const historicalData = await db.execute(sql`
      SELECT 
        miner_model,
        COALESCE(SUM(CAST(bitcoin_mined AS numeric)), 0) as total_bitcoin,
        AVG(CAST(difficulty AS numeric)) as avg_difficulty
      FROM historical_bitcoin_calculations
      WHERE EXTRACT(YEAR FROM settlement_date::date) = ${parseInt(year)}
      GROUP BY miner_model
    `);

    const currentPrice = (await fetchFromMinerstat()).price;

    // Always return the aggregated data, even if the current day has no records
    const summaries = historicalData.rows.map(record => ({
      minerModel: record.miner_model,
      bitcoinMined: record.total_bitcoin?.toString() || '0',
      valueAtMining: (Number(record.total_bitcoin || 0) * currentPrice).toString(),
      averageDifficulty: record.avg_difficulty?.toString() || '0'
    }));

    //If we got any data for the year, return it
    if(summaries.length > 0){
        console.log(`Found yearly data for ${year}:`, summaries);
        return res.json(summaries);
    }

    //Only return zero values if there's no data at all for the year
    const defaultModels = ['S19J_PRO', 'S9', 'M20S'].map(model => ({
      minerModel: model,
      bitcoinMined: '0',
      valueAtMining: '0',
      averageDifficulty: '0'
    }));
    return res.json(defaultModels);

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
    const [year, month] = yearMonth.split('-');
    console.log(`Fetching monthly summary for ${yearMonth}`);

    // Query historical calculations directly for the month
    const historicalData = await db.execute(sql`
      SELECT 
        miner_model,
        COALESCE(SUM(CAST(bitcoin_mined AS numeric)), 0) as total_bitcoin,
        AVG(CAST(difficulty AS numeric)) as avg_difficulty
      FROM historical_bitcoin_calculations
      WHERE EXTRACT(YEAR FROM settlement_date::date) = ${parseInt(year)}
      AND EXTRACT(MONTH FROM settlement_date::date) = ${parseInt(month)}
      GROUP BY miner_model
    `);

    const currentPrice = (await fetchFromMinerstat()).price;

    // Always return the aggregated data, even if the current day has no records
    const summaries = historicalData.rows.map(record => ({
      minerModel: record.miner_model,
      bitcoinMined: record.total_bitcoin?.toString() || '0',
      valueAtMining: (Number(record.total_bitcoin || 0) * currentPrice).toString(),
      averageDifficulty: record.avg_difficulty?.toString() || '0'
    }));

    //If we got any data for the month, return it
    if(summaries.length > 0){
        console.log(`Found monthly data for ${yearMonth}:`, summaries);
        return res.json(summaries);
    }

    //Only return zero values if there's no data at all for the month
    const defaultModels = ['S19J_PRO', 'S9', 'M20S'].map(model => ({
      minerModel: model,
      bitcoinMined: '0',
      valueAtMining: '0',
      averageDifficulty: '0'
    }));
    return res.json(defaultModels);

  } catch (error) {
    console.error('Error fetching monthly Bitcoin summaries:', error);
    res.status(500).json({ 
      error: 'Failed to fetch monthly Bitcoin summaries',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Daily summary endpoint remains unchanged as it correctly handles individual days
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
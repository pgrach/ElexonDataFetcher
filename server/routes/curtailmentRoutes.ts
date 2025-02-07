import { Router } from 'express';
import { format, parseISO, isToday, isValid } from 'date-fns';
import { calculateBitcoinForBMU, processHistoricalCalculations, processSingleDay } from '../services/bitcoinService';
import { BitcoinCalculation } from '../types/bitcoin';
import { db } from "@db";
import { historicalBitcoinCalculations } from "@db/schema";
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
  console.log('Starting Bitcoin mining calculation:', {
    date,
    minerModel,
    difficulty,
    currentPrice,
    leadParty,
    farmId
  });

  try {
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

    console.log(`Found ${records.length} historical records`);

    const totalBitcoin = records.reduce(
      (sum, record) => {
        const bitcoinAmount = Number(record.bitcoinMined);
        console.log(`Record bitcoin amount: ${bitcoinAmount}`);
        return sum + bitcoinAmount;
      },
      0
    );

    console.log('Total Bitcoin calculated:', totalBitcoin);
    return { totalBitcoin };
  } catch (error) {
    console.error('Error in calculateBitcoinMining:', error);
    throw error;
  }
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

    const minerstatData = await fetchFromMinerstat();
    console.log('Minerstat data:', minerstatData);

    let difficulty = minerstatData.difficulty;

    if (!isToday(requestDate)) {
      console.log(`Getting historical difficulty for ${formattedDate}`);
      difficulty = await getDifficultyData(formattedDate);
      console.log(`Using historical difficulty: ${difficulty}`);

      const historicalData = await db
        .select()
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, formattedDate),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );

      console.log('Historical data found:', {
        records: historicalData.length,
        date: formattedDate,
        difficulty: difficulty.toLocaleString()
      });

      if (historicalData && historicalData.length > 0) {
        const totalBitcoin = historicalData.reduce(
          (sum, record) => sum + Number(record.bitcoinMined),
          0
        );

        console.log('Calculated from historical data:', {
          totalBitcoin,
          valueAtCurrentPrice: totalBitcoin * minerstatData.price
        });

        return res.json({
          bitcoinMined: totalBitcoin,
          valueAtCurrentPrice: totalBitcoin * minerstatData.price,
          difficulty: Number(historicalData[0].difficulty),
          currentPrice: minerstatData.price
        });
      }
    }

    const result = await calculateBitcoinMining(
      formattedDate,
      minerModel,
      difficulty,
      minerstatData.price,
      leadParty,
      farmId
    );

    console.log('Final calculation result:', {
      bitcoinMined: result.totalBitcoin,
      valueAtCurrentPrice: result.totalBitcoin * minerstatData.price,
      difficulty,
      currentPrice: minerstatData.price
    });

    res.json({
      bitcoinMined: result.totalBitcoin,
      valueAtCurrentPrice: result.totalBitcoin * minerstatData.price,
      difficulty,
      currentPrice: minerstatData.price
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
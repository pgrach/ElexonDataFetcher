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
    const DEFAULT_NETWORK_DIFFICULTY = 71e12; // Set a reasonable default network difficulty

    if (!isToday(requestDate)) {
      console.log(`Getting historical difficulty for ${formattedDate}`);
      try {
        difficulty = await getDifficultyData(formattedDate);
        console.log(`Using historical difficulty for ${formattedDate}:`, difficulty ? difficulty.toLocaleString() : 'N/A');
      } catch (error) {
        console.error(`Error fetching historical difficulty for ${formattedDate}:`, error);
        difficulty = currentDifficulty || DEFAULT_NETWORK_DIFFICULTY; // Use current difficulty or default
      }

      const historicalData = await db
        .select()
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, formattedDate),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );

      console.log('Historical data from DB:', {
        found: historicalData.length > 0,
        date: formattedDate,
        firstRecord: historicalData[0],
        difficulty: difficulty ? difficulty.toLocaleString() : 'N/A'
      });

      if (historicalData && historicalData.length > 0) {
        const totalBitcoin = historicalData.reduce(
          (sum, record) => sum + Number(record.bitcoinMined),
          0
        );

        // Use the historical difficulty from the database records
        const historicalDifficulty = Number(historicalData[0].difficulty);
        console.log(`Using historical difficulty from database: ${historicalDifficulty}`);

        return res.json({
          bitcoinMined: totalBitcoin,
          valueAtCurrentPrice: totalBitcoin * currentPrice,
          difficulty: historicalDifficulty,
          currentPrice
        });
      }
    } else {
      // For today, use the current difficulty from Minerstat
      difficulty = currentDifficulty;
      if (!difficulty) {
        console.warn('Current difficulty not available from Minerstat, using default');
        difficulty = DEFAULT_NETWORK_DIFFICULTY;
      }
      console.log(`Using difficulty for today:`, difficulty.toLocaleString());
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
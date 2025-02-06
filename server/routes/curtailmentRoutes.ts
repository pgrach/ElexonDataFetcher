import { Router } from 'express';
import { format, parseISO, isToday, isValid } from 'date-fns';
import { fetchFromMinerstat, calculateBitcoinMining, processHistoricalCalculations, processSingleDay } from '../services/bitcoinService';
import { BitcoinCalculation } from '../types/bitcoin';
import { db } from "@db";
import { historicalBitcoinCalculations } from "@db/schema";
import { and, eq } from "drizzle-orm";
import { getDifficultyData } from '../services/dynamodbService';

const router = Router();

// Add historical calculations endpoint
router.post('/historical-calculations', async (req, res) => {
  try {
    const { startDate, endDate, minerModel = 'S19J_PRO' } = req.body;

    // Validate dates
    if (!startDate || !endDate || !isValid(parseISO(startDate)) || !isValid(parseISO(endDate))) {
      return res.status(400).json({
        error: 'Invalid date format. Please provide dates in YYYY-MM-DD format.'
      });
    }

    // Start processing in the background
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

    // Validate date
    if (!date || !isValid(parseISO(date))) {
      return res.status(400).json({
        error: 'Invalid date format. Please provide date in YYYY-MM-DD format.'
      });
    }

    console.log(`Starting regeneration for date: ${date}`);

    // Get historical difficulty data
    const difficulty = await getDifficultyData(date);
    console.log('Retrieved historical difficulty:', difficulty);

    // Process for all supported miner models if no specific model is provided
    const minerModels = minerModel ? [minerModel] : ['S19J_PRO', 'S9', 'M20S'];

    // Delete existing calculations for this date and specified models
    await db.delete(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          minerModel ? eq(historicalBitcoinCalculations.minerModel, minerModel) : undefined
        )
      );

    // Process each miner model
    for (const model of minerModels) {
      console.log(`Processing model ${model} with difficulty ${difficulty}`);
      await processSingleDay(date, model)
        .catch(error => {
          console.error(`Error processing Bitcoin calculations for ${date} with ${model}:`, error);
        });
    }

    // Verify the regenerated data
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
      difficulty,
      uniqueDifficulties: [...new Set(regeneratedData.map(r => r.difficulty))]
    });

  } catch (error) {
    console.error('Error in regenerate-historical endpoint:', error);
    res.status(500).json({ 
      error: 'Failed to regenerate historical calculations',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Add the bitcoin calculation endpoint
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

    // Get current price for fiat conversion
    const { price: currentPrice } = await fetchFromMinerstat();
    let difficulty;

    // For historical dates, get stored calculations
    if (!isToday(requestDate)) {
      // First try to get historical difficulty
      console.log(`Getting historical difficulty for ${formattedDate}`);
      difficulty = await getDifficultyData(formattedDate);
      console.log(`Using historical difficulty for ${formattedDate}:`, difficulty.toLocaleString());

      // Check for existing historical calculations
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
          currentPrice
        });
      }
    } else {
      // For today, use current network difficulty
      const { difficulty: currentDifficulty } = await fetchFromMinerstat();
      difficulty = currentDifficulty;
      console.log(`Using current difficulty for today:`, difficulty.toLocaleString());
    }

    // Calculate using appropriate difficulty
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
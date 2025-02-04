import { Router } from 'express';
import { format, parseISO, isToday, isValid } from 'date-fns';
import { fetchFromMinerstat, calculateBitcoinMining, processHistoricalCalculations } from '../services/bitcoinService';
import { BitcoinCalculation } from '../types/bitcoin';
import { db } from "@db";
import { historicalBitcoinCalculations } from "@db/schema";
import { and, eq } from "drizzle-orm";

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

    // For historical dates, fetch from historical_bitcoin_calculations
    if (!isToday(requestDate)) {
      const whereConditions = [
        eq(historicalBitcoinCalculations.settlementDate, formattedDate),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      ];

      if (farmId) {
        whereConditions.push(eq(historicalBitcoinCalculations.farmId, farmId));
      }

      const historicalData = await db
        .select()
        .from(historicalBitcoinCalculations)
        .where(and(...whereConditions));

      if (historicalData && historicalData.length > 0) {
        // Sum up all bitcoin mined and value for the day
        const totalBitcoin = historicalData.reduce(
          (sum, record) => sum + Number(record.bitcoinMined),
          0
        );
        const totalValue = historicalData.reduce(
          (sum, record) => sum + Number(record.valueAtCurrentPrice),
          0
        );

        return res.json({
          bitcoinMined: totalBitcoin,
          valueAtCurrentPrice: totalValue,
          difficulty: Number(historicalData[0].difficulty),
          price: totalValue / totalBitcoin, // Derive price from value and bitcoin amount
          periodCalculations: historicalData.map(record => ({
            period: record.settlementPeriod,
            bmuCalculations: [{
              farmId: record.farmId,
              bitcoinMined: Number(record.bitcoinMined),
              valueAtCurrentPrice: Number(record.valueAtCurrentPrice)
            }]
          }))
        });
      }
    }

    // For today's date or if no historical data found, use live calculation
    const { difficulty, price } = await fetchFromMinerstat();
    console.log('Minerstat data:', { difficulty, price });

    const result = await calculateBitcoinMining(
      formattedDate,
      minerModel,
      difficulty,
      price,
      leadParty,
      farmId
    );

    res.json({
      bitcoinMined: result.totalBitcoin,
      valueAtCurrentPrice: result.totalValue,
      difficulty,
      price,
      periodCalculations: result.periodCalculations
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
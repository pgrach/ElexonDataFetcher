import { Router } from 'express';
import { format, parseISO, isToday, isValid } from 'date-fns';
import { fetchFromMinerstat, calculateBitcoinMining, processHistoricalCalculations } from '../services/bitcoinService';
import { BitcoinCalculation } from '../types/bitcoin';

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

    console.log('Mining potential request:', {
      date: format(requestDate, 'yyyy-MM-dd'),
      minerModel,
      leadParty,
      farmId,
      isToday: isToday(requestDate)
    });

    // Only calculate for today's date
    if (!isToday(requestDate)) {
      console.log('Not today, returning zero values');
      return res.json({
        bitcoinMined: 0,
        valueAtCurrentPrice: 0,
        difficulty: 0,
        price: 0,
        periodCalculations: []
      });
    }

    const { difficulty, price } = await fetchFromMinerstat();
    console.log('Minerstat data:', { difficulty, price });

    const result = await calculateBitcoinMining(
      format(requestDate, 'yyyy-MM-dd'),
      minerModel,
      difficulty,
      price,
      leadParty,
      farmId
    );

    console.log('Calculation result:', {
      totalBitcoin: result.totalBitcoin,
      totalValue: result.totalValue,
      periodCount: result.periodCalculations.length
    });

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
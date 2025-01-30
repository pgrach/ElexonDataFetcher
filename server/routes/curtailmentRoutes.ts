import { Router } from 'express';
import { format, parseISO, isToday } from 'date-fns';
import { fetchFromMinerstat, calculateBitcoinMining } from '../services/bitcoinService';
import { BitcoinCalculation } from '../types/bitcoin';

const router = Router();

// Add the bitcoin calculation endpoint
router.get('/mining-potential', async (req, res) => {
  try {
    const requestDate = req.query.date ? parseISO(req.query.date as string) : new Date();
    const minerModel = req.query.minerModel as string || 'S19J_PRO';
    const leadParty = req.query.leadParty as string;

    console.log('Mining potential request:', {
      date: format(requestDate, 'yyyy-MM-dd'),
      minerModel,
      leadParty,
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
      leadParty
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
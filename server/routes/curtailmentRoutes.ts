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
    const curtailedEnergy = Number(req.query.energy || 0);

    // Only calculate for today's date
    if (!isToday(requestDate)) {
      return res.json({
        bitcoinMined: 0,
        valueAtCurrentPrice: 0,
        difficulty: 0,
        price: 0
      });
    }

    const { difficulty, price } = await fetchFromMinerstat();
    
    const calculation = calculateBitcoinMining(
      curtailedEnergy,
      minerModel,
      difficulty,
      price
    );

    res.json(calculation);
  } catch (error) {
    console.error('Error in mining-potential endpoint:', error);
    res.status(500).json({ 
      error: 'Failed to calculate mining potential',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

export default router;

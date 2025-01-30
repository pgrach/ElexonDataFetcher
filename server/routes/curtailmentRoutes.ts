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

    console.log('Mining potential request:', {
      date: requestDate,
      minerModel,
      curtailedEnergy,
      isToday: isToday(requestDate)
    });

    // Only calculate for today's date
    if (!isToday(requestDate)) {
      console.log('Not today, returning zero values');
      return res.json({
        bitcoinMined: 0,
        valueAtCurrentPrice: 0,
        difficulty: 0,
        price: 0
      });
    }

    const { difficulty, price } = await fetchFromMinerstat();
    console.log('Minerstat data:', { difficulty, price });

    const calculation = calculateBitcoinMining(
      curtailedEnergy,
      minerModel,
      difficulty,
      price
    );

    console.log('Calculation result:', calculation);
    res.json(calculation);
  } catch (error) {
    console.error('Error in mining-potential endpoint:', error);
    res.status(500).json({ 
      error: 'Failed to calculate mining potential',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

// Add hourly bitcoin mining potential endpoint
router.get('/hourly-mining-potential/:date', async (req, res) => {
  try {
    const requestDate = parseISO(req.params.date);
    const minerModel = req.query.minerModel as string || 'S19J_PRO';
    const hourlyData = req.query.hourlyData ? JSON.parse(req.query.hourlyData as string) : [];

    if (!isToday(requestDate)) {
      return res.json(hourlyData.map((hour: any) => ({
        ...hour,
        bitcoinMined: 0,
        valueAtCurrentPrice: 0
      })));
    }

    const { difficulty, price } = await fetchFromMinerstat();
    console.log('Minerstat data for hourly calculation:', { difficulty, price });

    const hourlyCalculations = hourlyData.map((hour: any) => {
      const calculation = calculateBitcoinMining(
        hour.curtailedEnergy,
        minerModel,
        difficulty,
        price
      );

      return {
        ...hour,
        bitcoinMined: calculation.bitcoinMined,
        valueAtCurrentPrice: calculation.valueAtCurrentPrice
      };
    });

    res.json(hourlyCalculations);
  } catch (error) {
    console.error('Error in hourly-mining-potential endpoint:', error);
    res.status(500).json({
      error: 'Failed to calculate hourly mining potential',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

export default router;
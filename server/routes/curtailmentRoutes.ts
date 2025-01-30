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

// Add hourly endpoint with Bitcoin calculations
router.get('/curtailment/hourly/:date', async (req, res) => {
  try {
    const date = parseISO(req.params.date);
    const minerModel = req.query.minerModel as string || 'S19J_PRO';
    let minerstatData;

    // Only fetch minerstat data if the date is today
    if (isToday(date)) {
      minerstatData = await fetchFromMinerstat();
    }

    // Fetch hourly curtailment data from your database here
    // This is a placeholder - replace with actual database query
    const hourlyData = Array.from({ length: 24 }, (_, i) => ({
      hour: `${String(i).padStart(2, '0')}:00`,
      curtailedEnergy: Math.random() * 50, // Replace with actual data
    }));

    // Add Bitcoin calculations for each hour
    const enrichedData = hourlyData.map(hour => {
      let bitcoinMined = 0;

      if (isToday(date) && minerstatData) {
        const calculation = calculateBitcoinMining(
          hour.curtailedEnergy,
          minerModel,
          minerstatData.difficulty,
          minerstatData.price
        );
        bitcoinMined = calculation.bitcoinMined;
      }

      return {
        ...hour,
        bitcoinMined,
      };
    });

    res.json(enrichedData);
  } catch (error) {
    console.error('Error in hourly endpoint:', error);
    res.status(500).json({ 
      error: 'Failed to fetch hourly data',
      message: error instanceof Error ? error.message : 'Unknown error'
    });
  }
});

export default router;
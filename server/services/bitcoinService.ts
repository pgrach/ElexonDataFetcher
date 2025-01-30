import { BitcoinCalculation, MinerStats, minerModels } from '../types/bitcoin';
import axios from 'axios';

export async function fetchFromMinerstat(): Promise<{ difficulty: number; price: number }> {
  try {
    const response = await axios.get('https://api.minerstat.com/v2/coins?list=BTC');
    const { difficulty, price } = response.data[0];

    if (!difficulty || !price) {
      throw new Error('Data not found in minerstat response');
    }

    return { difficulty, price };
  } catch (error) {
    console.error('Error fetching from minerstat:', error);
    throw new Error('Failed to fetch data from minerstat');
  }
}

export function calculateBitcoinMining(
  curtailedEnergy: number, // MWh
  minerModel: string,
  difficulty: number,
  currentPrice: number
): BitcoinCalculation {
  const stats = minerModels[minerModel];
  if (!stats) {
    throw new Error(`Invalid miner model: ${minerModel}`);
  }

  // Convert MWh to Joules (MWh * 3600 * 1000000)
  const energyJoules = curtailedEnergy * 3600 * 1000000;
  
  // Calculate how many hashes we could have computed with this energy
  // Energy (J) / efficiency (J/TH) = Total Terahashes
  const totalTerahashes = energyJoules / stats.efficiency;
  
  // Calculate expected bitcoin based on difficulty
  // Bitcoin per block = 6.25 (current reward)
  // Blocks are found every 10 minutes on average
  // Difficulty is target threshold for valid hash
  const totalHashes = totalTerahashes * Math.pow(10, 12); // Convert TH to H
  const expectedBlocks = (totalHashes / difficulty) / Math.pow(2, 32);
  const bitcoinMined = expectedBlocks * 6.25;

  return {
    bitcoinMined,
    valueAtCurrentPrice: bitcoinMined * currentPrice,
    difficulty,
    price: currentPrice
  };
}

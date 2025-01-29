import axios from 'axios';
import { getHistoricalDifficulty } from './dynamoDbService';

export async function fetchBitcoinDifficulty(date?: Date): Promise<number> {
  // If no date provided or date is today, use Minerstat API
  const today = new Date();
  const isToday = date ? 
    date.toISOString().split('T')[0] === today.toISOString().split('T')[0] : 
    true;

  if (isToday) {
    try {
      const minerstatResponse = await axios.get('https://api.minerstat.com/v2/coins?list=BTC');
      const difficulty = minerstatResponse.data[0].difficulty;
      if (!difficulty) {
        throw new Error('Failed to fetch Bitcoin difficulty from Minerstat');
      }
      return difficulty;
    } catch (error) {
      console.error('Minerstat API Error:', error);
      throw new Error('Failed to fetch current Bitcoin difficulty');
    }
  }

  // For historical dates, use DynamoDB
  if (date) {
    const historicalDifficulty = await getHistoricalDifficulty(date);
    if (historicalDifficulty !== null) {
      return historicalDifficulty;
    }
    throw new Error(`No historical difficulty data found for date: ${date.toISOString().split('T')[0]}`);
  }

  throw new Error('Invalid date parameter');
}

// Re-export the calculator function with our new difficulty fetcher
export async function calculatePotentialBtc(
  curtailedMwh: number,
  date?: Date,
  minerModel: 'S9' | 'S19J_PRO' | 'M20S' = 'S19J_PRO'
): Promise<number> {
  const miner = MINER_MODELS[minerModel];
  const curtailedKwh = curtailedMwh * 1000;
  const minerConsumptionKwh = miner.power / 1000;
  const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);
  
  const networkDifficulty = await fetchBitcoinDifficulty(date);
  const hashesPerBlock = networkDifficulty * Math.pow(2, 32);
  const networkHashRate = hashesPerBlock / 600;
  const networkHashRateTH = networkHashRate / 1e12;
  const totalHashPower = potentialMiners * miner.hashrate;
  const ourNetworkShare = totalHashPower / networkHashRateTH;
  const btcPerHour = ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_HOUR;
  
  return btcPerHour;
}

// Constants from the original implementation
const BLOCK_REWARD = 3.125;
const BLOCKS_PER_HOUR = 6;

export const MINER_MODELS = {
  S9: {
    name: "Antminer S9",
    hashrate: 13.5,
    power: 1323
  },
  S19J_PRO: {
    name: "Antminer S19j Pro",
    hashrate: 100,
    power: 3050
  },
  M20S: {
    name: "Whatsminer M20S",
    hashrate: 68,
    power: 3360
  }
} as const;

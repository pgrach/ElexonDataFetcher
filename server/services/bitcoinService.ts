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

// Current block reward and blocks per hour constants
const BLOCK_REWARD = 3.125; // Current block reward
const BLOCKS_PER_HOUR = 6; // ~6 blocks per hour

export function calculateBitcoinMining(
  curtailedMwh: number,
  minerModel: string,
  difficulty: number,
  currentPrice: number
): BitcoinCalculation {
  const miner = minerModels[minerModel];
  if (!miner) {
    throw new Error(`Invalid miner model: ${minerModel}`);
  }

  // Convert MWh to kWh
  const curtailedKwh = curtailedMwh * 1000;

  // Each miner consumes power in kWh per hour
  const minerConsumptionKwh = miner.power / 1000;

  // How many miners can be powered for one hour with the given curtailed energy?
  const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);

  // Calculate expected hashes to find a block from difficulty
  // Difficulty represents how many hashes on average needed to find a block
  // compared to the base difficulty of 1 which requires 2^32 hashes
  const hashesPerBlock = difficulty * Math.pow(2, 32);

  // Calculate network hashrate in hashes per second
  // Average block time is 600 seconds (10 minutes)
  const networkHashRate = hashesPerBlock / 600;

  // Convert to TH/s for consistency with miner hashrate
  const networkHashRateTH = networkHashRate / 1e12;

  // Total hash power from our miners in TH/s
  const totalHashPower = potentialMiners * miner.hashrate;

  // Calculate probability of finding a block
  const ourNetworkShare = totalHashPower / networkHashRateTH;

  // Estimate BTC mined per hour
  // We expect to find (ourNetworkShare * 6) blocks per hour
  const bitcoinMined = ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_HOUR;

  return {
    bitcoinMined,
    valueAtCurrentPrice: bitcoinMined * currentPrice,
    difficulty,
    price: currentPrice
  };
}
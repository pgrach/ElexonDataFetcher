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
const BLOCK_REWARD = 3.125; // Current block reward as of 2024 (6.25/2)
const BLOCKS_PER_HOUR = 6; // Average blocks per hour (1 block/10 minutes)

export function calculateBitcoinMining(
  curtailedMwh: number,
  minerModel: string,
  difficulty: number,
  currentPrice: number
): BitcoinCalculation {
  console.log('Input parameters:', {
    curtailedMwh,
    minerModel,
    difficulty,
    currentPrice
  });

  const miner = minerModels[minerModel];
  if (!miner) {
    throw new Error(`Invalid miner model: ${minerModel}`);
  }

  // Convert MWh to kWh with precise arithmetic
  const curtailedKwh = Number((curtailedMwh * 1000).toFixed(10));
  console.log('Curtailed kWh:', curtailedKwh);

  // Calculate miner consumption in kWh
  const minerConsumptionKwh = Number((miner.power / 1000).toFixed(10));
  console.log('Miner consumption kWh:', minerConsumptionKwh);

  // Calculate potential miners with floor to ensure we don't overestimate
  const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);
  console.log('Potential miners:', potentialMiners);

  // Calculate hashes needed per block with precise arithmetic
  const hashesPerBlock = Number((difficulty * Math.pow(2, 32)).toFixed(10));
  console.log('Hashes per block:', hashesPerBlock);

  // Network hashrate in H/s (600 seconds per block on average)
  const networkHashRate = Number((hashesPerBlock / 600).toFixed(10));
  console.log('Network hashrate (H/s):', networkHashRate);

  // Convert to TH/s for consistency
  const networkHashRateTH = Number((networkHashRate / 1e12).toFixed(10));
  console.log('Network hashrate (TH/s):', networkHashRateTH);

  // Calculate our total hashpower
  const totalHashPower = Number((potentialMiners * miner.hashrate).toFixed(10));
  console.log('Total hash power (TH/s):', totalHashPower);

  // Calculate our network share
  const ourNetworkShare = Number((totalHashPower / networkHashRateTH).toFixed(10));
  console.log('Network share:', ourNetworkShare);

  // Calculate expected BTC mined per hour
  const bitcoinMined = Number((ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_HOUR).toFixed(8));
  console.log('Bitcoin mined:', bitcoinMined);

  // Calculate value in current price
  const valueAtCurrentPrice = Number((bitcoinMined * currentPrice).toFixed(2));
  console.log('Value at current price:', valueAtCurrentPrice);

  return {
    bitcoinMined,
    valueAtCurrentPrice,
    difficulty,
    price: currentPrice
  };
}
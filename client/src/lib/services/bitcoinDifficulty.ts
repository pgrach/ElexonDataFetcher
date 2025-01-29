import axios from 'axios';

// Miner model interface and constants
export interface MinerModel {
  name: string;
  hashrate: number; // TH/s
  power: number;    // Watts
}

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

// Current block reward and blocks per hour constants
const BLOCK_REWARD = 3.125; // Current block reward
const BLOCKS_PER_HOUR = 6; // ~6 blocks per hour

export type MinerModelType = keyof typeof MINER_MODELS;

/**
 * Fetch Bitcoin network difficulty for a given date
 */
export async function fetchBitcoinDifficulty(date?: Date): Promise<number> {
  const url = new URL('/api/bitcoin/difficulty', window.location.origin);
  if (date) {
    url.searchParams.set('date', date.toISOString().split('T')[0]);
  }
  
  const response = await axios.get(url.toString());
  return response.data.difficulty;
}

/**
 * Calculate potential BTC mined from curtailed energy
 */
export async function calculatePotentialBtc(
  curtailedMwh: number,
  date?: Date,
  minerModel: MinerModelType = 'S19J_PRO'
): Promise<number> {
  const miner = MINER_MODELS[minerModel];
  
  // Convert MWh to kWh
  const curtailedKwh = curtailedMwh * 1000;
  
  // Each miner consumes power in kWh per hour
  const minerConsumptionKwh = miner.power / 1000;
  
  // How many miners can be powered for one hour with the given curtailed energy?
  const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);
  
  // Fetch current network difficulty
  const networkDifficulty = await fetchBitcoinDifficulty(date);
  
  // Calculate expected hashes to find a block from difficulty
  const hashesPerBlock = networkDifficulty * Math.pow(2, 32);
  
  // Calculate network hashrate in hashes per second (600 seconds per block)
  const networkHashRate = hashesPerBlock / 600;
  
  // Convert to TH/s for consistency with miner hashrate
  const networkHashRateTH = networkHashRate / 1e12;
  
  // Total hash power from our miners in TH/s
  const totalHashPower = potentialMiners * miner.hashrate;
  
  // Calculate probability of finding a block
  const ourNetworkShare = totalHashPower / networkHashRateTH;
  
  // Estimate BTC mined per hour
  const btcPerHour = ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_HOUR;
  
  return btcPerHour;
}

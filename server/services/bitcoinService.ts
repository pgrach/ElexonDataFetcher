import { BitcoinCalculation, MinerStats, minerModels } from '../types/bitcoin';
import axios from 'axios';
import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { and, eq } from "drizzle-orm";

const BLOCK_REWARD = 6.25; // Current Bitcoin block reward
const BLOCKS_PER_HOUR = 6; // Average blocks per hour (1 block/10 minutes)
const BLOCKS_PER_PERIOD = BLOCKS_PER_HOUR / 2; // Blocks per 30-minute period

interface BitcoinCalculation {
  bitcoinMined: number;
  valueAtCurrentPrice: number;
  difficulty: number;
  price: number;
}

interface PeriodCalculation extends BitcoinCalculation {
  period: number;
  curtailedMwh: number;
}

const minerModels: { [key: string]: { hashrate: number; power: number } } = {
  S19J_PRO: { hashrate: 104, power: 3068 },
  S19_XP: { hashrate: 141, power: 3010 },
  S19K_PRO: { hashrate: 112, power: 3472 },
  M50S: { hashrate: 130, power: 3300 },
};

function calculateBitcoinForPeriod(
  curtailedMwh: number,
  minerModel: string,
  difficulty: number,
  currentPrice: number
): BitcoinCalculation {
  console.log('Calculating for period:', {
    curtailedMwh,
    minerModel,
    difficulty,
    currentPrice
  });

  const miner = minerModels[minerModel];
  if (!miner) {
    throw new Error(`Invalid miner model: ${minerModel}`);
  }

  // Convert MWh to kWh for the 30-minute period
  const curtailedKwh = Number((curtailedMwh * 1000).toFixed(10));
  console.log('Curtailed kWh:', curtailedKwh);

  // Calculate miner consumption in kWh for 30-min period
  const minerConsumptionKwh = Number((miner.power / 1000 / 2).toFixed(10)); // Divide by 2 for 30-min period
  console.log('Miner consumption kWh per period:', minerConsumptionKwh);

  // Calculate potential miners
  const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);
  console.log('Potential miners:', potentialMiners);

  // Calculate hashes needed per block
  const hashesPerBlock = Number((difficulty * Math.pow(2, 32)).toFixed(10));
  console.log('Hashes per block:', hashesPerBlock);

  // Network hashrate in H/s (600 seconds per block on average)
  const networkHashRate = Number((hashesPerBlock / 600).toFixed(10));
  console.log('Network hashrate (H/s):', networkHashRate);

  // Convert to TH/s
  const networkHashRateTH = Number((networkHashRate / 1e12).toFixed(10));
  console.log('Network hashrate (TH/s):', networkHashRateTH);

  // Calculate our total hashpower
  const totalHashPower = Number((potentialMiners * miner.hashrate).toFixed(10));
  console.log('Total hash power (TH/s):', totalHashPower);

  // Calculate our network share
  const ourNetworkShare = Number((totalHashPower / networkHashRateTH).toFixed(10));
  console.log('Network share:', ourNetworkShare);

  // Calculate expected BTC mined per 30-minute period
  // We use BLOCKS_PER_PERIOD (3 blocks per 30 minutes) * full block reward * our network share
  const bitcoinMined = Number((ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_PERIOD).toFixed(8));
  console.log('Bitcoin mined in period:', bitcoinMined);

  // Calculate value at current price
  const valueAtCurrentPrice = Number((bitcoinMined * currentPrice).toFixed(2));
  console.log('Value at current price:', valueAtCurrentPrice);

  return {
    bitcoinMined,
    valueAtCurrentPrice,
    difficulty,
    price: currentPrice
  };
}

export async function calculateBitcoinMining(
  date: string,
  minerModel: string,
  difficulty: number,
  currentPrice: number
): Promise<{
  totalBitcoin: number;
  totalValue: number;
  periodCalculations: PeriodCalculation[];
}> {
  // Fetch all periods for the given date
  const periodRecords = await db
    .select({
      settlementPeriod: curtailmentRecords.settlementPeriod,
      volume: curtailmentRecords.volume
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .orderBy(curtailmentRecords.settlementPeriod);

  // Calculate mining potential for each period
  const periodCalculations: PeriodCalculation[] = [];
  let totalBitcoin = 0;
  let totalValue = 0;

  for (const record of periodRecords) {
    const curtailedMwh = Math.abs(Number(record.volume));
    const calculation = calculateBitcoinForPeriod(
      curtailedMwh,
      minerModel,
      difficulty,
      currentPrice
    );

    const periodResult: PeriodCalculation = {
      ...calculation,
      period: Number(record.settlementPeriod),
      curtailedMwh
    };

    periodCalculations.push(periodResult);
    totalBitcoin += calculation.bitcoinMined;
    totalValue += calculation.valueAtCurrentPrice;
  }

  return {
    totalBitcoin,
    totalValue,
    periodCalculations
  };
}

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
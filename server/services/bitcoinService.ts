import { BitcoinCalculation, MinerStats, minerModels } from '../types/bitcoin';
import axios from 'axios';
import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { and, eq } from "drizzle-orm";

const BLOCK_REWARD = 3.125; // Block reward per 10-minute block (after halvening in April 2024)

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

  // Calculate miner consumption in kWh for 10-minute block
  const minerConsumptionKwhPer10Min = Number((miner.power / 1000 / 6).toFixed(10)); // Divide by 6 for 10-min block
  console.log('Miner consumption kWh per 10-min block:', minerConsumptionKwhPer10Min);

  // Calculate potential miners based on 10-minute power consumption
  const potentialMiners = Math.floor((curtailedKwh / 3) / minerConsumptionKwhPer10Min); // Divide curtailedKwh by 3 for each 10-min block
  console.log('Potential miners per 10-min block:', potentialMiners);

  // Calculate hashes needed per block
  const hashesPerBlock = Number((difficulty * Math.pow(2, 32)).toFixed(10));
  console.log('Hashes per block:', hashesPerBlock);

  // Network hashrate in H/s (600 seconds per block)
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

  // Calculate expected BTC mined for three consecutive 10-minute blocks
  const bitcoinPerBlock = Number((ourNetworkShare * BLOCK_REWARD).toFixed(8));
  const bitcoinMined = Number((bitcoinPerBlock * 3).toFixed(8)); // 3 blocks in 30 minutes
  console.log('Bitcoin mined per block:', bitcoinPerBlock);
  console.log('Total Bitcoin mined in period (3 blocks):', bitcoinMined);

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
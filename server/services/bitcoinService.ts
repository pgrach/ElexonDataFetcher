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
  console.log('Curtailed kWh for 30-min period:', curtailedKwh);

  // Calculate energy available per 10-minute block
  const energyPer10MinBlock = Number((curtailedKwh / 3).toFixed(10));
  console.log('Energy available per 10-min block:', energyPer10MinBlock);

  // Calculate miner consumption in kWh for one 10-minute block
  const minerKwhPer10Min = Number((miner.power / 1000 / 6).toFixed(10)); // kW * (1/6) hour
  console.log('Single miner consumption per 10-min block:', minerKwhPer10Min);

  // Calculate how many miners we can run with energy from one 10-minute block
  const minersPerBlock = Math.floor(energyPer10MinBlock / minerKwhPer10Min);
  console.log('Miners we can run per 10-min block:', minersPerBlock);

  // Calculate hashes needed for one block
  const hashesPerBlock = Number((difficulty * Math.pow(2, 32)).toFixed(10));
  console.log('Hashes needed per block:', hashesPerBlock);

  // Network hashrate in H/s for one block (600 seconds = 10 minutes)
  const networkHashRate = Number((hashesPerBlock / 600).toFixed(10));
  console.log('Network hashrate (H/s):', networkHashRate);

  // Convert to TH/s for consistency with miner hashrates
  const networkHashRateTH = Number((networkHashRate / 1e12).toFixed(10));
  console.log('Network hashrate (TH/s):', networkHashRateTH);

  // Calculate our total hashpower for one block
  const totalHashPower = Number((minersPerBlock * miner.hashrate).toFixed(10));
  console.log('Our total hash power (TH/s):', totalHashPower);

  // Calculate our share of network for one block
  const networkShare = Number((totalHashPower / networkHashRateTH).toFixed(10));
  console.log('Our network share:', networkShare);

  // Calculate Bitcoin mined for each 10-minute block
  const bitcoinPerBlock = Number((networkShare * BLOCK_REWARD).toFixed(8));
  console.log('Bitcoin mined in one 10-min block:', bitcoinPerBlock);

  // Total Bitcoin mined in 30-minute period (3 blocks)
  const bitcoinMined = Number((bitcoinPerBlock * 3).toFixed(8));
  console.log('Total Bitcoin mined in 30-min period (3 blocks):', bitcoinMined);

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
import { BitcoinCalculation, MinerStats, minerModels } from '../types/bitcoin';
import axios from 'axios';
import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { and, eq } from "drizzle-orm";

const BLOCK_REWARD = 3.125; // Block reward per 10-minute block (after halvening in April 2024)
const SETTLEMENT_PERIOD_MINUTES = 30; // Length of each settlement period
const BLOCKS_PER_SETTLEMENT_PERIOD = 3; // Bitcoin produces ~3 blocks in 30 minutes

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

  // Convert MWh to kWh for the settlement period
  const curtailedKwh = Number((curtailedMwh * 1000).toFixed(10));
  console.log('Curtailed kWh for settlement period:', curtailedKwh);

  // Calculate miner consumption in kWh for the settlement period
  const minerConsumptionKwh = Number((miner.power / 1000) * (SETTLEMENT_PERIOD_MINUTES / 60)).toFixed(10);
  console.log('Miner consumption kWh per settlement period:', minerConsumptionKwh);

  // Calculate how many miners we can run with the available energy
  const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);
  console.log('Potential miners for settlement period:', potentialMiners);

  // Calculate hashes needed for one block
  const hashesPerBlock = Number((difficulty * Math.pow(2, 32)).toFixed(10));
  console.log('Hashes needed per block:', hashesPerBlock);

  // Network hashrate in H/s (600 seconds = 10 minutes)
  const networkHashRate = Number((hashesPerBlock / 600).toFixed(10));
  console.log('Network hashrate (H/s):', networkHashRate);

  // Convert to TH/s for consistency with miner hashrates
  const networkHashRateTH = Number((networkHashRate / 1e12).toFixed(10));
  console.log('Network hashrate (TH/s):', networkHashRateTH);

  // Calculate our total hashpower
  const totalHashPower = Number((potentialMiners * miner.hashrate).toFixed(10));
  console.log('Our total hash power (TH/s):', totalHashPower);

  // Calculate our share of network
  const ourNetworkShare = Number((totalHashPower / networkHashRateTH).toFixed(10));
  console.log('Our network share:', ourNetworkShare);

  // Calculate Bitcoin mined for the settlement period
  const btcPerSettlementPeriod = Number((ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_SETTLEMENT_PERIOD).toFixed(8));
  console.log('Bitcoin mined in settlement period:', btcPerSettlementPeriod);

  // Calculate value at current price
  const valueAtCurrentPrice = Number((btcPerSettlementPeriod * currentPrice).toFixed(2));
  console.log('Value at current price:', valueAtCurrentPrice);

  return {
    bitcoinMined: btcPerSettlementPeriod,
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
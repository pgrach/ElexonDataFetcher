import { BitcoinCalculation, MinerStats, minerModels } from '../types/bitcoin';
import axios from 'axios';
import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { and, eq } from "drizzle-orm";

// Bitcoin network constants
const BLOCK_REWARD = 3.125; // Current block reward
const SETTLEMENT_PERIOD_MINUTES = 30; // Each settlement period is 30 minutes
const BLOCKS_PER_SETTLEMENT_PERIOD = 3; // 3 blocks per 30 minutes (1 block every 10 minutes)

interface BMUCalculation {
  farmId: string;
  bitcoinMined: number;
  valueAtCurrentPrice: number;
  curtailedMwh: number;
}

/**
 * Calculate potential BTC mined from curtailed energy using dynamic network difficulty
 * @param curtailedMwh - Curtailed energy in MWh for a 30-minute settlement period
 * @param minerModel - Miner model to use for calculations
 * @param difficulty - Current network difficulty
 * @param currentPrice - Current Bitcoin price in USD
 * @returns Potential BTC that could be mined in the 30-minute settlement period
 */
function calculateBitcoinForBMU(
  curtailedMwh: number,
  minerModel: string,
  difficulty: number,
  currentPrice: number
): BitcoinCalculation {
  console.log('Calculating for BMU:', {
    curtailedMwh,
    minerModel,
    difficulty,
    currentPrice
  });

  const miner = minerModels[minerModel];
  if (!miner) {
    throw new Error(`Invalid miner model: ${minerModel}`);
  }

  // Convert MWh to kWh
  const curtailedKwh = curtailedMwh * 1000;
  console.log('Curtailed kWh:', curtailedKwh);

  // Each miner consumes power in kWh per settlement period
  const minerConsumptionKwh = (miner.power / 1000) * (SETTLEMENT_PERIOD_MINUTES / 60);
  console.log('Miner consumption kWh per settlement period:', minerConsumptionKwh);

  // How many miners can be powered for the settlement period
  const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);
  console.log('Potential miners for period:', potentialMiners);

  // Calculate expected hashes to find a block from difficulty
  const hashesPerBlock = difficulty * Math.pow(2, 32);
  console.log('Hashes per block:', hashesPerBlock);

  // Calculate network hashrate (hashes per second)
  const networkHashRate = hashesPerBlock / 600; // 600 seconds = 10 minutes
  console.log('Network hashrate (H/s):', networkHashRate);

  // Convert to TH/s for consistency with miner hashrates
  const networkHashRateTH = networkHashRate / 1e12;
  console.log('Network hashrate (TH/s):', networkHashRateTH);

  // Total hash power from our miners in TH/s
  const totalHashPower = potentialMiners * miner.hashrate;
  console.log('Our total hash power (TH/s):', totalHashPower);

  // Calculate probability of finding blocks
  const ourNetworkShare = totalHashPower / networkHashRateTH;
  console.log('Our network share:', ourNetworkShare);

  // Estimate BTC mined per settlement period
  const bitcoinMined = Number((ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_SETTLEMENT_PERIOD).toFixed(8));
  console.log('Bitcoin mined in settlement period:', bitcoinMined);

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
  currentPrice: number,
  leadParty?: string
): Promise<{
  totalBitcoin: number;
  totalValue: number;
  periodCalculations: any[];
}> {
  // Fetch all periods for the given date, filtered by leadParty if provided
  const query = db
    .select({
      settlementPeriod: curtailmentRecords.settlementPeriod,
      volume: curtailmentRecords.volume,
      farmId: curtailmentRecords.farmId,
      leadPartyName: curtailmentRecords.leadPartyName
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));

  if (leadParty) {
    query.where(and(
      eq(curtailmentRecords.settlementDate, date),
      eq(curtailmentRecords.leadPartyName, leadParty)
    ));
  }

  const periodRecords = await query.orderBy(curtailmentRecords.settlementPeriod);

  // Group records by period and calculate for each BMU
  const periodCalculations: any[] = [];
  let totalBitcoin = 0;
  let totalValue = 0;

  // Group records by settlement period
  const periodGroups = periodRecords.reduce((groups, record) => {
    const period = Number(record.settlementPeriod);
    if (!groups[period]) {
      groups[period] = [];
    }
    groups[period].push(record);
    return groups;
  }, {} as Record<number, typeof periodRecords>);

  // Calculate for each period
  for (const [period, records] of Object.entries(periodGroups)) {
    const bmuCalculations: BMUCalculation[] = [];

    // Calculate for each BMU in the period
    for (const record of records) {
      const curtailedMwh = Math.abs(Number(record.volume));
      const calculation = calculateBitcoinForBMU(
        curtailedMwh,
        minerModel,
        difficulty,
        currentPrice
      );

      bmuCalculations.push({
        farmId: record.farmId,
        bitcoinMined: calculation.bitcoinMined,
        valueAtCurrentPrice: calculation.valueAtCurrentPrice,
        curtailedMwh
      });

      totalBitcoin += calculation.bitcoinMined;
      totalValue += calculation.valueAtCurrentPrice;
    }

    periodCalculations.push({
      period: Number(period),
      bmuCalculations,
      periodTotal: {
        bitcoinMined: bmuCalculations.reduce((sum, calc) => sum + calc.bitcoinMined, 0),
        valueAtCurrentPrice: bmuCalculations.reduce((sum, calc) => sum + calc.valueAtCurrentPrice, 0),
        curtailedMwh: bmuCalculations.reduce((sum, calc) => sum + calc.curtailedMwh, 0)
      }
    });
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
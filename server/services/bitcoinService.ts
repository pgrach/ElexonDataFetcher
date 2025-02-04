import { BitcoinCalculation, MinerStats, minerModels } from '../types/bitcoin';
import axios from 'axios';
import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { and, eq, between } from "drizzle-orm";
import { format, parseISO, eachDayOfInterval } from 'date-fns';
import { getHistoricalData } from './dynamodbService';
import pLimit from 'p-limit';

// Bitcoin network constants
const BLOCK_REWARD = 3.125; // Current block reward
const SETTLEMENT_PERIOD_MINUTES = 30; // Each settlement period is 30 minutes
const BLOCKS_PER_SETTLEMENT_PERIOD = 3; // 3 blocks per 30 minutes (1 block every 10 minutes)
const MAX_CONCURRENT_DAYS = 5; // Maximum number of days to process concurrently

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

/**
 * Process a single day's calculations
 */
async function processSingleDay(
  date: string,
  minerModel: string
): Promise<void> {
  console.log(`Processing date: ${date}`);

  try {
    // Get historical data from DynamoDB with error handling
    const historicalData = await getHistoricalData(date);
    if (!historicalData.difficulty || !historicalData.price) {
      throw new Error(`Missing historical data for date: ${date}`);
    }

    // Get all curtailment records for the day
    const records = await db
      .select({
        settlementPeriod: curtailmentRecords.settlementPeriod,
        farmId: curtailmentRecords.farmId,
        volume: curtailmentRecords.volume
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    // Group records by period and farm
    const periodGroups = records.reduce((groups, record) => {
      const key = `${record.settlementPeriod}-${record.farmId}`;
      if (!groups[key]) {
        groups[key] = {
          settlementPeriod: record.settlementPeriod,
          farmId: record.farmId,
          totalVolume: 0
        };
      }
      groups[key].totalVolume += Math.abs(Number(record.volume));
      return groups;
    }, {} as Record<string, { settlementPeriod: number; farmId: string; totalVolume: number; }>);

    // Calculate and store results for each group
    for (const group of Object.values(periodGroups)) {
      const calculation = calculateBitcoinForBMU(
        group.totalVolume,
        minerModel,
        historicalData.difficulty,
        historicalData.price
      );

      try {
        // Store the calculation in the historical table
        await db.insert(historicalBitcoinCalculations).values({
          settlementDate: date,
          settlementPeriod: group.settlementPeriod,
          farmId: group.farmId,
          minerModel,
          bitcoinMined: calculation.bitcoinMined.toString(),
          valueAtCurrentPrice: calculation.valueAtCurrentPrice.toString(),
          difficulty: historicalData.difficulty.toString()
        }).onConflictDoUpdate({
          target: [
            historicalBitcoinCalculations.settlementDate,
            historicalBitcoinCalculations.settlementPeriod,
            historicalBitcoinCalculations.farmId,
            historicalBitcoinCalculations.minerModel
          ],
          set: {
            bitcoinMined: calculation.bitcoinMined.toString(),
            valueAtCurrentPrice: calculation.valueAtCurrentPrice.toString(),
            difficulty: historicalData.difficulty.toString(),
            calculatedAt: new Date()
          }
        });
      } catch (error) {
        console.error('Error inserting/updating calculation:', error);
        throw error;
      }
    }

    console.log(`Completed processing for date: ${date}`);
  } catch (error) {
    console.error(`Error processing date ${date}:`, error);
    throw error;
  }
}

/**
 * Process historical Bitcoin mining calculations for a date range with parallel processing
 */
async function processHistoricalCalculations(
  startDate: string,
  endDate: string,
  minerModel: string = 'S19J_PRO'
): Promise<void> {
  console.log('Processing historical calculations:', { startDate, endDate, minerModel });

  const dateRange = eachDayOfInterval({
    start: parseISO(startDate),
    end: parseISO(endDate)
  });

  // Create a limit function to control concurrency
  const limit = pLimit(MAX_CONCURRENT_DAYS);

  // Process days in parallel with controlled concurrency
  const processPromises = dateRange.map(date => {
    const formattedDate = format(date, 'yyyy-MM-dd');
    return limit(() => processSingleDay(formattedDate, minerModel));
  });

  try {
    await Promise.all(processPromises);
    console.log('Completed processing all dates');
  } catch (error) {
    console.error('Error during parallel processing:', error);
    throw error;
  }
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

/**
 * Process historical Bitcoin mining calculations for a date range
 * @param startDate Start date in YYYY-MM-DD format
 * @param endDate End date in YYYY-MM-DD format
 * @param minerModel Miner model to use for calculations
 */

export async function calculateBitcoinMining(
  date: string,
  minerModel: string,
  difficulty: number,
  currentPrice: number,
  leadParty?: string,
  farmId?: string
): Promise<{
  totalBitcoin: number;
  totalValue: number;
  periodCalculations: any[];
}> {
  // Build the where clause based on filters
  const whereClause = [eq(curtailmentRecords.settlementDate, date)];

  if (farmId) {
    whereClause.push(eq(curtailmentRecords.farmId, farmId));
  } else if (leadParty) {
    whereClause.push(eq(curtailmentRecords.leadPartyName, leadParty));
  }

  // Fetch records with filters
  const periodRecords = await db
    .select({
      settlementPeriod: curtailmentRecords.settlementPeriod,
      volume: curtailmentRecords.volume,
      farmId: curtailmentRecords.farmId,
      leadPartyName: curtailmentRecords.leadPartyName
    })
    .from(curtailmentRecords)
    .where(and(...whereClause))
    .orderBy(curtailmentRecords.settlementPeriod);

  // Group records by period
  const periodGroups = periodRecords.reduce((groups, record) => {
    const period = Number(record.settlementPeriod);
    if (!groups[period]) {
      groups[period] = [];
    }
    groups[period].push(record);
    return groups;
  }, {} as Record<number, typeof periodRecords>);

  // Calculate for each period
  const periodCalculations: any[] = [];
  let totalBitcoin = 0;
  let totalValue = 0;

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

      const bmuResult = {
        farmId: record.farmId,
        bitcoinMined: calculation.bitcoinMined,
        valueAtCurrentPrice: calculation.valueAtCurrentPrice,
        curtailedMwh
      };

      bmuCalculations.push(bmuResult);

      // Only add to totals if it matches our filter criteria
      if ((!farmId || record.farmId === farmId) &&
        (!leadParty || record.leadPartyName === leadParty)) {
        totalBitcoin += calculation.bitcoinMined;
        totalValue += calculation.valueAtCurrentPrice;
      }
    }

    // Calculate period totals only for matching records
    const matchingBMUs = bmuCalculations.filter(calc =>
      (!farmId || calc.farmId === farmId)
    );

    periodCalculations.push({
      period: Number(period),
      bmuCalculations: matchingBMUs,
      periodTotal: {
        bitcoinMined: matchingBMUs.reduce((sum, calc) => sum + calc.bitcoinMined, 0),
        valueAtCurrentPrice: matchingBMUs.reduce((sum, calc) => sum + calc.valueAtCurrentPrice, 0),
        curtailedMwh: matchingBMUs.reduce((sum, calc) => sum + calc.curtailedMwh, 0)
      }
    });
  }

  return {
    totalBitcoin,
    totalValue,
    periodCalculations
  };
}

export interface BMUCalculation {
  farmId: string;
  bitcoinMined: number;
  valueAtCurrentPrice: number;
  curtailedMwh: number;
}

export { processHistoricalCalculations };
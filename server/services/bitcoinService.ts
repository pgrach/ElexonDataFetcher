import { BitcoinCalculation, MinerStats, minerModels, BMUCalculation, DEFAULT_DIFFICULTY } from '../types/bitcoin';
import axios from 'axios';
import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { and, eq, between, sql } from "drizzle-orm";
import { format, parseISO, eachDayOfInterval } from 'date-fns';
import { getDifficultyData } from './dynamodbService';
import pLimit from 'p-limit';

// Bitcoin network constants
const BLOCK_REWARD = 3.125; // Current block reward
const SETTLEMENT_PERIOD_MINUTES = 30; // Each settlement period is 30 minutes
const BLOCKS_PER_SETTLEMENT_PERIOD = 3; // 3 blocks per 30 minutes (1 block every 10 minutes)
const MAX_CONCURRENT_DAYS = 5; // Maximum number of days to process concurrently

// Shared difficulty cache
const DIFFICULTY_CACHE = new Map<string, string>();
const PROCESSING_LOCK = new Map<string, Promise<void>>();
const MAX_CONCURRENT_MINERS = 3;

async function prefetchDifficultyData(dates: string[]): Promise<Map<string, string>> {
  const uncachedDates = dates.filter(date => !DIFFICULTY_CACHE.has(date));

  if (uncachedDates.length > 0) {
    console.log(`[Bitcoin Service] Prefetching difficulty data for ${uncachedDates.length} dates`);

    for (const date of uncachedDates) {
      try {
        const difficulty = await getDifficultyData(date);
        DIFFICULTY_CACHE.set(date, difficulty.toString());
        console.log(`[Bitcoin Service] Cached difficulty for ${date}: ${difficulty.toLocaleString()}`);
      } catch (error) {
        console.error(`[Bitcoin Service] Error fetching difficulty for ${date}:`, error);
        DIFFICULTY_CACHE.set(date, DEFAULT_DIFFICULTY.toString());
      }
    }
  }

  return DIFFICULTY_CACHE;
}

async function processSingleDay(
  date: string,
  minerModel: string
): Promise<void> {
  const lockKey = `${date}_${minerModel}`;

  // Check if this combination is already being processed
  if (PROCESSING_LOCK.has(lockKey)) {
    console.log(`[Bitcoin Service] Waiting for existing process: ${lockKey}`);
    await PROCESSING_LOCK.get(lockKey);
    return;
  }

  // Create a new processing promise
  const processingPromise = (async () => {
    try {
      console.log(`[Bitcoin Service] Processing date: ${date} for model ${minerModel}`);

      // Check existing records
      const existingRecords = await db
        .select({
          count: sql<number>`count(*)::int`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );

      const recordCount = existingRecords[0]?.count || 0;

      if (recordCount > 0) {
        console.log(`[Bitcoin Service] Skipping ${date} for ${minerModel} - already processed`);
        return;
      }

      // Check for curtailment records
      const curtailmentCount = await db
        .select({
          count: sql<number>`count(*)::int`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date));

      if (curtailmentCount[0]?.count === 0) {
        console.log(`[Bitcoin Service] No curtailment records found for ${date}, skipping`);
        return;
      }

      // Use cached difficulty data
      const difficulty = DIFFICULTY_CACHE.get(date) || DEFAULT_DIFFICULTY.toString();

      // Fetch all records for the date
      const records = await db
        .select()
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date));

      if (records.length === 0) {
        console.log(`[Bitcoin Service] No curtailment records to process for ${date}`);
        return;
      }

      // Process in transaction for atomicity
      await db.transaction(async (tx) => {
        // Group and process records
        const periodGroups = records.reduce((groups, record) => {
          const key = `${record.settlementPeriod}`;
          if (!groups[key]) {
            groups[key] = {
              settlementPeriod: record.settlementPeriod,
              totalVolume: 0,
              farms: new Map<string, number>()
            };
          }
          const absVolume = Math.abs(Number(record.volume));
          groups[key].totalVolume += absVolume;
          const currentFarmTotal = groups[key].farms.get(record.farmId) || 0;
          groups[key].farms.set(record.farmId, currentFarmTotal + absVolume);
          return groups;
        }, {} as Record<string, {
          settlementPeriod: number;
          totalVolume: number;
          farms: Map<string, number>;
        }>);

        // Bulk insert preparation
        const calculations = [];

        for (const periodData of Object.values(periodGroups)) {
          const periodBitcoin = calculateBitcoinForBMU(
            periodData.totalVolume,
            minerModel,
            parseFloat(difficulty)
          );

          for (const [farmId, farmVolume] of periodData.farms.entries()) {
            const farmShare = farmVolume / periodData.totalVolume;
            const farmBitcoin = (periodBitcoin * farmShare).toFixed(8);

            calculations.push({
              settlementDate: date,
              settlementPeriod: periodData.settlementPeriod,
              farmId: farmId,
              minerModel,
              bitcoinMined: farmBitcoin,
              difficulty: difficulty
            });
          }
        }

        // Bulk insert all calculations
        if (calculations.length > 0) {
          await tx.insert(historicalBitcoinCalculations)
            .values(calculations)
            .onConflictDoUpdate({
              target: [
                historicalBitcoinCalculations.settlementDate,
                historicalBitcoinCalculations.settlementPeriod,
                historicalBitcoinCalculations.farmId,
                historicalBitcoinCalculations.minerModel
              ],
              set: {
                bitcoinMined: sql`EXCLUDED.bitcoin_mined`,
                difficulty: sql`EXCLUDED.difficulty`,
                calculatedAt: new Date()
              }
            });
        }
      });

      console.log(`[Bitcoin Service] Completed processing for date: ${date}, model: ${minerModel}`);
    } finally {
      PROCESSING_LOCK.delete(lockKey);
    }
  })();

  // Store the promise in the lock map
  PROCESSING_LOCK.set(lockKey, processingPromise);

  // Wait for processing to complete
  await processingPromise;
}

function calculateBitcoinForBMU(
  curtailedMwh: number,
  minerModel: string,
  difficulty: number
): number {
  console.log('[Bitcoin Calculation] Starting calculation with parameters:', {
    curtailedMwh,
    minerModel,
    difficulty: difficulty.toLocaleString(),
    difficultySource: difficulty === DEFAULT_DIFFICULTY ? 'DEFAULT_DIFFICULTY' : 'Historical'
  });

  const miner = minerModels[minerModel];
  if (!miner) {
    throw new Error(`Invalid miner model: ${minerModel}`);
  }

  // Convert MWh to kWh
  const curtailedKwh = curtailedMwh * 1000;

  // Each miner consumes power in kWh per settlement period
  const minerConsumptionKwh = (miner.power / 1000) * (SETTLEMENT_PERIOD_MINUTES / 60);

  // How many miners can be powered for the settlement period
  const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);

  // Calculate expected hashes to find a block from difficulty
  // Ensure difficulty is treated as a number
  const difficultyNum = typeof difficulty === 'string' ? parseFloat(difficulty) : difficulty;
  const hashesPerBlock = difficultyNum * Math.pow(2, 32);

  // Calculate network hashrate (hashes per second)
  const networkHashRate = hashesPerBlock / 600; // 600 seconds = 10 minutes

  // Convert to TH/s for consistency with miner hashrates
  const networkHashRateTH = networkHashRate / 1e12;

  // Total hash power from our miners in TH/s
  const totalHashPower = potentialMiners * miner.hashrate;

  // Calculate probability of finding blocks
  const ourNetworkShare = totalHashPower / networkHashRateTH;

  // Estimate BTC mined per settlement period
  const bitcoinMined = Number((ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_SETTLEMENT_PERIOD).toFixed(8));

  console.log('[Bitcoin Calculation] Calculation details:', {
    curtailedMwh,
    curtailedKwh,
    minerConsumptionKwh,
    potentialMiners,
    networkHashRateTH: networkHashRateTH.toLocaleString(),
    totalHashPower: totalHashPower.toLocaleString(),
    ourNetworkShare,
    bitcoinMined,
    usedDifficulty: difficultyNum.toLocaleString(),
    minerModel,
    minerHashrate: miner.hashrate,
    minerPower: miner.power
  });

  return bitcoinMined;
}

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

  // Prefetch ALL difficulty data at once
  const dates = dateRange.map(date => format(date, 'yyyy-MM-dd'));
  await prefetchDifficultyData(dates);

  // Process miner models in parallel with concurrency limit
  const limit = pLimit(MAX_CONCURRENT_MINERS);
  const MINER_MODELS = Object.keys(minerModels);

  const minerPromises = MINER_MODELS.map(model =>
    limit(async () => {
      try {
        await processSingleDay(startDate, model);
      } catch (error) {
        console.error(`Failed to process ${model} for ${startDate}:`, error);
        throw error;
      }
    })
  );

  try {
    await Promise.all(minerPromises);
    console.log(`Completed processing all miner models for ${startDate}`);
  } catch (error) {
    console.error('Error during parallel processing:', error);
    throw error;
  }
}

async function fetchFromMinerstat(): Promise<{ difficulty: number; price: number }> {
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

async function calculateBitcoinMining(
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
  console.log('[Bitcoin Mining] Starting calculation with parameters:', {
    date,
    minerModel,
    difficulty,
    currentPrice,
    leadParty,
    farmId
  });

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

  console.log(`[Bitcoin Mining] Retrieved ${periodRecords.length} records for processing`);

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
        difficulty
      );

      const bmuResult = {
        farmId: record.farmId,
        bitcoinMined: calculation,
        valueAtCurrentPrice: calculation * currentPrice,
        curtailedMwh
      };

      bmuCalculations.push(bmuResult);

      // Only add to totals if it matches our filter criteria
      if ((!farmId || record.farmId === farmId) &&
        (!leadParty || record.leadPartyName === leadParty)) {
        totalBitcoin += calculation;
        totalValue += calculation * currentPrice;
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

  console.log('[Bitcoin Mining] Calculation completed:', {
    totalBitcoin,
    totalValue,
    usedDifficulty: difficulty,
    periodCount: periodCalculations.length
  });

  return {
    totalBitcoin,
    totalValue,
    periodCalculations
  };
}

// Single consolidated export statement
export {
  calculateBitcoinForBMU,
  calculateBitcoinMining,
  processHistoricalCalculations,
  fetchFromMinerstat,
  processSingleDay,
  prefetchDifficultyData
};
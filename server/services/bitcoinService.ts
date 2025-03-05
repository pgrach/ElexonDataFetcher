import { BitcoinCalculation, MinerStats, minerModels, BMUCalculation } from '../types/bitcoin';
import axios from 'axios';
import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations, bitcoinMonthlySummaries } from "@db/schema";
import { and, eq, between, sql, inArray } from "drizzle-orm";
import { format, parseISO, eachDayOfInterval, startOfMonth, endOfMonth } from 'date-fns';
import { getDifficultyData } from './dynamodbService';
import pLimit from 'p-limit';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// ES Module path setup
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Bitcoin network constants
const BLOCK_REWARD = 3.125;
const SETTLEMENT_PERIOD_MINUTES = 30;
const BLOCKS_PER_SETTLEMENT_PERIOD = 3;

// Processing configuration
const MAX_RETRIES = 5;
const BASE_DELAY = 5000; // 5 seconds base delay

// Cache file path
const CACHE_FILE = path.join(__dirname, '..', 'data', '2024_difficulties.json');

// Shared caches
const DIFFICULTY_CACHE = new Map<string, string>();
const PROCESSING_LOCK = new Map<string, Promise<void>>();

async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function loadCachedDifficulties(): Promise<void> {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      const data = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf-8'));
      Object.entries(data).forEach(([date, difficulty]) => {
        DIFFICULTY_CACHE.set(date, difficulty.toString());
      });
      console.log(`Loaded ${DIFFICULTY_CACHE.size} difficulties from cache`);
    }
  } catch (error) {
    console.error('Error loading cached difficulties:', error);
  }
}

async function saveDifficultiesToCache(): Promise<void> {
  try {
    const cacheDir = path.dirname(CACHE_FILE);
    if (!fs.existsSync(cacheDir)) {
      fs.mkdirSync(cacheDir, { recursive: true });
    }
    const data = Object.fromEntries(DIFFICULTY_CACHE.entries());
    fs.writeFileSync(CACHE_FILE, JSON.stringify(data, null, 2));
    console.log('Saved difficulties to cache file');
  } catch (error) {
    console.error('Error saving difficulties to cache:', error);
  }
}

async function fetch2024Difficulties(): Promise<void> {
  console.log('Fetching 2024 difficulty data...');
  await loadCachedDifficulties();

  const dateRange = eachDayOfInterval({
    start: new Date('2024-01-01'),
    end: new Date('2024-12-31')
  });

  const dates = dateRange.map(date => format(date, 'yyyy-MM-dd'));
  const uncachedDates = dates.filter(date => !DIFFICULTY_CACHE.has(date));

  if (uncachedDates.length === 0) {
    console.log('All difficulties already cached');
    return;
  }

  console.log(`Need to fetch ${uncachedDates.length} difficulties`);
  const limit = pLimit(1); // Single request at a time

  for (const date of uncachedDates) {
    await limit(async () => {
      let retries = 0;
      let success = false;

      while (!success && retries < MAX_RETRIES) {
        try {
          console.log(`[${retries + 1}/${MAX_RETRIES}] Fetching difficulty for ${date}`);
          const data = await getDifficultyData(date);
          // Type guard for difficulty - improved type handling
          const difficultyValue: number = typeof data === 'number' ? data : (typeof data === 'string' ? parseFloat(data) : (data && typeof data.difficulty === 'number' ? data.difficulty : DEFAULT_DIFFICULTY));

          DIFFICULTY_CACHE.set(date, difficultyValue.toString());
          console.log(`✓ Cached difficulty for ${date}: ${difficultyValue}`);
          success = true;
          await saveDifficultiesToCache();
        } catch (error) {
          retries++;
          if (retries === MAX_RETRIES) {
            console.error(`Max retries reached for ${date}, using default difficulty`);
            DIFFICULTY_CACHE.set(date, DEFAULT_DIFFICULTY.toString());
            await saveDifficultiesToCache();
            break;
          }
          const delay = BASE_DELAY * Math.pow(2, retries - 1);
          console.log(`Attempt ${retries} failed, waiting ${delay/1000}s before retry`);
          await sleep(delay);
        }
      }

      await sleep(5000); // 5 second cooldown between requests
    });
  }

  console.log(`Completed caching difficulties. Total cached: ${DIFFICULTY_CACHE.size}`);
}

async function processSingleDay(
  date: string,
  minerModel: string
): Promise<void> {
  const lockKey = `${date}_${minerModel}`;

  if (PROCESSING_LOCK.has(lockKey)) {
    await PROCESSING_LOCK.get(lockKey);
    return;
  }

  const processingPromise = (async () => {
    try {
      // If difficulty is not in cache, fetch it
      let difficultyValue: number;
      if (!DIFFICULTY_CACHE.has(date)) {
        const difficultyData = await getDifficultyData(date);
        // Handle both possible return types (number or object with difficulty property)
        const difficulty = typeof difficultyData === 'object' && difficultyData !== null
          ? (difficultyData as { difficulty: number }).difficulty
          : typeof difficultyData === 'number' ? difficultyData : parseFloat(difficultyData as string);

        difficultyValue = difficulty ?? DEFAULT_DIFFICULTY;
        DIFFICULTY_CACHE.set(date, difficultyValue.toString());
        console.log(`Fetched and cached difficulty for ${date}: ${difficultyValue}`);
      } else {
        difficultyValue = parseFloat(DIFFICULTY_CACHE.get(date) || DEFAULT_DIFFICULTY.toString());
      }


      return await db.transaction(async (tx) => {
        const curtailmentData = await tx
          .select({
            periods: sql<number[]>`array_agg(DISTINCT settlement_period)`,
            farmIds: sql<string[]>`array_agg(DISTINCT farm_id)`
          })
          .from(curtailmentRecords)
          .where(
            and(
              eq(curtailmentRecords.settlementDate, date),
              sql`ABS(volume::numeric) > 0`
            )
          );

        if (!curtailmentData[0] || !curtailmentData[0].periods || curtailmentData[0].periods.length === 0) {
          console.log(`No curtailment records with volume for ${date}`);
          return;
        }

        const periods = curtailmentData[0].periods;
        const farmIds = curtailmentData[0].farmIds;

        await tx.delete(historicalBitcoinCalculations)
          .where(
            and(
              eq(historicalBitcoinCalculations.settlementDate, date),
              eq(historicalBitcoinCalculations.minerModel, minerModel)
            )
          );

        const records = await tx
          .select()
          .from(curtailmentRecords)
          .where(
            and(
              eq(curtailmentRecords.settlementDate, date),
              inArray(curtailmentRecords.settlementPeriod, periods),
              sql`ABS(volume::numeric) > 0`
            )
          );

        console.log(`Processing ${date} with difficulty ${difficultyValue}`);
        console.log(`Found ${records.length} curtailment records across ${periods.length} periods and ${farmIds.length} farms`);

        const periodGroups = new Map<number, { totalVolume: number; farms: Map<string, number> }>();

        for (const record of records) {
          if (!periodGroups.has(record.settlementPeriod)) {
            periodGroups.set(record.settlementPeriod, {
              totalVolume: 0,
              farms: new Map<string, number>()
            });
          }

          const group = periodGroups.get(record.settlementPeriod)!;
          const absVolume = Math.abs(Number(record.volume));
          group.totalVolume += absVolume;
          group.farms.set(
            record.farmId,
            (group.farms.get(record.farmId) || 0) + absVolume
          );
        }

        const bulkInsertData: Array<{
          settlementDate: string;
          settlementPeriod: number;
          farmId: string;
          minerModel: string;
          bitcoinMined: string;
          difficulty: string;
          calculatedAt: Date;
        }> = [];

        for (const [period, data] of periodGroups) {
          const periodBitcoin = calculateBitcoinForBMU(
            data.totalVolume,
            minerModel,
            difficultyValue as number
          );

          for (const [farmId, farmVolume] of data.farms) {
            const bitcoinShare = (periodBitcoin * farmVolume) / data.totalVolume;
            bulkInsertData.push({
              settlementDate: date,
              settlementPeriod: period,
              farmId,
              minerModel,
              bitcoinMined: bitcoinShare.toFixed(8),
              difficulty: String(difficultyValue ?? DEFAULT_DIFFICULTY),
              calculatedAt: new Date()
            });
          }
        }

        if (bulkInsertData.length > 0) {
          await tx.insert(historicalBitcoinCalculations)
            .values(bulkInsertData);

          console.log(`Inserted ${bulkInsertData.length} records for ${date} ${minerModel}`);
          console.log(`Processed periods: ${periods.join(', ')}`);
        } else {
          console.log(`No records to insert for ${date} ${minerModel}`);
        }
      });
    } catch (error) {
      console.error(`Error processing ${date} for ${minerModel}:`, error);
      throw error;
    } finally {
      PROCESSING_LOCK.delete(lockKey);
    }
  })();

  PROCESSING_LOCK.set(lockKey, processingPromise);
  await processingPromise;
}

function calculateBitcoinForBMU(
  curtailedMwh: number,
  minerModel: string,
  difficultyValue: number
): number {
  const miner = minerModels[minerModel];
  if (!miner) throw new Error(`Invalid miner model: ${minerModel}`);

  // Convert MWh to joules
  const energyInJoules = curtailedMwh * 3600000000;

  // Bitcoin mining calculation
  const hashesPerBlock = difficultyValue * Math.pow(2, 32);
  const hashrateInHashes = (miner.hashrate * Math.pow(10, 12)); // TH/s to H/s
  const networkHashRate = hashesPerBlock / 600;
  const networkHashRateTH = networkHashRate / 1e12;
  const totalHashPower =  miner.hashrate * 1e12; // Assuming potentialMiners is not relevant for this calculation.  This needs clarification from the user.
  const ourNetworkShare = totalHashPower / networkHashRateTH;
  return Number((ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_SETTLEMENT_PERIOD).toFixed(8));
}

async function processHistoricalCalculations(
  startDate: string,
  endDate: string,
  minerModel: string = 'S19J_PRO'
): Promise<void> {
  await fetch2024Difficulties();
  const limit = pLimit(10);
  const MINER_MODELS = Object.keys(minerModels);

  await Promise.all(
    MINER_MODELS.map(model =>
      limit(async () => {
        try {
          const dates = eachDayOfInterval({ start: new Date(startDate), end: new Date(endDate) });
          await Promise.all(dates.map(date => processSingleDay(format(date, 'yyyy-MM-dd'), model)));
        } catch (error) {
          console.error(`Failed to process ${model} for ${startDate}:`, error);
          throw error;
        }
      })
    )
  );
}


function convertEmptyValues(data: any): any {
  //Basic check for null and undefined
  if (data === null || data === undefined) {
    return null;
  }

  //Check for empty objects and arrays
  if (typeof data === 'object' ) {
    if (Array.isArray(data) && data.length === 0){
      return null;
    }
    if (Object.keys(data).length === 0){
      return null;
    }
  }

  return data;
}


async function calculateMonthlyBitcoinSummary(yearMonth: string, minerModel: string): Promise<void> {
  console.log(`Calculating monthly Bitcoin summary for ${yearMonth} with ${minerModel}`);

  const [year, month] = yearMonth.split('-');
  const startDate = startOfMonth(new Date(parseInt(year), parseInt(month) - 1));
  const endDate = endOfMonth(startDate);

  try {
    const monthlyData = await db
      .select({
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`,
        avgDifficulty: sql<string>`AVG(difficulty::numeric)`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          between(
            historicalBitcoinCalculations.settlementDate,
            format(startDate, 'yyyy-MM-dd'),
            format(endDate, 'yyyy-MM-dd')
          ),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );

    if (!monthlyData[0]?.totalBitcoin) {
      console.log(`No Bitcoin data found for ${yearMonth}`);
      return;
    }

    const totalBitcoin = Number(monthlyData[0].totalBitcoin);
    const avgDifficulty = Number(monthlyData[0].avgDifficulty);

    await db.transaction(async (tx) => {
      // Delete existing summary if any
      await tx
        .delete(bitcoinMonthlySummaries)
        .where(
          and(
            eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
            eq(bitcoinMonthlySummaries.minerModel, minerModel)
          )
        );

      // Insert new summary with the updated schema
      await tx
        .insert(bitcoinMonthlySummaries)
        .values({
          yearMonth,
          minerModel,
          bitcoinMined: totalBitcoin.toString(),
          valueAtMining: "0", // Set to 0 as this will be calculated with current price when queried
          averageDifficulty: avgDifficulty.toString(),
          createdAt: new Date(),
          updatedAt: new Date()
        });
    });

    console.log(`Updated monthly summary for ${yearMonth}: ${totalBitcoin.toFixed(8)} BTC`);
  } catch (error) {
    console.error(`Error calculating monthly summary for ${yearMonth}:`, error);
    throw error;
  }
}

export {
  calculateBitcoinForBMU,
  processHistoricalCalculations,
  processSingleDay,
  fetch2024Difficulties,
  calculateMonthlyBitcoinSummary,
  convertEmptyValues
};

const DEFAULT_DIFFICULTY = 110000000000000; // Default difficulty value
import { BitcoinCalculation, MinerStats, minerModels, BMUCalculation, DEFAULT_DIFFICULTY } from '../types/bitcoin';
import axios from 'axios';
import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { and, eq, between, sql } from "drizzle-orm";
import { format, parseISO, eachDayOfInterval } from 'date-fns';
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
const MAX_CONCURRENT_MINERS = 10;
const BATCH_SIZE = 500;
const DB_BATCH_SIZE = 1000;
const DYNAMO_CONCURRENCY = 1; // Single request at a time
const DYNAMO_BATCH_DELAY = 5000; // 5 seconds between requests

// Cache file path
const CACHE_FILE = path.join(__dirname, '..', 'data', '2024_difficulties.json');

// Shared caches
const DIFFICULTY_CACHE = new Map<string, string>();
const PROCESSING_LOCK = new Map<string, Promise<void>>();

// Exponential backoff settings
const MAX_RETRIES = 5;
const BASE_DELAY = 5000; // 5 seconds base delay

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

  // Load existing cache first
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
  const limit = pLimit(DYNAMO_CONCURRENCY);

  for (const date of uncachedDates) {
    await limit(async () => {
      let retries = 0;
      let success = false;

      while (!success && retries < MAX_RETRIES) {
        try {
          console.log(`[${retries + 1}/${MAX_RETRIES}] Fetching difficulty for ${date}`);
          const difficulty = await getDifficultyData(date);
          DIFFICULTY_CACHE.set(date, difficulty.toString());
          console.log(`✓ Cached difficulty for ${date}`);
          success = true;

          // Save to cache file after each successful fetch
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
          console.log(`Attempt ${retries} failed, waiting ${delay/1000}s before retry: ${error.message}`);
          await sleep(delay);
        }
      }

      // Add substantial delay between requests regardless of success
      const cooldownDelay = success ? DYNAMO_BATCH_DELAY : DYNAMO_BATCH_DELAY * 2;
      console.log(`Cooling down for ${cooldownDelay/1000}s before next request...`);
      await sleep(cooldownDelay);
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
      // Start transaction
      return await db.transaction(async (tx) => {
        // Check existing records within transaction
        const existingRecords = await tx
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

        if (existingRecords[0]?.count > 0) {
          console.log(`Skipping ${date} for ${minerModel} - already processed`);
          return;
        }

        // Get curtailment records
        const records = await tx
          .select()
          .from(curtailmentRecords)
          .where(eq(curtailmentRecords.settlementDate, date));

        if (records.length === 0) {
          console.log(`No curtailment records found for ${date}`);
          return;
        }

        const difficulty = DIFFICULTY_CACHE.get(date) || DEFAULT_DIFFICULTY.toString();
        console.log(`Using difficulty ${difficulty} for ${date}`);

        // Pre-calculate bitcoin values
        const periodCalculations = new Map<number, number>();
        const periodGroups = records.reduce((acc, record) => {
          const periodKey = record.settlementPeriod;
          if (!acc.has(periodKey)) {
            acc.set(periodKey, {
              totalVolume: 0,
              farms: new Map<string, number>()
            });
          }
          const group = acc.get(periodKey)!;
          const absVolume = Math.abs(Number(record.volume));
          group.totalVolume += absVolume;
          group.farms.set(
            record.farmId,
            (group.farms.get(record.farmId) || 0) + absVolume
          );
          return acc;
        }, new Map<number, { totalVolume: number; farms: Map<string, number> }>());

        // Calculate bitcoin for each period
        for (const [period, data] of periodGroups.entries()) {
          periodCalculations.set(
            period,
            calculateBitcoinForBMU(data.totalVolume, minerModel, parseFloat(difficulty))
          );
        }

        // Prepare bulk insert data
        const bulkInsertData = Array.from(periodGroups.entries()).flatMap(([period, data]) => {
          const periodBitcoin = periodCalculations.get(period)!;
          return Array.from(data.farms.entries()).map(([farmId, farmVolume]) => ({
            settlementDate: date,
            settlementPeriod: period,
            farmId,
            minerModel,
            bitcoinMined: ((periodBitcoin * farmVolume) / data.totalVolume).toFixed(8),
            difficulty,
            calculatedAt: new Date()
          }));
        });

        // Perform insert within transaction
        if (bulkInsertData.length > 0) {
          await tx.insert(historicalBitcoinCalculations)
            .values(bulkInsertData)
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
                calculatedAt: sql`EXCLUDED.calculated_at`
              }
            });

          console.log(`Inserted ${bulkInsertData.length} records for ${date} ${minerModel}`);
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
  difficulty: number
): number {
  const miner = minerModels[minerModel];
  if (!miner) throw new Error(`Invalid miner model: ${minerModel}`);

  const curtailedKwh = curtailedMwh * 1000;
  const minerConsumptionKwh = (miner.power / 1000) * (SETTLEMENT_PERIOD_MINUTES / 60);
  const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);
  const difficultyNum = typeof difficulty === 'string' ? parseFloat(difficulty) : difficulty;
  const hashesPerBlock = difficultyNum * Math.pow(2, 32);
  const networkHashRate = hashesPerBlock / 600;
  const networkHashRateTH = networkHashRate / 1e12;
  const totalHashPower = potentialMiners * miner.hashrate;
  const ourNetworkShare = totalHashPower / networkHashRateTH;
  return Number((ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_SETTLEMENT_PERIOD).toFixed(8));
}

async function processHistoricalCalculations(
  startDate: string,
  endDate: string,
  minerModel: string = 'S19J_PRO'
): Promise<void> {
  await fetch2024Difficulties(); // Fetch 2024 difficulties before processing
  const limit = pLimit(MAX_CONCURRENT_MINERS);
  const MINER_MODELS = Object.keys(minerModels);

  await Promise.all(
    MINER_MODELS.map(model =>
      limit(async () => {
        try {
          await processSingleDay(startDate, model);
        } catch (error) {
          console.error(`Failed to process ${model} for ${startDate}:`, error);
          throw error;
        }
      })
    )
  );
}

export {
  calculateBitcoinForBMU,
  processHistoricalCalculations,
  processSingleDay,
  fetch2024Difficulties
};
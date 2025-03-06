import { BitcoinCalculation, MinerStats, minerModels, BMUCalculation, DEFAULT_DIFFICULTY } from '../types/bitcoin';
import axios from 'axios';
import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations, bitcoinMonthlySummaries, bitcoinYearlySummaries } from "@db/schema";
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
          const difficulty = await getDifficultyData(date);
          DIFFICULTY_CACHE.set(date, difficulty.toString());
          console.log(`✓ Cached difficulty for ${date}: ${difficulty}`);
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
      if (!DIFFICULTY_CACHE.has(date)) {
        const difficulty = await getDifficultyData(date);
        DIFFICULTY_CACHE.set(date, difficulty.toString());
        console.log(`Fetched and cached difficulty for ${date}: ${difficulty}`);
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

        const difficulty = DIFFICULTY_CACHE.get(date) || DEFAULT_DIFFICULTY.toString();
        console.log(`Processing ${date} with difficulty ${difficulty}`);
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
            parseFloat(difficulty)
          );

          for (const [farmId, farmVolume] of data.farms) {
            const bitcoinShare = (periodBitcoin * farmVolume) / data.totalVolume;
            bulkInsertData.push({
              settlementDate: date,
              settlementPeriod: period,
              farmId,
              minerModel,
              bitcoinMined: bitcoinShare.toFixed(8),
              difficulty,
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
  await fetch2024Difficulties();
  const limit = pLimit(10);
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

/**
 * Manually update monthly Bitcoin summaries for a specific month
 * This is used to fix the current issue with March 2025 and can be used
 * for any month that needs to be recalculated.
 */
async function manualUpdateMonthlyBitcoinSummary(yearMonth: string): Promise<void> {
  console.log(`\n=== Manual Monthly Bitcoin Summary Update ===`);
  console.log(`Updating summaries for ${yearMonth}`);
  
  // Process all miner models
  for (const minerModel of Object.keys(minerModels)) {
    try {
      console.log(`- Processing ${minerModel}`);
      await calculateMonthlyBitcoinSummary(yearMonth, minerModel);
    } catch (error) {
      console.error(`Error updating ${yearMonth} for ${minerModel}:`, error);
    }
  }
  
  // Verify results
  const summaries = await db
    .select({
      minerModel: bitcoinMonthlySummaries.minerModel,
      bitcoinMined: bitcoinMonthlySummaries.bitcoinMined
    })
    .from(bitcoinMonthlySummaries)
    .where(eq(bitcoinMonthlySummaries.yearMonth, yearMonth));
    
  if (summaries.length > 0) {
    console.log(`\nVerification Results for ${yearMonth}:`);
    for (const summary of summaries) {
      console.log(`- ${summary.minerModel}: ${Number(summary.bitcoinMined).toFixed(8)} BTC`);
    }
  } else {
    console.log(`No summaries found for ${yearMonth} after update attempt`);
  }
  
  // Since a monthly summary was updated, also update the yearly summary
  const year = yearMonth.substring(0, 4);
  try {
    console.log(`Updating yearly summary for ${year} based on monthly changes...`);
    await manualUpdateYearlyBitcoinSummary(year);
  } catch (error) {
    console.error(`Error updating yearly summary for ${year}:`, error);
  }
  
  console.log(`=== Monthly Summary Update Complete ===\n`);
}

// Process command line arguments if run directly
if (process.argv[1].includes('bitcoinService.ts')) {
  const args = process.argv.slice(2);
  
  (async () => {
    try {
      if (args.includes('--update-monthly') && args.length > 1) {
        const yearMonthIndex = args.indexOf('--update-monthly') + 1;
        if (yearMonthIndex < args.length) {
          const yearMonth = args[yearMonthIndex];
          if (/^\d{4}-\d{2}$/.test(yearMonth)) {
            await manualUpdateMonthlyBitcoinSummary(yearMonth);
            process.exit(0);
          } else {
            console.error('Invalid year-month format. Must be YYYY-MM.');
            process.exit(1);
          }
        }
      } else {
        console.log('Usage: npx tsx bitcoinService.ts --update-monthly YYYY-MM');
        process.exit(1);
      }
    } catch (error) {
      console.error('Error:', error);
      process.exit(1);
    }
  })();
}

/**
 * Calculate yearly Bitcoin summary from monthly summaries
 * This aggregates all monthly data for a specific year and miner model
 */
async function calculateYearlyBitcoinSummary(year: string, minerModel: string): Promise<void> {
  console.log(`Calculating yearly Bitcoin summary for ${year} with ${minerModel}`);
  
  try {
    // Find all monthly summaries for this year and miner model
    const monthlySummaries = await db
      .select({
        yearMonth: bitcoinMonthlySummaries.yearMonth,
        bitcoinMined: bitcoinMonthlySummaries.bitcoinMined,
        averageDifficulty: bitcoinMonthlySummaries.averageDifficulty
      })
      .from(bitcoinMonthlySummaries)
      .where(
        and(
          sql`${bitcoinMonthlySummaries.yearMonth} LIKE ${year + '-%'}`,
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        )
      )
      .orderBy(bitcoinMonthlySummaries.yearMonth);
    
    if (monthlySummaries.length === 0) {
      console.log(`No monthly summaries found for ${year} with ${minerModel}`);
      return;
    }
    
    console.log(`Found ${monthlySummaries.length} monthly summaries for ${year}`);
    
    // Calculate totals and averages
    let totalBitcoin = 0;
    let totalDifficulty = 0;
    
    for (const summary of monthlySummaries) {
      totalBitcoin += Number(summary.bitcoinMined);
      totalDifficulty += Number(summary.averageDifficulty);
    }
    
    const avgDifficulty = totalDifficulty / monthlySummaries.length;
    
    // Check if yearly summary already exists
    const existingSummary = await db
      .select()
      .from(bitcoinYearlySummaries)
      .where(
        and(
          eq(bitcoinYearlySummaries.year, year),
          eq(bitcoinYearlySummaries.minerModel, minerModel)
        )
      );
    
    // Update or insert the yearly summary
    if (existingSummary.length > 0) {
      await db
        .update(bitcoinYearlySummaries)
        .set({
          bitcoinMined: totalBitcoin.toString(),
          valueAtMining: "0", // Set to 0 as this will be calculated with current price when queried
          averageDifficulty: avgDifficulty.toString(),
          updatedAt: new Date()
        })
        .where(
          and(
            eq(bitcoinYearlySummaries.year, year),
            eq(bitcoinYearlySummaries.minerModel, minerModel)
          )
        );
    } else {
      await db
        .insert(bitcoinYearlySummaries)
        .values({
          year,
          minerModel,
          bitcoinMined: totalBitcoin.toString(),
          valueAtMining: "0", // Set to 0 as this will be calculated with current price when queried
          averageDifficulty: avgDifficulty.toString(),
          createdAt: new Date(),
          updatedAt: new Date()
        });
    }
    
    console.log(`Updated yearly summary for ${year}: ${totalBitcoin.toFixed(8)} BTC with ${minerModel}`);
  } catch (error) {
    console.error(`Error calculating yearly summary for ${year}:`, error);
    throw error;
  }
}

/**
 * Manually update yearly Bitcoin summaries for a specific year
 * This recalculates the yearly summary based on the monthly data
 */
async function manualUpdateYearlyBitcoinSummary(year: string): Promise<void> {
  console.log(`\n=== Manual Yearly Bitcoin Summary Update ===`);
  console.log(`Updating summaries for ${year}`);
  
  // Process all miner models
  for (const minerModel of Object.keys(minerModels)) {
    try {
      console.log(`- Processing ${minerModel}`);
      await calculateYearlyBitcoinSummary(year, minerModel);
    } catch (error) {
      console.error(`Error updating ${year} for ${minerModel}:`, error);
    }
  }
  
  // Verify the update was successful
  const summaries = await db
    .select()
    .from(bitcoinYearlySummaries)
    .where(eq(bitcoinYearlySummaries.year, year));
  
  if (summaries.length > 0) {
    console.log(`\nVerification Results for ${year}:`);
    for (const summary of summaries) {
      console.log(`- ${summary.minerModel}: ${Number(summary.bitcoinMined).toFixed(8)} BTC`);
    }
  } else {
    console.log(`No summaries found for ${year} after update attempt`);
  }
  
  console.log(`=== Yearly Summary Update Complete ===\n`);
}

export {
  calculateBitcoinForBMU,
  processHistoricalCalculations,
  processSingleDay,
  fetch2024Difficulties,
  calculateMonthlyBitcoinSummary,
  manualUpdateMonthlyBitcoinSummary,
  calculateYearlyBitcoinSummary,
  manualUpdateYearlyBitcoinSummary
};
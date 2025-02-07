import { format, eachDayOfInterval, isValid, addDays, isBefore } from 'date-fns';
import { minerModels } from '../types/bitcoin';
import { processSingleDay, fetch2024Difficulties } from '../services/bitcoinService';
import { db } from "@db";
import { historicalBitcoinCalculations, curtailmentRecords } from "@db/schema";
import { and, eq, sql } from "drizzle-orm";
import pLimit from 'p-limit';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// ES Module path setup
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const START_DATE = '2024-01-01';
const END_DATE = '2025-02-28'; // Updated to end of February 2025
const BATCH_SIZE = 2; // Process 2 days at a time
const MAX_RETRIES = 3;
const RETRY_DELAY = 2000;
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const PROGRESS_FILE = path.join(__dirname, '..', 'data', 'historical_progress.json');

interface ProcessingProgress {
  lastProcessedDate: string;
  totalProcessed: number;
  failures: Array<{
    date: string;
    minerModel: string;
    error: string;
  }>;
}

async function saveProgress(progress: ProcessingProgress): Promise<void> {
  try {
    const dir = path.dirname(PROGRESS_FILE);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
  } catch (error) {
    console.error('Error saving progress:', error);
  }
}

async function loadProgress(): Promise<ProcessingProgress | null> {
  try {
    if (fs.existsSync(PROGRESS_FILE)) {
      const savedProgress = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf-8'));
      // Ensure we don't resume from a date beyond our new end date
      if (savedProgress.lastProcessedDate > END_DATE) {
        return null;
      }
      return savedProgress;
    }
  } catch (error) {
    console.error('Error loading progress:', error);
  }
  return null;
}

async function getLastProcessedDate(): Promise<string | null> {
  try {
    const lastRecord = await db
      .select()
      .from(historicalBitcoinCalculations)
      .orderBy(sql`settlement_date DESC`)
      .limit(1);

    const lastDate = lastRecord[0]?.settlementDate || null;

    // Don't return dates beyond our end date
    if (lastDate && lastDate > END_DATE) {
      return END_DATE;
    }

    return lastDate;
  } catch (error) {
    console.error('Error getting last processed date:', error);
    return null;
  }
}

async function verifyDayCalculations(date: string, minerModel: string) {
  try {
    const curtailments = await db
      .select({
        count: sql<number>`count(*)::int`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    if (curtailments[0]?.count === 0) {
      console.log(`No curtailment records found for ${date}, marking as complete`);
      return {
        recordCount: 0,
        curtailmentCount: 0,
        difficulty: null,
        totalBitcoin: 0,
        isComplete: true 
      };
    }

    const records = await db
      .select()
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );

    const bitcoinMined = records.reduce((sum, r) => sum + Number(r.bitcoinMined), 0);

    return {
      recordCount: records.length,
      curtailmentCount: curtailments[0]?.count || 0,
      difficulty: records[0]?.difficulty,
      totalBitcoin: bitcoinMined,
      isComplete: records.length > 0 && records[0]?.difficulty !== null
    };
  } catch (error) {
    console.error(`Error verifying calculations for ${date} - ${minerModel}:`, error);
    throw error;
  }
}

async function processBatch(date: Date, progress: ProcessingProgress): Promise<boolean> {
  const formattedDate = format(date, 'yyyy-MM-dd');
  console.log(`\n=== Processing Date: ${formattedDate} ===`);

  let batchSuccess = true;
  let retryQueue: { minerModel: string; retryCount: number }[] = [];

  for (const minerModel of MINER_MODELS) {
    try {
      console.log(`- Processing ${minerModel}`);

      const existingData = await verifyDayCalculations(formattedDate, minerModel);
      if (existingData.isComplete) {
        console.log(`✓ Data already exists for ${minerModel} on ${formattedDate}`);
        progress.totalProcessed++;
        continue;
      }

      let success = false;
      let attempt = 0;

      while (!success && attempt < MAX_RETRIES) {
        try {
          await db.transaction(async (tx) => {
            await processSingleDay(formattedDate, minerModel);
          });
          success = true;
        } catch (error) {
          attempt++;
          console.error(`Attempt ${attempt} failed for ${minerModel} on ${formattedDate}:`, error);

          if (attempt === MAX_RETRIES) {
            throw error;
          }

          await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * Math.pow(2, attempt)));
        }
      }

      const verification = await verifyDayCalculations(formattedDate, minerModel);

      if (!verification.isComplete) {
        console.log(`! Incomplete data for ${minerModel}, will retry`);
        retryQueue.push({ minerModel, retryCount: 0 });
        batchSuccess = false;
      } else {
        console.log(`✓ Completed ${minerModel} for ${formattedDate}:`, {
          records: verification.recordCount,
          curtailments: verification.curtailmentCount,
          totalBitcoin: verification.totalBitcoin.toFixed(8)
        });
      }

      progress.totalProcessed++;
      progress.lastProcessedDate = formattedDate;

      await saveProgress(progress);

    } catch (error) {
      console.error(`× Error processing ${minerModel} for ${formattedDate}:`, error);
      progress.failures.push({
        date: formattedDate,
        minerModel,
        error: error instanceof Error ? error.message : 'Unknown error'
      });
      batchSuccess = false;
      await saveProgress(progress);
    }

    await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
  }

  while (retryQueue.length > 0) {
    const item = retryQueue.shift()!;

    if (item.retryCount >= MAX_RETRIES) {
      console.error(`! Maximum retries reached for ${item.minerModel}`);
      progress.failures.push({
        date: formattedDate,
        minerModel: item.minerModel,
        error: 'Maximum retries exceeded'
      });
      await saveProgress(progress);
      continue;
    }

    try {
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * Math.pow(2, item.retryCount)));
      await processSingleDay(formattedDate, item.minerModel);

      const verification = await verifyDayCalculations(formattedDate, item.minerModel);
      if (!verification.isComplete) {
        retryQueue.push({
          ...item,
          retryCount: item.retryCount + 1
        });
      }
    } catch (error) {
      console.error(`× Error during retry for ${item.minerModel}:`, error);
      if (item.retryCount < MAX_RETRIES - 1) {
        retryQueue.push({
          ...item,
          retryCount: item.retryCount + 1
        });
      } else {
        progress.failures.push({
          date: formattedDate,
          minerModel: item.minerModel,
          error: error instanceof Error ? error.message : 'Retry failed'
        });
        await saveProgress(progress);
      }
    }
  }

  return batchSuccess;
}

export async function updateHistoricalCalculations() {
  try {
    console.log(`\n=== Starting Historical Calculations Update ===`);
    console.log(`Full Date Range: ${START_DATE} to ${END_DATE}`);
    console.log(`Miner Models: ${MINER_MODELS.join(', ')}\n`);

    const startDate = new Date(START_DATE);
    const endDate = new Date(END_DATE);

    if (!isValid(startDate) || !isValid(endDate)) {
      throw new Error(`Invalid date format. Please use YYYY-MM-DD format.`);
    }

    // Try to load saved progress first
    let savedProgress = await loadProgress();
    let currentDate: Date;
    let progress: ProcessingProgress;

    if (savedProgress) {
      currentDate = new Date(savedProgress.lastProcessedDate);
      progress = savedProgress;
      console.log(`Resuming from saved progress: ${format(currentDate, 'yyyy-MM-dd')}`);
    } else {
      const lastProcessedDate = await getLastProcessedDate();
      currentDate = lastProcessedDate ? new Date(lastProcessedDate) : startDate;
      progress = {
        lastProcessedDate: format(currentDate, 'yyyy-MM-dd'),
        totalProcessed: 0,
        failures: []
      };
      console.log(`Starting from date: ${format(currentDate, 'yyyy-MM-dd')}`);
    }

    const totalDays = Math.ceil((endDate.getTime() - currentDate.getTime()) / (24 * 60 * 60 * 1000));
    let daysProcessed = 0;

    // Pre-fetch all difficulties at the start
    console.log('\nPre-fetching difficulties...');
    await fetch2024Difficulties();
    console.log('Difficulties pre-fetch complete\n');

    while (isBefore(currentDate, endDate)) {
      const success = await processBatch(currentDate, progress);
      daysProcessed += BATCH_SIZE;

      if (!success) {
        console.log(`\nWarning: Incomplete processing for batch starting ${format(currentDate, 'yyyy-MM-dd')}`);
      }

      const overallProgress = ((daysProcessed / totalDays) * 100).toFixed(1);
      console.log(`\nOverall Progress: ${overallProgress}% (${daysProcessed}/${totalDays} days)`);

      if (progress.failures.length > 0) {
        console.log('\nFailures:', progress.failures);
      }

      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY * 2));
      currentDate = addDays(currentDate, BATCH_SIZE);
      await saveProgress(progress);
    }

    console.log('\n=== Historical Calculations Update Complete ===');
    console.log('Final Statistics:', {
      daysProcessed,
      totalDays,
      totalFailures: progress.failures.length,
      lastProcessedDate: progress.lastProcessedDate
    });

    if (progress.failures.length > 0) {
      console.log('\nFailed Calculations:', progress.failures);
    }

  } catch (error) {
    console.error('Error during historical calculations update:', error);
    throw error;
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  updateHistoricalCalculations()
    .catch(error => {
      console.error('Script failed:', error);
      process.exit(1);
    });
}
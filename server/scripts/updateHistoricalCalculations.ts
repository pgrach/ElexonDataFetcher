import { format, eachDayOfInterval, isValid, addDays, isBefore } from 'date-fns';
import { minerModels } from '../types/bitcoin';
import { processSingleDay, fetch2024Difficulties } from '../services/bitcoinService';
import { db } from "@db";
import { historicalBitcoinCalculations, curtailmentRecords } from "@db/schema";
import { and, eq, sql } from "drizzle-orm";
import pLimit from 'p-limit';

const START_DATE = '2024-01-01';
const END_DATE = '2025-12-31';
const BATCH_SIZE = 5; // Process 5 days at a time
const MAX_RETRIES = 3;
const RETRY_DELAY = 2000; // 2 seconds between retries
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

interface ProcessingProgress {
  lastProcessedDate: string;
  totalProcessed: number;
  failures: Array<{
    date: string;
    minerModel: string;
    error: string;
  }>;
}

async function getLastProcessedDate(): Promise<string | null> {
  try {
    const lastRecord = await db
      .select()
      .from(historicalBitcoinCalculations)
      .orderBy(historicalBitcoinCalculations.settlementDate)
      .limit(1);

    return lastRecord[0]?.settlementDate || null;
  } catch (error) {
    console.error('Error getting last processed date:', error);
    return null;
  }
}

async function verifyDayCalculations(date: string, minerModel: string) {
  try {
    // First check if there are any curtailment records for this date
    const curtailments = await db
      .select({
        count: sql<number>`count(*)::int`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    // If no curtailment records exist, the day is considered complete
    if (curtailments[0]?.count === 0) {
      console.log(`No curtailment records found for ${date}, marking as complete`);
      return {
        recordCount: 0,
        curtailmentCount: 0,
        difficulty: null,
        totalBitcoin: 0,
        isComplete: true // Mark as complete since there's nothing to process
      };
    }

    // Verify historical calculations exist
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

  // Fetch difficulties once at the start of processing
  await fetch2024Difficulties();

  let batchSuccess = true;
  let retryQueue: { minerModel: string; retryCount: number }[] = [];

  for (const minerModel of MINER_MODELS) {
    try {
      console.log(`- Processing ${minerModel}`);

      // First verify if we already have complete data
      const existingData = await verifyDayCalculations(formattedDate, minerModel);
      if (existingData.isComplete) {
        console.log(`✓ Data already exists for ${minerModel} on ${formattedDate}`);
        progress.totalProcessed++;
        continue;
      }

      // Process with retries
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

      // Verify the calculations after processing
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

    } catch (error) {
      console.error(`× Error processing ${minerModel} for ${formattedDate}:`, error);
      progress.failures.push({
        date: formattedDate,
        minerModel,
        error: error instanceof Error ? error.message : 'Unknown error'
      });
      batchSuccess = false;
    }

    // Add delay between miner models
    await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
  }

  // Handle any remaining retries
  while (retryQueue.length > 0) {
    const item = retryQueue.shift()!;

    if (item.retryCount >= MAX_RETRIES) {
      console.error(`! Maximum retries reached for ${item.minerModel}`);
      progress.failures.push({
        date: formattedDate,
        minerModel: item.minerModel,
        error: 'Maximum retries exceeded'
      });
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

    // Parse and validate dates
    const startDate = new Date(START_DATE);
    const endDate = new Date(END_DATE);

    if (!isValid(startDate) || !isValid(endDate)) {
      throw new Error(`Invalid date format. Please use YYYY-MM-DD format.`);
    }

    // Get the last processed date to enable resume capability
    const lastProcessedDate = await getLastProcessedDate();
    let currentDate = lastProcessedDate ? new Date(lastProcessedDate) : startDate;

    let progress: ProcessingProgress = {
      lastProcessedDate: format(currentDate, 'yyyy-MM-dd'),
      totalProcessed: 0,
      failures: []
    };

    const totalDays = Math.ceil((endDate.getTime() - currentDate.getTime()) / (24 * 60 * 60 * 1000));
    let daysProcessed = 0;

    console.log(`Resuming from date: ${format(currentDate, 'yyyy-MM-dd')}`);

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

      currentDate = addDays(currentDate, BATCH_SIZE);

      // Add cooldown between batches
      await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
    }

    console.log('\n=== Historical Calculations Update Complete ===');
    console.log('Final Statistics:', {
      daysProcessed,
      totalDays,
      totalFailures: progress.failures.length
    });

    if (progress.failures.length > 0) {
      console.log('\nFailed Calculations:', progress.failures);
    }

  } catch (error) {
    console.error('Error during historical calculations update:', error);
    throw error;
  }
}

// Use import.meta.url to check if file is being run directly
if (import.meta.url === `file://${process.argv[1]}`) {
  updateHistoricalCalculations()
    .catch(error => {
      console.error('Script failed:', error);
      process.exit(1);
    });
}
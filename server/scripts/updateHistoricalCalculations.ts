import { format, eachDayOfInterval, isValid, addWeeks, startOfWeek, endOfWeek, isBefore } from 'date-fns';
import { minerModels } from '../types/bitcoin';
import { processSingleDay } from '../services/bitcoinService';
import { db } from "@db";
import { historicalBitcoinCalculations, curtailmentRecords } from "@db/schema";
import { and, eq } from "drizzle-orm";
import pLimit from 'p-limit';

const START_DATE = '2024-01-01';
const END_DATE = '2024-12-31';
const MAX_CONCURRENT_DAYS = 3; // Reduced for better stability
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function verifyDayCalculations(date: string, minerModel: string) {
  try {
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

    // Verify curtailment records exist
    const curtailments = await db
      .select()
      .from(curtailmentRecords)
      .where(
        eq(curtailmentRecords.settlementDate, date)
      );

    const bitcoinMined = records.reduce((sum, r) => sum + Number(r.bitcoinMined), 0);

    return {
      recordCount: records.length,
      curtailmentCount: curtailments.length,
      difficulty: records[0]?.difficulty,
      totalBitcoin: bitcoinMined,
      isComplete: records.length > 0 && records[0]?.difficulty !== null && curtailments.length > 0
    };
  } catch (error) {
    console.error(`Error verifying calculations for ${date} - ${minerModel}:`, error);
    return {
      recordCount: 0,
      curtailmentCount: 0,
      difficulty: null,
      totalBitcoin: 0,
      isComplete: false
    };
  }
}

async function processWeeklyBatch(startDate: Date, endDate: Date) {
  console.log(`\n=== Processing Weekly Batch ===`);
  console.log(`Date Range: ${format(startDate, 'yyyy-MM-dd')} to ${format(endDate, 'yyyy-MM-dd')}`);

  const dateRange = eachDayOfInterval({
    start: startDate,
    end: endDate
  });

  // Create a limit function to control concurrency
  const limit = pLimit(MAX_CONCURRENT_DAYS);
  let retryQueue: { date: string; minerModel: string }[] = [];
  let totalProcessed = 0;
  const totalToProcess = dateRange.length * MINER_MODELS.length;

  console.log(`Total days to process: ${dateRange.length}`);
  console.log(`Total calculations to perform: ${totalToProcess}\n`);

  // Process each date for all miner models
  const processPromises = dateRange.map(date => {
    const formattedDate = format(date, 'yyyy-MM-dd');

    return limit(async () => {
      console.log(`\nProcessing date: ${formattedDate}`);

      for (const minerModel of MINER_MODELS) {
        try {
          console.log(`- Starting calculations for ${minerModel}`);

          // First verify if we already have complete data
          const existingData = await verifyDayCalculations(formattedDate, minerModel);
          if (existingData.isComplete) {
            console.log(`✓ Data already exists for ${minerModel} on ${formattedDate}`);
            totalProcessed++;
            continue;
          }

          // Process the day within a transaction
          try {
            await db.transaction(async (tx) => {
              await processSingleDay(formattedDate, minerModel);
            });
          } catch (error) {
            console.error(`Failed to process date ${formattedDate} in transaction:`, error);
            throw error;
          }

          // Verify the calculations
          const verification = await verifyDayCalculations(formattedDate, minerModel);

          if (!verification.isComplete) {
            console.log(`! Incomplete data for ${minerModel} on ${formattedDate}, adding to retry queue`);
            retryQueue.push({ date: formattedDate, minerModel });
          } else {
            console.log(`✓ Completed ${minerModel} for ${formattedDate}:`, {
              records: verification.recordCount,
              curtailments: verification.curtailmentCount,
              difficulty: verification.difficulty,
              totalBitcoin: verification.totalBitcoin.toFixed(8)
            });
          }

          totalProcessed++;
          const progress = ((totalProcessed / totalToProcess) * 100).toFixed(1);
          console.log(`Progress: ${progress}% (${totalProcessed}/${totalToProcess})`);

        } catch (error) {
          console.error(`× Error processing ${minerModel} for ${formattedDate}:`, error);
          retryQueue.push({ date: formattedDate, minerModel });
        }

        // Add a small delay between iterations to prevent overwhelming DynamoDB
        await new Promise(resolve => setTimeout(resolve, 1000)); // Increased delay
      }
    });
  });

  try {
    await Promise.all(processPromises);
    console.log('\nCompleted initial processing of weekly batch');

    // Handle retry queue
    if (retryQueue.length > 0) {
      console.log(`\nProcessing retry queue (${retryQueue.length} items)...`);
      for (const item of retryQueue) {
        console.log(`Retrying ${item.minerModel} for ${item.date}`);
        try {
          await db.transaction(async (tx) => {
            await processSingleDay(item.date, item.minerModel);
          });

          const verification = await verifyDayCalculations(item.date, item.minerModel);
          if (verification.isComplete) {
            console.log(`✓ Retry successful for ${item.minerModel} on ${item.date}`);
          } else {
            console.error(`! Failed retry for ${item.minerModel} on ${item.date} - incomplete data`);
          }
        } catch (error) {
          console.error(`× Failed retry for ${item.minerModel} on ${item.date}:`, error);
        }
        // Add delay between retries
        await new Promise(resolve => setTimeout(resolve, 2000)); // Increased delay for retries
      }
    }

    return {
      totalProcessed,
      retryQueueSize: retryQueue.length,
      isComplete: retryQueue.length === 0
    };
  } catch (error) {
    console.error('Error during parallel processing:', error);
    throw error;
  }
}

async function updateHistoricalCalculations() {
  try {
    console.log(`\n=== Starting Historical Calculations Update ===`);
    console.log(`Full Date Range: ${START_DATE} to ${END_DATE}`);
    console.log(`Miner Models: ${MINER_MODELS.join(', ')}\n`);

    // Parse and validate dates
    const startDate = new Date(START_DATE);
    const endDate = new Date(END_DATE);

    if (!isValid(startDate) || !isValid(endDate)) {
      throw new Error(`Invalid date format. Please provide dates in YYYY-MM-DD format. Provided dates: ${START_DATE}, ${END_DATE}`);
    }

    let currentWeekStart = startOfWeek(startDate);
    const finalEndDate = endOfWeek(endDate);
    let totalWeeksProcessed = 0;
    const totalWeeks = Math.ceil((finalEndDate.getTime() - currentWeekStart.getTime()) / (7 * 24 * 60 * 60 * 1000));

    while (isBefore(currentWeekStart, finalEndDate)) {
      const currentWeekEnd = endOfWeek(currentWeekStart);
      const weekEndDate = isBefore(currentWeekEnd, finalEndDate) ? currentWeekEnd : finalEndDate;

      console.log(`\n=== Processing Week ${format(currentWeekStart, 'yyyy-MM-dd')} ===`);
      console.log(`Week ${totalWeeksProcessed + 1} of ${totalWeeks}`);

      const result = await processWeeklyBatch(currentWeekStart, weekEndDate);

      totalWeeksProcessed++;
      const weeklyProgress = ((totalWeeksProcessed / totalWeeks) * 100).toFixed(1);
      console.log(`\nOverall Progress: ${weeklyProgress}% (${totalWeeksProcessed}/${totalWeeks} weeks)`);

      currentWeekStart = addWeeks(currentWeekStart, 1);
    }

    console.log('\n=== Historical Calculations Update Complete ===');
    console.log('Final Statistics:', {
      totalWeeksProcessed,
      totalWeeks,
      isComplete: totalWeeksProcessed === totalWeeks
    });
    console.log('\n');

  } catch (error) {
    console.error('Error during historical calculations update:', error);
    process.exit(1);
  }
}

// Start the update process
updateHistoricalCalculations();
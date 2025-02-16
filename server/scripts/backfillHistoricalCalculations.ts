import { format, parseISO, startOfMonth, endOfMonth, eachMonthOfInterval } from 'date-fns';
import { minerModels } from '../types/bitcoin';
import { processSingleDay, calculateMonthlyBitcoinSummary } from '../services/bitcoinService';
import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { and, sql, between } from "drizzle-orm";
import pLimit from 'p-limit';

const START_DATE = '2022-01-01';
const END_DATE = '2023-12-31';
const CONCURRENT_PROCESSES = 1; // Reduced to 1 to avoid DynamoDB throttling
const BATCH_SIZE = 3; // Smaller batches
const BATCH_DELAY = 5000; // 5 second delay between batches
const DATE_DELAY = 2000; // 2 second delay between dates

async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processMonth(yearMonth: string) {
  try {
    console.log(`\n=== Processing month: ${yearMonth} ===`);

    const [year, month] = yearMonth.split('-');
    const startDate = startOfMonth(new Date(parseInt(year), parseInt(month) - 1));
    const endDate = endOfMonth(startDate);

    // Get all dates with curtailment records for this month
    const dates = await db
      .select({
        date: sql<string>`DISTINCT settlement_date::text`
      })
      .from(curtailmentRecords)
      .where(
        and(
          between(
            curtailmentRecords.settlementDate,
            format(startDate, 'yyyy-MM-dd'),
            format(endDate, 'yyyy-MM-dd')
          ),
          sql`ABS(volume::numeric) > 0`
        )
      )
      .orderBy(sql`settlement_date`);

    if (dates.length === 0) {
      console.log(`No curtailment records found for ${yearMonth}`);
      return;
    }

    console.log(`Found ${dates.length} dates with curtailment records`);
    const limit = pLimit(CONCURRENT_PROCESSES);
    const MINER_MODEL_LIST = Object.keys(minerModels);

    // Process in smaller batches
    for (let i = 0; i < dates.length; i += BATCH_SIZE) {
      const batch = dates.slice(i, i + BATCH_SIZE);
      console.log(`\nProcessing batch ${Math.floor(i/BATCH_SIZE) + 1}/${Math.ceil(dates.length/BATCH_SIZE)}`);

      // Process each date in the batch sequentially
      for (const { date } of batch) {
        // Process one miner model at a time
        for (const minerModel of MINER_MODEL_LIST) {
          try {
            const progress = (((i + batch.indexOf({ date }) + 1) / dates.length) * 100).toFixed(1);
            console.log(`[${progress}%] Processing ${date} for ${minerModel}...`);

            await processSingleDay(date, minerModel);
            console.log(`✓ Completed ${date} for ${minerModel}`);

            // Add delay between miner models
            await sleep(DATE_DELAY);
          } catch (error) {
            console.error(`Error processing ${date} for ${minerModel}:`, error);
            // Continue with next model even if one fails
          }
        }
      }

      // Longer delay between batches
      console.log(`Waiting ${BATCH_DELAY/1000}s before next batch...`);
      await sleep(BATCH_DELAY);
    }

    // Calculate monthly summary after processing all days
    console.log(`\nCalculating monthly summaries for ${yearMonth}...`);
    for (const minerModel of MINER_MODEL_LIST) {
      try {
        console.log(`\nCalculating summary for ${yearMonth} ${minerModel}...`);
        await calculateMonthlyBitcoinSummary(yearMonth, minerModel);
        console.log(`✓ Summary complete for ${yearMonth} ${minerModel}`);
        await sleep(DATE_DELAY); // Add delay between summary calculations
      } catch (error) {
        console.error(`Error calculating summary for ${yearMonth} ${minerModel}:`, error);
      }
    }

    console.log(`\n=== Completed month: ${yearMonth} ===`);
  } catch (error) {
    console.error(`Error processing month ${yearMonth}:`, error);
  }
}

async function backfillHistoricalCalculations() {
  try {
    console.log('\n=== Starting Historical Bitcoin Calculations Backfill (2022-2023) ===');
    console.log(`Processing range: ${START_DATE} to ${END_DATE}\n`);

    // Generate list of months to process
    const months = eachMonthOfInterval({
      start: new Date(START_DATE),
      end: new Date(END_DATE)
    }).map(date => format(date, 'yyyy-MM'));

    console.log(`Found ${months.length} months to process:`, months);

    // Process each month sequentially
    for (const month of months) {
      await processMonth(month);
      // Add a longer delay between months
      await sleep(10000); // 10 second delay between months
    }

    console.log('\n=== Historical Calculations Backfill Complete ===');

  } catch (error) {
    console.error('Error during backfill:', error);
    process.exit(1);
  }
}

// Start the backfill process
backfillHistoricalCalculations();
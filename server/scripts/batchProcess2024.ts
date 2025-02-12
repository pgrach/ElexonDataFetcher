import { format, eachDayOfInterval } from 'date-fns';
import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { processHistoricalCalculations, fetch2024Difficulties } from '../services/bitcoinService';
import { eq, sql } from 'drizzle-orm';
import pLimit from 'p-limit';

const START_DATE = '2024-01-01';
const END_DATE = '2024-12-31';
const BATCH_SIZE = 100; // Reduced from 300 to prevent overwhelming DynamoDB
const MAX_CONCURRENT_DATES = 15; // Reduced from 25 to manage resource usage
const DELAY_BETWEEN_BATCHES = 500; // Reduced delay since we've already cached difficulties

async function findDatesWithData() {
  console.log('Finding settlement periods with curtailment data...');

  const dates = await db
    .select({
      date: curtailmentRecords.settlementDate,
      periodCount: sql<number>`count(distinct settlement_period)::int`,
      totalCount: sql<number>`count(*)::int`,
      totalVolume: sql<number>`sum(abs(volume))::float`
    })
    .from(curtailmentRecords)
    .where(sql`settlement_date between ${START_DATE} and ${END_DATE}`)
    .groupBy(curtailmentRecords.settlementDate)
    .having(sql`count(*) > 0`)
    .orderBy(curtailmentRecords.settlementDate);

  const totalPeriods = dates.reduce((sum, d) => sum + d.periodCount, 0);
  const totalRecords = dates.reduce((sum, d) => sum + d.totalCount, 0);
  const totalVolume = dates.reduce((sum, d) => sum + d.totalVolume, 0);

  console.log(`Found ${dates.length} dates with ${totalPeriods} settlement periods`);
  console.log(`Total records: ${totalRecords}, Total volume: ${totalVolume.toFixed(2)} MWh`);

  return dates.map(d => d.date);
}

async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processBatch() {
  try {
    const datesWithData = await findDatesWithData();

    if (datesWithData.length === 0) {
      console.log('No dates with curtailment data found in the specified range');
      return;
    }

    // Fetch all difficulties upfront
    console.log('Fetching all 2024 difficulty data...');
    await fetch2024Difficulties();
    console.log('Difficulty data cached, proceeding with batch processing...');

    const limit = pLimit(MAX_CONCURRENT_DATES);
    let processedCount = 0;
    let failedDates: string[] = [];
    const startTime = Date.now();

    // Process dates in smaller batches
    for (let i = 0; i < datesWithData.length; i += BATCH_SIZE) {
      const batchDates = datesWithData.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i/BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(datesWithData.length/BATCH_SIZE);
      const batchStartTime = Date.now();

      console.log(`\nProcessing batch ${batchNumber}/${totalBatches} (${batchDates.length} dates)`);

      try {
        // Process dates in parallel with reduced concurrency
        const promises = batchDates.map(date => 
          limit(async () => {
            try {
              await processHistoricalCalculations(date, date);
              processedCount++;
              const progress = ((processedCount / datesWithData.length) * 100).toFixed(1);
              const elapsedTime = ((Date.now() - startTime) / 1000).toFixed(1);
              console.log(`✓ Completed ${date} (Progress: ${progress}%, Time: ${elapsedTime}s)`);
            } catch (error) {
              console.error(`× Failed to process ${date}:`, error);
              failedDates.push(date);
            }
          })
        );

        await Promise.all(promises);
        const batchTime = ((Date.now() - batchStartTime) / 1000).toFixed(1);
        console.log(`\nCompleted batch ${batchNumber}/${totalBatches} in ${batchTime}s`);

        // Add shorter delay between batches since we're not fetching difficulties anymore
        if (i + BATCH_SIZE < datesWithData.length) {
          await sleep(DELAY_BETWEEN_BATCHES);
        }

      } catch (error) {
        console.error(`Error processing batch ${batchNumber}:`, error);
        await sleep(DELAY_BETWEEN_BATCHES * 2); // Double delay on error
        continue;
      }
    }

    const totalTime = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log('\nBatch processing complete!');
    console.log(`Successfully processed ${processedCount} dates in ${totalTime} seconds`);

    if (failedDates.length > 0) {
      console.log(`Failed to process ${failedDates.length} dates:`, failedDates);
    }

  } catch (error) {
    console.error('Error during batch processing:', error);
    process.exit(1);
  }
}

console.log(`Starting batch process for date range: ${START_DATE} to ${END_DATE}`);
console.log(`Processing with concurrency limit of ${MAX_CONCURRENT_DATES} dates`);

// Run the process
processBatch()
  .then(() => console.log('Processing complete'))
  .catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
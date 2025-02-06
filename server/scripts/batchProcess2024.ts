import { format, eachDayOfInterval } from 'date-fns';
import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { processHistoricalCalculations } from '../services/bitcoinService';
import { eq, sql } from 'drizzle-orm';
import pLimit from 'p-limit';

const START_DATE = '2024-01-01';
const END_DATE = '2024-12-31'; // Process full year
const BATCH_SIZE = 30; // Process a month at a time
const MAX_CONCURRENT_DATES = 5; // Process 5 dates concurrently

async function findDatesWithData() {
  console.log('Finding dates with curtailment data...');

  const dates = await db
    .select({
      date: curtailmentRecords.settlementDate,
      count: sql<number>`count(*)::int`
    })
    .from(curtailmentRecords)
    .where(sql`settlement_date between ${START_DATE} and ${END_DATE}`)
    .groupBy(curtailmentRecords.settlementDate)
    .having(sql`count(*) > 0`)
    .orderBy(curtailmentRecords.settlementDate);

  return dates.map(d => d.date);
}

async function processBatch() {
  try {
    // Only get dates that actually have curtailment data
    const datesWithData = await findDatesWithData();
    console.log(`Found ${datesWithData.length} dates with curtailment data`);

    if (datesWithData.length === 0) {
      console.log('No dates with curtailment data found in the specified range');
      return;
    }

    // Create a concurrency limit for processing dates
    const limit = pLimit(MAX_CONCURRENT_DATES);

    // Process dates in batches
    for (let i = 0; i < datesWithData.length; i += BATCH_SIZE) {
      const batchDates = datesWithData.slice(i, i + BATCH_SIZE);
      console.log(`\nProcessing batch ${Math.floor(i/BATCH_SIZE) + 1}:`, 
        batchDates.map(d => format(new Date(d), 'yyyy-MM-dd')).join(', '));

      // Process all dates in this batch concurrently with limits
      const promises = batchDates.map(date => {
        return limit(async () => {
          const formattedDate = format(new Date(date), 'yyyy-MM-dd');
          try {
            await processHistoricalCalculations(formattedDate, formattedDate);
            console.log(`✓ Completed processing for ${formattedDate}`);
          } catch (error) {
            console.error(`× Failed to process ${formattedDate}:`, error);
            throw error;
          }
        });
      });

      // Wait for all dates in the batch to complete
      await Promise.all(promises);
      console.log(`\nCompleted batch ${Math.floor(i/BATCH_SIZE) + 1}`);

      // Add a small delay between batches to prevent overwhelming services
      if (i + BATCH_SIZE < datesWithData.length) {
        console.log('Waiting 5 seconds before next batch...');
        await new Promise(resolve => setTimeout(resolve, 5000));
      }
    }

    console.log('\nBatch processing complete!');

  } catch (error) {
    console.error('Error during batch processing:', error);
    process.exit(1);
  }
}

console.log(`Starting batch process for date range: ${START_DATE} to ${END_DATE}`);
console.log(`Processing with concurrency limit of ${MAX_CONCURRENT_DATES} dates`);

processBatch()
  .then(() => console.log('Processing complete'))
  .catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
import { format, eachDayOfInterval } from 'date-fns';
import { reprocessDay } from '../services/historicalReconciliation';
import { db } from "@db";
import { historicalBitcoinCalculations } from "@db/schema";
import { eq, sql } from "drizzle-orm";

const START_DATE = '2022-01-01';
const END_DATE = '2023-12-31';  // Changed date range to cover 2022-2023
const BATCH_SIZE = 5;
const FORCE_REPROCESS = true; // Force reprocess to ensure all dates are covered

async function processBatch(dates: string[]) {
  console.log(`Processing batch of ${dates.length} dates:`, dates);

  const results = {
    success: [] as string[],
    failed: [] as string[],
    skipped: [] as string[]
  };

  for (const date of dates) {
    try {
      console.log(`\nProcessing ${date}...`);
      await reprocessDay(date);
      results.success.push(date);
    } catch (error) {
      console.error(`Error processing ${date}:`, error);
      results.failed.push(date);
      // Continue with next date even if one fails
    }
  }

  return results;
}

async function batchReprocess() {
  try {
    const startDate = new Date(START_DATE);
    const endDate = new Date(END_DATE);

    console.log(`\n=== Starting Batch Reprocessing ===`);
    console.log(`Date Range: ${START_DATE} to ${END_DATE}`);
    console.log(`Force Reprocess: ${FORCE_REPROCESS}\n`);

    const dateRange = eachDayOfInterval({ start: startDate, end: endDate });
    const formattedDates = dateRange.map(date => format(date, 'yyyy-MM-dd'));

    const totalResults = {
      success: [] as string[],
      failed: [] as string[],
      skipped: [] as string[]
    };

    // Process dates in batches
    for (let i = 0; i < formattedDates.length; i += BATCH_SIZE) {
      const batch = formattedDates.slice(i, i + BATCH_SIZE);
      const batchResults = await processBatch(batch);

      totalResults.success.push(...batchResults.success);
      totalResults.failed.push(...batchResults.failed);
      totalResults.skipped.push(...batchResults.skipped);

      // Progress update
      const processed = Math.min(i + BATCH_SIZE, formattedDates.length);
      const progress = ((processed / formattedDates.length) * 100).toFixed(1);
      console.log(`\nProgress: ${progress}% (${processed}/${formattedDates.length} dates)`);
      console.log(`Success: ${totalResults.success.length}, Failed: ${totalResults.failed.length}, Skipped: ${totalResults.skipped.length}`);
    }

    console.log('\n=== Batch Reprocessing Complete ===');
    console.log(`Total Successful: ${totalResults.success.length}`);
    console.log(`Total Failed: ${totalResults.failed.length}`);
    console.log(`Total Skipped: ${totalResults.skipped.length}`);
    if (totalResults.failed.length > 0) {
      console.log('Failed dates:', totalResults.failed);
    }
    console.log('\n');

  } catch (error) {
    console.error('Error during batch reprocessing:', error);
    process.exit(1);
  }
}

// Start the batch reprocessing
batchReprocess();
import { eachDayOfInterval, format, parseISO } from "date-fns";
import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries, ingestionProgress } from "@db/schema";
import { eq, desc } from "drizzle-orm";

const API_CALL_DELAY = 2000; // 2 seconds between API calls
const DAY_DELAY = 45000; // 45 seconds between days
const MAX_RETRIES = 3;
const RETRY_DELAY = 15000; // 15 seconds between retries

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function getLastProcessedDate(): Promise<string | null> {
  const progress = await db.query.ingestionProgress.findFirst({
    orderBy: [desc(ingestionProgress.lastProcessedDate)]
  });

  return progress ? format(progress.lastProcessedDate, 'yyyy-MM-dd') : null;
}

async function updateProgress(date: string, status: 'completed' | 'in_progress' | 'failed', errorMessage?: string) {
  await db.insert(ingestionProgress).values({
    lastProcessedDate: date,
    status,
    errorMessage,
    updatedAt: new Date()
  });
}

async function processDay(dateStr: string, retryCount = 0): Promise<boolean> {
  try {
    // Check if we already have data for this date
    const existingData = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, dateStr)
    });

    if (existingData) {
      console.log(`✓ Data already exists for ${dateStr}, skipping...`);
      await updateProgress(dateStr, 'completed');
      return true;
    }

    console.log(`\nStarting processing for ${dateStr} (Attempt ${retryCount + 1}/${MAX_RETRIES + 1})`);
    await updateProgress(dateStr, 'in_progress');
    await processDailyCurtailment(dateStr);
    await updateProgress(dateStr, 'completed');
    console.log(`✓ Successfully processed ${dateStr}`);
    return true;
  } catch (error) {
    console.error(`Error processing ${dateStr} (attempt ${retryCount + 1}):`, error);

    if (retryCount < MAX_RETRIES) {
      console.log(`Retrying ${dateStr} in ${RETRY_DELAY/1000} seconds...`);
      await delay(RETRY_DELAY);
      return processDay(dateStr, retryCount + 1);
    }

    await updateProgress(dateStr, 'failed', (error as Error).message);
    console.error(`Failed to process ${dateStr} after ${MAX_RETRIES + 1} attempts`);
    return false;
  }
}

async function ingestHistoricalData() {
  try {
    // Process just one day at a time
    const remainingDays = [
      "2024-11-07", // Next day to process
      "2024-11-08", "2024-11-09", "2024-11-10",
      "2024-11-11", "2024-11-12", "2024-11-13",
      "2024-11-14", "2024-11-15", "2024-11-16",
      "2024-11-17", "2024-11-18", "2024-11-19",
      "2024-11-20", "2024-11-21", "2024-11-22",
      "2024-11-23", "2024-11-24", "2024-11-25",
      "2024-11-26", "2024-11-27", "2024-11-28",
      "2024-11-29", "2024-11-30"
    ];

    console.log('\n=== Starting November 2024 Data Ingestion ===');
    console.log(`Total days to process: ${remainingDays.length}`);
    console.log('=======================================\n');

    const lastProcessed = await getLastProcessedDate();
    let startFromIndex = 0;

    if (lastProcessed) {
      startFromIndex = remainingDays.findIndex(date => date > lastProcessed);
      if (startFromIndex === -1) startFromIndex = remainingDays.length;
      console.log(`Resuming from ${remainingDays[startFromIndex]} (after ${lastProcessed})`);
    }

    // Process one day at a time
    for (let i = startFromIndex; i < remainingDays.length; i++) {
      const dateStr = remainingDays[i];
      const progress = Math.round(((i + 1) / remainingDays.length) * 100);

      console.log(`\n=== Processing day ${i + 1}/${remainingDays.length} (${progress}%) ===`);
      console.log(`Current date: ${dateStr}`);
      console.log('=======================================\n');

      const success = await processDay(dateStr);

      if (success) {
        console.log(`\nWaiting ${DAY_DELAY/1000} seconds before next day...\n`);
        await delay(DAY_DELAY);
      } else {
        console.log(`\nSkipping to next day after failure...\n`);
        await delay(DAY_DELAY * 2); // Double delay after failures
      }
    }

    console.log('\n=== November 2024 Data Ingestion Completed ===');
    console.log('All remaining days have been processed');
    console.log('===========================================\n');
  } catch (error) {
    console.error('Fatal error during ingestion:', error);
    process.exit(1);
  }
}

// Run the ingestion for November
ingestHistoricalData();

export { ingestHistoricalData };
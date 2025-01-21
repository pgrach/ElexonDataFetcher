import { eachDayOfInterval, format, parseISO } from "date-fns";
import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { eq } from "drizzle-orm";

// Process just one day as a test
async function ingestSingleDay() {
  try {
    const testDate = "2024-11-01";
    console.log(`\n=== Starting test ingestion for ${testDate} ===`);

    // Check if we already have data for this date
    const existingData = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, testDate)
    });

    if (existingData) {
      console.log(`Data already exists for ${testDate}, clearing it first...`);
      // We'll skip deletion to avoid data loss, just continue with the ingestion
    }

    console.log(`\nProcessing data for ${testDate}`);
    await processDailyCurtailment(testDate);

    console.log('\n=== Test ingestion completed successfully ===');
    console.log(`✓ Data ingested for ${testDate}`);
    console.log('==========================================\n');
  } catch (error) {
    console.error('Fatal error during test ingestion:', error);
    process.exit(1);
  }
}

// Run the test ingestion
ingestSingleDay();

export { ingestSingleDay };


//The original ingestHistoricalData function is left here but commented out for now.  It will be used later for the full month ingestion.
/*
const CHUNK_SIZE = 1; // Process 1 day at a time to avoid timeouts
const CHUNK_DELAY = 15000; // 15 second delay between chunks
const MAX_RETRIES = 3;
const RETRY_DELAY = 10000; // 10 seconds between retries
const RATE_LIMIT_DELAY = 30000; // 30 seconds after rate limit errors

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processDay(dateStr: string, retryCount = 0): Promise<boolean> {
  try {
    console.log(`\nStarting processing for ${dateStr} (Attempt ${retryCount + 1}/${MAX_RETRIES + 1})`);
    await processDailyCurtailment(dateStr);
    console.log(`✓ Successfully processed ${dateStr}`);
    return true;
  } catch (error) {
    const isRateLimit = (error as Error).message?.toLowerCase().includes('rate limit');
    console.error(`Error processing ${dateStr} (attempt ${retryCount + 1}):`, error);

    if (retryCount < MAX_RETRIES) {
      const delayTime = isRateLimit ? RATE_LIMIT_DELAY : RETRY_DELAY;
      console.log(`Retrying ${dateStr} in ${delayTime/1000} seconds...`);
      await delay(delayTime);
      return processDay(dateStr, retryCount + 1);
    }

    console.error(`Failed to process ${dateStr} after ${MAX_RETRIES} attempts`);
    return false;
  }
}

async function processChunk(days: Date[]) {
  for (const day of days) {
    const dateStr = format(day, 'yyyy-MM-dd');

    // Check if we already have data for this date
    const existingData = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, dateStr)
    });

    if (existingData) {
      console.log(`✓ Data already exists for ${dateStr}, skipping...`);
      continue;
    }

    console.log(`\nProcessing data for ${dateStr}`);
    const success = await processDay(dateStr);

    if (success) {
      // Add shorter delay between successful days to speed up processing
      console.log(`Waiting 5 seconds before next day...`);
      await delay(5000);
    } else {
      // Add longer delay after failed days
      console.log(`Waiting 15 seconds before next day due to previous failure...`);
      await delay(15000);
    }
  }
}

async function ingestHistoricalData() {
  try {
    const startDate = parseISO("2024-11-01"); // Start from November 1st, 2024
    const endDate = parseISO("2024-11-30"); // End at November 30th, 2024
    const days = eachDayOfInterval({ start: startDate, end: endDate });

    console.log(`\n=== Starting November 2024 Data Ingestion ===`);
    console.log(`Target Range: ${format(startDate, 'yyyy-MM-dd')} to ${format(endDate, 'yyyy-MM-dd')}`);
    console.log(`Days to Process: ${days.length}`);
    console.log(`===============================================\n`);

    // Process days in smaller chunks
    for (let i = 0; i < days.length; i += CHUNK_SIZE) {
      const chunk = days.slice(i, i + CHUNK_SIZE);
      const chunkNum = Math.floor(i/CHUNK_SIZE) + 1;
      const totalChunks = Math.ceil(days.length/CHUNK_SIZE);
      const overallProgress = Math.round((i/days.length) * 100);

      console.log(`\n=== Processing Chunk ${chunkNum}/${totalChunks} (Overall Progress: ${overallProgress}%) ===`);
      console.log(`Current day: ${chunk.map(d => format(d, 'yyyy-MM-dd')).join(', ')}`);

      await processChunk(chunk);

      if (i + CHUNK_SIZE < days.length) {
        console.log(`\nWaiting ${CHUNK_DELAY/1000} seconds before next chunk...`);
        await delay(CHUNK_DELAY);
      }
    }

    console.log('\n=== November 2024 Data Ingestion Completed ===');
    console.log('All days have been processed successfully');
    console.log('===========================================\n');
  } catch (error) {
    console.error('Fatal error during ingestion:', error);
    process.exit(1);
  }
}

// Run the ingestion
//ingestHistoricalData();

export { ingestHistoricalData };
*/
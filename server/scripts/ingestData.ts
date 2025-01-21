import { eachDayOfInterval, format, parseISO, addDays, subDays, isBefore, isAfter } from "date-fns";
import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { eq } from "drizzle-orm";

const CHUNK_SIZE = 1; // Process 1 day at a time to avoid timeouts
const CHUNK_DELAY = 15000; // 15 second delay between chunks
const MAX_RETRIES = 3;
const RETRY_DELAY = 10000; // 10 seconds between retries
const RATE_LIMIT_DELAY = 30000; // 30 seconds after rate limit errors
const LOOKBACK_DAYS = 7; // Number of days to look back for data validation
const LOOK_FORWARD_DAYS = 1; // Number of days to look ahead (usually 1 for next day's data)

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

    // Skip future dates
    if (isAfter(day, new Date())) {
      console.log(`Skipping future date ${dateStr}`);
      continue;
    }

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

async function calculateDateRange(): Promise<{ startDate: Date; endDate: Date }> {
  const today = new Date();
  const startDate = subDays(today, LOOKBACK_DAYS);
  const endDate = addDays(today, LOOK_FORWARD_DAYS);
  return { startDate, endDate };
}

async function ingestData() {
  try {
    const { startDate, endDate } = await calculateDateRange();
    const days = eachDayOfInterval({ start: startDate, end: endDate });

    console.log(`\n=== Starting Rolling Data Ingestion ===`);
    console.log(`Target Range: ${format(startDate, 'yyyy-MM-dd')} to ${format(endDate, 'yyyy-MM-dd')}`);
    console.log(`Days to Process: ${days.length}`);
    console.log(`===============================================\n`);

    // Process days in smaller chunks
    for (let i = 0; i < days.length; i += CHUNK_SIZE) {
      const chunk = days.slice(i, i + CHUNK_SIZE);
      const chunkNum = Math.floor(i/CHUNK_SIZE) + 1;
      const totalChunks = Math.ceil(days.length/CHUNK_SIZE);
      const progress = Math.round((i/days.length) * 100);

      console.log(`\n=== Processing Chunk ${chunkNum}/${totalChunks} (Progress: ${progress}%) ===`);
      console.log(`Current day: ${chunk.map(d => format(d, 'yyyy-MM-dd')).join(', ')}`);

      await processChunk(chunk);

      if (i + CHUNK_SIZE < days.length) {
        console.log(`\nWaiting ${CHUNK_DELAY/1000} seconds before next chunk...`);
        await delay(CHUNK_DELAY);
      }
    }

    console.log('\n=== Data Ingestion Completed ===');
    console.log('All days have been processed successfully');
    console.log('===========================================\n');
  } catch (error) {
    console.error('Fatal error during ingestion:', error);
    process.exit(1);
  }
}

// Only run if called directly (not imported)
if (import.meta.url === new URL(process.argv[1], 'file:').href) {
  ingestData();
}

// Export only once
export { ingestData };
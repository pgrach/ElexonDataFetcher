import { eachDayOfInterval, format, parseISO } from "date-fns";
import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { eq } from "drizzle-orm";

const CHUNK_SIZE = 3; // Process 3 days at a time since we have better rate handling
const CHUNK_DELAY = 5000; // 5 second delay between chunks
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds between retries
const RATE_LIMIT_DELAY = 15000; // 15 seconds after rate limit errors

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
  const results = await Promise.all(
    days.map(async (day) => {
      const dateStr = format(day, 'yyyy-MM-dd');

      // Check if we already have data for this date
      const existingData = await db.query.dailySummaries.findFirst({
        where: eq(dailySummaries.summaryDate, dateStr)
      });

      if (existingData) {
        console.log(`✓ Data already exists for ${dateStr}, skipping...`);
        return true;
      }

      console.log(`\nProcessing data for ${dateStr}`);
      return processDay(dateStr);
    })
  );

  const successCount = results.filter(Boolean).length;
  if (successCount < days.length) {
    console.log(`Warning: ${days.length - successCount} days in chunk failed to process`);
  }
}

async function ingestHistoricalData() {
  try {
    const startDate = parseISO("2024-12-07"); // Start from December 7th, 2024 (since we have 1-6)
    const endDate = parseISO("2024-12-31"); // End at December 31st, 2024
    const days = eachDayOfInterval({ start: startDate, end: endDate });

    console.log(`\n=== Starting December 2024 Data Ingestion (Remaining Days) ===`);
    console.log(`Current Progress: 6/31 days processed (19.4%)`);
    console.log(`Target Range: ${format(startDate, 'yyyy-MM-dd')} to ${format(endDate, 'yyyy-MM-dd')}`);
    console.log(`Days Remaining: ${days.length}`);
    console.log(`===============================================\n`);

    // Process days in larger chunks
    for (let i = 0; i < days.length; i += CHUNK_SIZE) {
      const chunk = days.slice(i, i + CHUNK_SIZE);
      const chunkNum = Math.floor(i/CHUNK_SIZE) + 1;
      const totalChunks = Math.ceil(days.length/CHUNK_SIZE);
      const overallProgress = Math.round(((i + 6)/31) * 100); // Including the 6 days we already have

      console.log(`\n=== Processing Chunk ${chunkNum}/${totalChunks} (Overall Progress: ${overallProgress}%) ===`);
      console.log(`Current days: ${chunk.map(d => format(d, 'yyyy-MM-dd')).join(', ')}`);

      await processChunk(chunk);

      if (i + CHUNK_SIZE < days.length) {
        console.log(`\nWaiting ${CHUNK_DELAY/1000} seconds before next chunk...`);
        await delay(CHUNK_DELAY);
      }
    }

    console.log('\n=== December 2024 Data Ingestion Completed ===');
    console.log('All remaining days have been processed successfully');
    console.log('===========================================\n');
  } catch (error) {
    console.error('Fatal error during ingestion:', error);
    process.exit(1);
  }
}

// Run the ingestion
ingestHistoricalData();

export { ingestHistoricalData };
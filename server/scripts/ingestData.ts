import { eachDayOfInterval, format, parseISO } from "date-fns";
import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { eq } from "drizzle-orm";

// Reduced concurrent API calls and increased delays
const CHUNK_SIZE = 2; // Process 2 days at a time to avoid rate limits
const CHUNK_DELAY = 30000; // 30 second delay between chunks
const MAX_RETRIES = 3;
const RETRY_DELAY = 20000; // 20 seconds between retries
const RATE_LIMIT_DELAY = 60000; // 60 seconds after rate limit errors

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processDay(dateStr: string, retryCount = 0): Promise<boolean> {
  try {
    console.log(`\nStarting processing for ${dateStr} (Attempt ${retryCount + 1}/${MAX_RETRIES + 1})`);

    // Add delay between API calls within the same day
    const result = await processDailyCurtailment(dateStr);

    // Verify the data was actually saved
    const savedData = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, dateStr)
    });

    if (!savedData || (Number(savedData.totalCurtailedEnergy) === 0 && Number(savedData.totalPayment) === 0)) {
      throw new Error('Data validation failed: No data or zero values saved');
    }

    console.log(`✓ Successfully processed ${dateStr} with values:`, {
      energy: savedData.totalCurtailedEnergy,
      payment: savedData.totalPayment
    });

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

    console.error(`Failed to process ${dateStr} after ${MAX_RETRIES + 1} attempts`);
    return false;
  }
}

async function processChunk(days: Date[]) {
  // Process days sequentially within chunk to avoid overwhelming the API
  for (const day of days) {
    const dateStr = format(day, 'yyyy-MM-dd');

    // Remove existing data for this date if it exists
    await db.delete(dailySummaries).where(eq(dailySummaries.summaryDate, dateStr));
    console.log(`\nProcessing data for ${dateStr}`);

    const success = await processDay(dateStr);

    if (!success) {
      throw new Error(`Failed to process ${dateStr}`);
    }

    // Add delay between days within chunk
    await delay(5000);
  }
}

async function ingestHistoricalData() {
  try {
    const startDate = parseISO("2024-12-08"); // Start from December 8th (where data is missing)
    const endDate = parseISO("2024-12-31");
    const days = eachDayOfInterval({ start: startDate, end: endDate });

    console.log(`\n=== Starting December 2024 Data Re-Ingestion ===`);
    console.log(`Target Range: ${format(startDate, 'yyyy-MM-dd')} to ${format(endDate, 'yyyy-MM-dd')}`);
    console.log(`Days to Process: ${days.length}`);
    console.log(`Processing Strategy: ${CHUNK_SIZE} days at a time`);
    console.log(`===============================================\n`);

    // Process days in chunks
    for (let i = 0; i < days.length; i += CHUNK_SIZE) {
      const chunk = days.slice(i, i + CHUNK_SIZE);
      const chunkNum = Math.floor(i/CHUNK_SIZE) + 1;
      const totalChunks = Math.ceil(days.length/CHUNK_SIZE);
      const progress = Math.round((i/days.length) * 100);

      console.log(`\n=== Processing Chunk ${chunkNum}/${totalChunks} (Progress: ${progress}%) ===`);
      console.log(`Days in chunk: ${chunk.map(d => format(d, 'yyyy-MM-dd')).join(', ')}`);

      try {
        await processChunk(chunk);
      } catch (error) {
        console.error(`Error processing chunk ${chunkNum}:`, error);
        // Wait longer after a chunk failure before trying the next chunk
        await delay(RATE_LIMIT_DELAY);
        // Instead of continue, throw error to stop processing
        throw error;
      }

      if (i + CHUNK_SIZE < days.length) {
        console.log(`\nWaiting ${CHUNK_DELAY/1000} seconds before next chunk...`);
        await delay(CHUNK_DELAY);
      }
    }

    console.log('\n=== December 2024 Data Re-Ingestion Completed ===');
    console.log('All days have been processed');
    console.log('===========================================\n');

    // Final verification
    const results = await db.query.dailySummaries.findMany({
      where: eq(dailySummaries.totalCurtailedEnergy, "0")
    });

    if (results.length > 0) {
      console.log(`Warning: Found ${results.length} days with zero values:`, 
        results.map(r => r.summaryDate).join(', ')
      );
    }

  } catch (error) {
    console.error('Fatal error during ingestion:', error);
    process.exit(1);
  }
}

// Run the ingestion
ingestHistoricalData();

export { ingestHistoricalData };
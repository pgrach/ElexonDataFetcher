import { eachDayOfInterval, format, parseISO } from "date-fns";
import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { eq } from "drizzle-orm";

// Process 2 days at a time to avoid timeouts while maintaining good throughput
const CHUNK_SIZE = 2;
const CHUNK_DELAY = 10000; // 10 second delay between chunks
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds between retries
const RATE_LIMIT_DELAY = 15000; // 15 seconds after rate limit errors

// Specific dates that need reprocessing
const PROBLEM_DATES = [
  "2024-12-12",
  "2024-12-11",
  "2024-12-28",
  "2024-12-29"
];

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processDay(dateStr: string, retryCount = 0): Promise<boolean> {
  try {
    console.log(`\nStarting processing for ${dateStr} (Attempt ${retryCount + 1}/${MAX_RETRIES + 1})`);
    await processDailyCurtailment(dateStr);

    // Verify the data was actually ingested properly
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, dateStr)
    });

    // If summary exists but has zero values, consider it a failed ingestion
    if (summary && Number(summary.totalCurtailedEnergy) === 0 && Number(summary.totalPayment) === 0) {
      console.log(`Warning: Zero values detected for ${dateStr}, will retry...`);
      throw new Error('Zero values detected in summary');
    }

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

    console.error(`Failed to process ${dateStr} after ${MAX_RETRIES + 1} attempts`);
    return false;
  }
}

async function processChunk(dates: string[]) {
  console.log(`Processing dates: ${dates.join(', ')}`);

  const results = await Promise.all(
    dates.map(async (dateStr) => {
      // Always process these problem dates
      console.log(`Processing ${dateStr}...`);
      return processDay(dateStr);
    })
  );

  const successCount = results.filter(Boolean).length;
  if (successCount < dates.length) {
    console.log(`Warning: ${dates.length - successCount} dates in chunk failed to process`);
  }
}

async function reprocessProblemDates() {
  try {
    console.log(`\n=== Starting Reprocessing of Problem Dates ===`);
    console.log(`Dates to process: ${PROBLEM_DATES.join(', ')}`);
    console.log(`===============================================\n`);

    // Process dates in chunks
    for (let i = 0; i < PROBLEM_DATES.length; i += CHUNK_SIZE) {
      const chunk = PROBLEM_DATES.slice(i, i + CHUNK_SIZE);
      const chunkNum = Math.floor(i/CHUNK_SIZE) + 1;
      const totalChunks = Math.ceil(PROBLEM_DATES.length/CHUNK_SIZE);
      const progress = Math.round((i/PROBLEM_DATES.length) * 100);

      console.log(`\n=== Processing Chunk ${chunkNum}/${totalChunks} (Progress: ${progress}%) ===`);
      await processChunk(chunk);

      if (i + CHUNK_SIZE < PROBLEM_DATES.length) {
        console.log(`\nWaiting ${CHUNK_DELAY/1000} seconds before next chunk...`);
        await delay(CHUNK_DELAY);
      }
    }

    console.log('\n=== Problem Dates Reprocessing Completed ===');
    console.log('All specified dates have been processed');
    console.log('==========================================\n');
  } catch (error) {
    console.error('Fatal error during reprocessing:', error);
    process.exit(1);
  }
}

// Run the reprocessing
reprocessProblemDates();

export { reprocessProblemDates };
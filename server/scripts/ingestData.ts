import { eachDayOfInterval, format, parseISO } from "date-fns";
import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { eq } from "drizzle-orm";

const CHUNK_DELAY = 5000; // 5 second delay between chunks
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

async function ingestSpecificDate() {
  try {
    const targetDate = "2024-12-21";

    console.log(`\n=== Starting December 21, 2024 Data Ingestion ===`);
    console.log(`Target Date: ${targetDate}`);
    console.log(`===============================================\n`);

    // Process the target date
    const success = await processDay(targetDate);

    if (success) {
      console.log(`\n=== December 21 Data Ingestion Completed Successfully ===`);
    } else {
      console.error(`\n=== Failed to complete December 21 data ingestion ===`);
      process.exit(1);
    }
  } catch (error) {
    console.error('Fatal error during ingestion:', error);
    process.exit(1);
  }
}

// Run the ingestion
ingestSpecificDate();

export { ingestSpecificDate };
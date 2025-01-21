import { format } from "date-fns";
import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { eq } from "drizzle-orm";

const TARGET_DATE = "2024-12-10";
const MAX_RETRIES = 5;
const RETRY_DELAY = 30000; // 30 seconds between retries
const RATE_LIMIT_DELAY = 60000; // 60 seconds after rate limit errors

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processDay(dateStr: string, retryCount = 0): Promise<boolean> {
  try {
    console.log(`\n=== Reingesting data for ${dateStr} (Attempt ${retryCount + 1}/${MAX_RETRIES + 1}) ===`);
    
    // Delete existing summary for the date if any
    await db.delete(dailySummaries).where(eq(dailySummaries.summaryDate, dateStr));
    console.log(`✓ Cleared existing summary for ${dateStr}`);
    
    // Process the day's data
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

async function reingestDecember10() {
  try {
    console.log('\n=== Starting December 10, 2024 Data Reingestion ===');
    console.log(`Target Date: ${TARGET_DATE}`);
    console.log('===========================================\n');

    const success = await processDay(TARGET_DATE);

    if (success) {
      console.log('\n=== December 10, 2024 Data Reingestion Completed Successfully ===');
      
      // Verify the data was properly ingested
      const summary = await db.query.dailySummaries.findFirst({
        where: eq(dailySummaries.summaryDate, TARGET_DATE)
      });
      
      console.log('\nVerification Results:');
      console.log('Summary:', summary);
      console.log('===========================================\n');
    } else {
      console.error('\n=== December 10, 2024 Data Reingestion Failed ===');
      process.exit(1);
    }
  } catch (error) {
    console.error('Fatal error during reingestion:', error);
    process.exit(1);
  }
}

// Run the reingestion
reingestDecember10();

export { reingestDecember10 };

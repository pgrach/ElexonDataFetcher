import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { eq } from "drizzle-orm";

// Configuration for retry mechanism
const MAX_RETRIES = 5;
const RETRY_DELAY = 30000; // 30 seconds between retries

// Get date from command line argument or use default
const dateToProcess = process.argv[2] || "2024-12-10";

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processDate(dateStr: string, retryCount = 0): Promise<boolean> {
  try {
    console.log(`\n=== Processing Date: ${dateStr} ===`);
    console.log(`Attempt ${retryCount + 1}/${MAX_RETRIES}`);
    console.log(`Time: ${new Date().toISOString()}`);
    console.log(`===============================\n`);

    await processDailyCurtailment(dateStr);

    // Verify the data was actually ingested
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, dateStr)
    });

    if (!summary || (Number(summary.totalCurtailedEnergy) === 0 && Number(summary.totalPayment) === 0)) {
      console.log(`Warning: Zero values detected for ${dateStr}, will retry...`);
      throw new Error('Zero values detected in summary');
    }

    console.log(`\n=== Successfully processed ${dateStr} ===`);
    console.log(`Total Energy: ${summary.totalCurtailedEnergy} MWh`);
    console.log(`Total Payment: £${summary.totalPayment}`);
    console.log(`======================================\n`);

    return true;
  } catch (error) {
    const isRateLimit = error.message?.toLowerCase().includes('rate limit');
    console.error(`Error processing ${dateStr} (attempt ${retryCount + 1}):`, error);

    if (retryCount < MAX_RETRIES) {
      const delayTime = isRateLimit ? RETRY_DELAY * 2 : RETRY_DELAY;
      console.log(`\nRetrying ${dateStr} in ${delayTime/1000} seconds...`);
      await delay(delayTime);
      return processDate(dateStr, retryCount + 1);
    }

    console.error(`Failed to process ${dateStr} after ${MAX_RETRIES} attempts`);
    return false;
  }
}

// Process single date
async function main() {
  try {
    const success = await processDate(dateToProcess);

    if (!success) {
      console.error(`Failed to process ${dateToProcess}`);
      process.exit(1);
    }

    // Final validation
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, dateToProcess)
    });

    if (summary) {
      console.log('\nFinal Data Validation:');
      console.log(`Date: ${dateToProcess}`);
      console.log(`Total Curtailed Energy: ${Number(summary.totalCurtailedEnergy).toFixed(2)} MWh`);
      console.log(`Total Payment: £${Number(summary.totalPayment).toFixed(2)}`);
    }

  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  }
}

// Run the script
main();
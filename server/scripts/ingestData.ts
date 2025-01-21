import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { eq, and, sql } from "drizzle-orm";

const API_CALL_DELAY = 10000; // 10 seconds between API calls
const MAX_RETRIES = 5;
const BATCH_SIZE = 1; // Process 1 day at a time for the final day

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processDay(dateStr: string, retryCount = 0): Promise<boolean> {
  try {
    // Check if we already have data for this date
    const existingData = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, dateStr)
    });

    if (existingData && Number(existingData.totalCurtailedEnergy) > 0) {
      console.log(`✓ Valid data exists for ${dateStr}, skipping...`);
      return true;
    }

    console.log(`\nProcessing ${dateStr} (Attempt ${retryCount + 1}/${MAX_RETRIES + 1})`);
    await processDailyCurtailment(dateStr);

    // Verify the data was actually stored
    const verifyData = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, dateStr)
    });

    if (!verifyData) {
      throw new Error(`Data verification failed for ${dateStr}`);
    }

    console.log(`✓ Successfully processed ${dateStr}`);
    return true;
  } catch (error) {
    console.error(`Error processing ${dateStr} (attempt ${retryCount + 1}):`, error);

    if (retryCount < MAX_RETRIES) {
      const backoffDelay = Math.min(15000 * Math.pow(2, retryCount), 180000); // Exponential backoff, max 3 minutes
      console.log(`Retrying ${dateStr} in ${backoffDelay/1000} seconds...`);
      await delay(backoffDelay);
      return processDay(dateStr, retryCount + 1);
    }

    console.error(`Failed to process ${dateStr} after ${MAX_RETRIES + 1} attempts`);
    return false;
  }
}

async function ingestHistoricalData() {
  try {
    // Final day: November 30
    const remainingDays = [
      "2024-11-30"
    ];

    console.log('\n=== Starting November 30, 2024 Data Ingestion ===');
    console.log(`Days to process: ${remainingDays.length}`);

    // Process the final day
    for (let i = 0; i < remainingDays.length; i += BATCH_SIZE) {
      const batch = remainingDays.slice(i, i + BATCH_SIZE);
      console.log(`\nProcessing final day of November`);
      console.log(`Days in batch: ${batch.join(', ')}`);

      for (const dateStr of batch) {
        const success = await processDay(dateStr);

        if (success) {
          const processedData = await db.query.dailySummaries.findFirst({
            where: eq(dailySummaries.summaryDate, dateStr)
          });

          if (processedData) {
            console.log(`${dateStr} Summary:`);
            console.log(`- Total Curtailed Energy: ${Number(processedData.totalCurtailedEnergy).toFixed(2)} MWh`);
            console.log(`- Total Payment: £${Number(processedData.totalPayment).toFixed(2)}`);
          }

          console.log(`Waiting ${API_CALL_DELAY/1000} seconds before next day...\n`);
          await delay(API_CALL_DELAY);
        } else {
          console.log(`\nSkipping to next day after failure of ${dateStr}...\n`);
          await delay(API_CALL_DELAY * 2); // Double delay after failures
        }
      }
    }

    console.log('\n=== November 30 Data Ingestion Complete ===');
    console.log('\n=== All November 2024 Data Ingestion Complete ===');

    // Show final status for November 30
    const novemberData = await db.select({
      summaryDate: dailySummaries.summaryDate,
      totalCurtailedEnergy: dailySummaries.totalCurtailedEnergy,
      totalPayment: dailySummaries.totalPayment
    })
    .from(dailySummaries)
    .where(sql`${dailySummaries.summaryDate} = '2024-11-30'`)
    .orderBy(dailySummaries.summaryDate);

    console.log('\nProcessed day:');
    novemberData.forEach(day => {
      console.log(`${day.summaryDate}: ${Number(day.totalCurtailedEnergy).toFixed(2)} MWh, £${Number(day.totalPayment).toFixed(2)}`);
    });

  } catch (error) {
    console.error('Fatal error during ingestion:', error);
    process.exit(1);
  }
}

// Run the ingestion
ingestHistoricalData();
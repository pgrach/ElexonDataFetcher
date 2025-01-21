import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { eq } from "drizzle-orm";

const API_CALL_DELAY = 2000; // 2 seconds between API calls
const MAX_RETRIES = 3;

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
    console.log(`✓ Successfully processed ${dateStr}`);
    return true;
  } catch (error) {
    console.error(`Error processing ${dateStr} (attempt ${retryCount + 1}):`, error);

    if (retryCount < MAX_RETRIES) {
      console.log(`Retrying ${dateStr} in 15 seconds...`);
      await delay(15000);
      return processDay(dateStr, retryCount + 1);
    }

    console.error(`Failed to process ${dateStr} after ${MAX_RETRIES + 1} attempts`);
    return false;
  }
}

async function ingestHistoricalData() {
  try {
    // List of remaining days to process
    const remainingDays = [
      "2024-11-08", // Failed day
      "2024-11-11", "2024-11-12", "2024-11-13",
      "2024-11-14", "2024-11-15", "2024-11-16",
      "2024-11-17", "2024-11-18", "2024-11-19",
      "2024-11-20", "2024-11-21", "2024-11-22",
      "2024-11-23", "2024-11-24", "2024-11-25",
      "2024-11-26", "2024-11-27", "2024-11-28",
      "2024-11-29", "2024-11-30"
    ];

    console.log('\n=== Starting November 2024 Data Ingestion ===');
    console.log(`Days to process: ${remainingDays.length}`);

    for (let i = 0; i < remainingDays.length; i++) {
      const dateStr = remainingDays[i];
      const success = await processDay(dateStr);

      if (success) {
        console.log(`\nWaiting 45 seconds before next day...\n`);
        await delay(45000);
      } else {
        console.log(`\nSkipping to next day after failure...\n`);
        await delay(60000); // Longer delay after failures
      }
    }

    console.log('\n=== November 2024 Data Ingestion Status ===');

    // Show final status
    const novemberData = await db.query.dailySummaries.findMany({
      where: eq(dailySummaries.summaryDate.toString(), /^2024-11/),
      orderBy: [dailySummaries.summaryDate]
    });

    console.log('\nProcessed days:');
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
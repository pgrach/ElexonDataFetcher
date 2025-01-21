import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { eq, and, sql } from "drizzle-orm";

const API_CALL_DELAY = 5000; // 5 seconds between API calls
const MAX_RETRIES = 5;
const BATCH_SIZE = 5; // Process 5 days at a time

// API Request tracking
let apiRequestsPerMinute: { [key: string]: number } = {};
let totalRequests = 0;

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function trackApiRequest() {
  const now = new Date();
  const minute = `${now.getHours()}:${now.getMinutes()}`;
  apiRequestsPerMinute[minute] = (apiRequestsPerMinute[minute] || 0) + 1;
  totalRequests++;

  // Log current minute's requests
  console.log(`[${now.toLocaleTimeString()}] API Requests this minute: ${apiRequestsPerMinute[minute]}`);
}

async function getLastProcessedDate(): Promise<string | null> {
  const lastRecord = await db.query.dailySummaries.findFirst({
    where: sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date '2024-09-01'`,
    orderBy: dailySummaries.summaryDate,
    columns: {
      summaryDate: true
    }
  });
  return lastRecord?.summaryDate ?? null;
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
    trackApiRequest(); // Track API request
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
      const backoffDelay = Math.min(15000 * Math.pow(2, retryCount), 180000);
      console.log(`Retrying ${dateStr} in ${backoffDelay/1000} seconds...`);
      await delay(backoffDelay);
      return processDay(dateStr, retryCount + 1);
    }

    console.error(`Failed to process ${dateStr} after ${MAX_RETRIES + 1} attempts`);
    return false;
  }
}

async function ingestBatch(startDate: string) {
  try {
    console.log(`\n=== Starting batch ingestion from ${startDate} ===`);

    // Generate the next 5 days from the start date
    const batchDays = Array.from({ length: BATCH_SIZE }, (_, i) => {
      const date = new Date(startDate);
      date.setDate(date.getDate() + i);
      return date.toISOString().split('T')[0];
    }).filter(date => date.startsWith('2024-09')); // Only process September 2024 dates

    console.log(`Processing ${batchDays.length} days starting from ${startDate}`);

    // Process each day in the batch
    for (const dateStr of batchDays) {
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

        await delay(API_CALL_DELAY);
      } else {
        console.log(`\nSkipping to next day after failure of ${dateStr}...\n`);
        await delay(API_CALL_DELAY * 2);
      }
    }

    // Show batch statistics
    console.log('\nBatch API Request Statistics:');
    console.log(`Total API Requests Made: ${totalRequests}`);
    console.log('Requests per minute:');
    Object.entries(apiRequestsPerMinute)
      .sort()
      .forEach(([minute, count]) => {
        console.log(`${minute}: ${count} requests`);
      });

  } catch (error) {
    console.error('Fatal error during batch ingestion:', error);
    process.exit(1);
  }
}

// Get the command line argument for start date
const startDate = process.argv[2] || '2024-09-01';

// Run the batch ingestion
ingestBatch(startDate);

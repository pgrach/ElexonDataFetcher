import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { eq, and, sql } from "drizzle-orm";

const API_CALL_DELAY = 5000; // Reduced to 5 seconds between API calls
const MAX_RETRIES = 5;
const BATCH_SIZE = 1;

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

async function ingestHistoricalData() {
  try {
    console.log('\n=== Starting September 2024 Data Ingestion ===');

    // Find the last processed date to resume from there
    const lastProcessedDate = await getLastProcessedDate();
    console.log(`Last processed date: ${lastProcessedDate || 'None'}`);

    // Generate remaining days in September 2024
    const allSeptemberDays = Array.from({ length: 30 }, (_, i) => {
      const day = (i + 1).toString().padStart(2, '0');
      return `2024-09-${day}`;
    });

    // Filter out already processed days
    const remainingDays = lastProcessedDate 
      ? allSeptemberDays.filter(date => date > lastProcessedDate)
      : allSeptemberDays;

    console.log(`Remaining days to process: ${remainingDays.length}`);

    // Process remaining days
    for (let i = 0; i < remainingDays.length; i += BATCH_SIZE) {
      const batch = remainingDays.slice(i, i + BATCH_SIZE);
      console.log(`\nProcessing batch ${Math.floor(i/BATCH_SIZE) + 1} of ${Math.ceil(remainingDays.length/BATCH_SIZE)}`);

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

          await delay(API_CALL_DELAY);
        } else {
          console.log(`\nSkipping to next day after failure of ${dateStr}...\n`);
          await delay(API_CALL_DELAY * 2);
        }
      }

      // Log API request statistics every 5 days
      if (i % 5 === 0) {
        console.log('\nInterim API Request Statistics:');
        console.log(`Total Requests: ${totalRequests}`);
        Object.entries(apiRequestsPerMinute)
          .sort()
          .forEach(([minute, count]) => {
            console.log(`${minute}: ${count} requests`);
          });
      }
    }

    // Show final statistics
    const septemberData = await db.select({
      summaryDate: dailySummaries.summaryDate,
      totalCurtailedEnergy: dailySummaries.totalCurtailedEnergy,
      totalPayment: dailySummaries.totalPayment
    })
    .from(dailySummaries)
    .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date '2024-09-01'`)
    .orderBy(dailySummaries.summaryDate);

    console.log('\n=== September 2024 Data Ingestion Complete ===');
    console.log('\nFinal API Request Statistics:');
    console.log(`Total API Requests Made: ${totalRequests}`);
    console.log('\nProcessed Days Summary:');
    septemberData.forEach(day => {
      console.log(`${day.summaryDate}: ${Number(day.totalCurtailedEnergy).toFixed(2)} MWh, £${Number(day.totalPayment).toFixed(2)}`);
    });

  } catch (error) {
    console.error('Fatal error during ingestion:', error);
    process.exit(1);
  }
}

// Run the ingestion
ingestHistoricalData();
import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries, ingestionProgress } from "@db/schema";
import { eq, and, sql } from "drizzle-orm";
import { performance } from "perf_hooks";

const INITIAL_BATCH_SIZE = 2;  // We only have 2 dates to process
const MIN_API_DELAY = 1000;     // 1 second minimum delay
const MAX_RETRIES = 3;          // Reduced retries for faster failure detection

// Tracking structures
let apiRequestsPerMinute: { [key: string]: number } = {};
let totalRequests = 0;
let failedRequests = 0;
let totalProcessingTime = 0;

// Performance tracking
let avgProcessingTime = 0;
let successfulBatches = 0;

interface ProcessingMetrics {
  requestCount: number;
  duration: number;
  success: boolean;
}

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function trackApiRequest() {
  const now = new Date();
  const minute = `${now.getHours()}:${now.getMinutes()}`;
  apiRequestsPerMinute[minute] = (apiRequestsPerMinute[minute] || 0) + 1;
  totalRequests++;
}

async function processDay(dateStr: string, retryCount = 0): Promise<ProcessingMetrics> {
  const startTime = performance.now();
  let requestCount = 0;

  try {
    // Check for existing valid data
    const existingData = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, dateStr)
    });

    if (existingData && Number(existingData.totalCurtailedEnergy) > 0) {
      console.log(`✓ Valid data exists for ${dateStr}, skipping...`);
      return { requestCount: 0, duration: 0, success: true };
    }

    console.log(`\nProcessing ${dateStr} (Attempt ${retryCount + 1}/${MAX_RETRIES + 1})`);
    trackApiRequest();
    requestCount++;

    await processDailyCurtailment(dateStr);

    // Verify data storage
    const verifyData = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, dateStr)
    });

    if (!verifyData) {
      throw new Error(`Data verification failed for ${dateStr}`);
    }

    const duration = performance.now() - startTime;
    return { requestCount, duration, success: true };

  } catch (error) {
    console.error(`Error processing ${dateStr} (attempt ${retryCount + 1}):`, error);

    if (retryCount < MAX_RETRIES) {
      const backoffDelay = Math.min(MIN_API_DELAY * Math.pow(2, retryCount), 10000);
      await delay(backoffDelay);
      return processDay(dateStr, retryCount + 1);
    }

    failedRequests++;
    const duration = performance.now() - startTime;
    return { requestCount, duration, success: false };
  }
}

async function recordProgress(date: string, status: string, errorMessage?: string) {
  try {
    const existing = await db.query.ingestionProgress.findFirst({
      where: eq(ingestionProgress.lastProcessedDate, date)
    });

    if (existing) {
      await db
        .update(ingestionProgress)
        .set({
          status,
          errorMessage,
          updatedAt: new Date()
        })
        .where(eq(ingestionProgress.lastProcessedDate, date));
    } else {
      await db.insert(ingestionProgress).values({
        lastProcessedDate: date,
        status,
        errorMessage,
        updatedAt: new Date()
      });
    }
  } catch (error) {
    console.error('Error updating progress tracking:', error);
  }
}

async function ingestMissingOctoberData() {
  console.log('\n=== Processing Missing October 21-22, 2024 Data ===');

  // Only process October 21-22
  const missingDays = ['2024-10-21', '2024-10-22'];

  console.log(`Processing missing days: ${missingDays.join(', ')}`);

  // Process the two days in parallel
  const results = await Promise.all(
    missingDays.map(async dateStr => {
      const metrics = await processDay(dateStr);

      if (metrics.success) {
        const data = await db.query.dailySummaries.findFirst({
          where: eq(dailySummaries.summaryDate, dateStr)
        });

        if (data) {
          console.log(`${dateStr} Summary:`);
          console.log(`- Total Curtailed Energy: ${Number(data.totalCurtailedEnergy).toFixed(2)} MWh`);
          console.log(`- Total Payment: £${Number(data.totalPayment).toFixed(2)}`);

          await recordProgress(dateStr, 'completed');
        }
      } else {
        await recordProgress(dateStr, 'failed', 'Processing failed after max retries');
      }

      return { dateStr, ...metrics };
    })
  );

  // Final statistics
  console.log('\n=== October 21-22 Data Processing Complete ===');
  console.log('\nPerformance Statistics:');
  console.log(`Total API Requests: ${totalRequests}`);
  console.log(`Failed Requests: ${failedRequests}`);
  console.log(`Success Rate: ${((totalRequests-failedRequests)/totalRequests*100).toFixed(1)}%`);
}

// Run the ingestion for missing days
ingestMissingOctoberData().catch(error => {
  console.error('Fatal error during ingestion:', error);
  process.exit(1);
});
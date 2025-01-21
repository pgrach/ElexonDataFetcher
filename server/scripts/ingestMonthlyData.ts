import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries, ingestionProgress } from "@db/schema";
import { eq, and, sql } from "drizzle-orm";
import { performance } from "perf_hooks";
import { format, getDaysInMonth, startOfMonth, endOfMonth } from "date-fns";

const INITIAL_BATCH_SIZE = 5;  // Start with moderate batch size
const MIN_API_DELAY = 1000;    // 1 second minimum delay
const MAX_RETRIES = 3;         // Reduced retries for faster failure detection

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

async function ingestMonthlyData(yearMonth: string) {
  try {
    const [year, month] = yearMonth.split('-').map(Number);
    const daysInMonth = getDaysInMonth(new Date(year, month - 1));

    console.log(`\n=== Starting Data Ingestion for ${yearMonth} ===`);

    // Generate all days in the specified month
    const allDays = Array.from({ length: daysInMonth }, (_, i) => {
      const day = (i + 1).toString().padStart(2, '0');
      return `${yearMonth}-${day}`;
    });

    let currentBatchSize = INITIAL_BATCH_SIZE;
    let consecutiveSuccesses = 0;
    let consecutiveFailures = 0;

    // Process in optimized batches
    for (let i = 0; i < allDays.length; i += currentBatchSize) {
      const batchStartTime = performance.now();
      const batch = allDays.slice(i, i + currentBatchSize);

      console.log(`\nProcessing batch ${Math.floor(i/currentBatchSize) + 1} of ${Math.ceil(allDays.length/currentBatchSize)}`);
      console.log(`Current batch size: ${currentBatchSize}`);

      // Process batch in parallel
      const results = await Promise.all(
        batch.map(async dateStr => {
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

      // Calculate batch performance metrics
      const batchDuration = performance.now() - batchStartTime;
      const batchSuccess = results.every(r => r.success);
      const avgDuration = batchDuration / results.length;

      // Adapt batch size based on performance
      if (batchSuccess) {
        consecutiveSuccesses++;
        consecutiveFailures = 0;
        if (consecutiveSuccesses >= 2 && avgDuration < 30000) { // If batch completes in under 30s
          currentBatchSize = Math.min(currentBatchSize + 2, 10); // Increase batch size up to max 10
        }
      } else {
        consecutiveFailures++;
        consecutiveSuccesses = 0;
        if (consecutiveFailures >= 2) {
          currentBatchSize = Math.max(currentBatchSize - 2, 3); // Decrease batch size but maintain minimum of 3
        }
      }

      // Update progress tracking
      totalProcessingTime += batchDuration;
      successfulBatches += batchSuccess ? 1 : 0;

      // Log performance metrics
      console.log('\nBatch Performance Metrics:');
      console.log(`Duration: ${(batchDuration/1000).toFixed(2)}s`);
      console.log(`Average processing time per day: ${(avgDuration/1000).toFixed(2)}s`);
      console.log(`Success rate: ${(results.filter(r => r.success).length/results.length*100).toFixed(1)}%`);
      console.log(`API requests this batch: ${results.reduce((sum, r) => sum + r.requestCount, 0)}`);

      // Adaptive delay between batches
      const optimalDelay = Math.max(MIN_API_DELAY, Math.min(avgDuration * 0.1, 5000));
      await delay(optimalDelay);
    }

    // Final statistics
    console.log(`\n=== ${yearMonth} Data Ingestion Complete ===`);
    console.log('\nPerformance Statistics:');
    console.log(`Total API Requests: ${totalRequests}`);
    console.log(`Failed Requests: ${failedRequests}`);
    console.log(`Success Rate: ${((totalRequests-failedRequests)/totalRequests*100).toFixed(1)}%`);
    console.log(`Average Processing Time per Day: ${(totalProcessingTime/(daysInMonth*1000)).toFixed(2)}s`);

    // Show monthly summary
    const monthlyData = await db.select({
      summaryDate: dailySummaries.summaryDate,
      totalCurtailedEnergy: dailySummaries.totalCurtailedEnergy,
      totalPayment: dailySummaries.totalPayment
    })
    .from(dailySummaries)
    .where(
      sql`${dailySummaries.summaryDate}::date >= date_trunc('month', ${yearMonth + '-01'}::date) AND 
          ${dailySummaries.summaryDate}::date < date_trunc('month', ${yearMonth + '-01'}::date) + interval '1 month'`
    )
    .orderBy(dailySummaries.summaryDate);

    console.log(`\n${yearMonth} Summary:`);
    let monthlyTotal = {
      energy: 0,
      payment: 0
    };

    monthlyData.forEach(day => {
      console.log(`${day.summaryDate}: ${Number(day.totalCurtailedEnergy).toFixed(2)} MWh, £${Number(day.totalPayment).toFixed(2)}`);
      monthlyTotal.energy += Number(day.totalCurtailedEnergy);
      monthlyTotal.payment += Number(day.totalPayment);
    });

    console.log(`\nMonthly Totals for ${yearMonth}:`);
    console.log(`Total Energy Curtailed: ${monthlyTotal.energy.toFixed(2)} MWh`);
    console.log(`Total Payment: £${monthlyTotal.payment.toFixed(2)}`);

  } catch (error) {
    console.error('Fatal error during ingestion:', error);
    process.exit(1);
  }
}

// Get the command line argument for year-month
const yearMonth = process.argv[2];
if (!yearMonth || !yearMonth.match(/^\d{4}-\d{2}$/)) {
  console.error('Please provide a year-month in the format YYYY-MM');
  console.error('Example: npm run ingest-month 2024-10');
  process.exit(1);
}

// Run the ingestion for the specified month
ingestMonthlyData(yearMonth);
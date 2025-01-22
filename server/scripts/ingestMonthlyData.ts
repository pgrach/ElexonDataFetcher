import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries, ingestionProgress, curtailmentRecords } from "@db/schema";
import { eq, and, sql } from "drizzle-orm";
import { performance } from "perf_hooks";
import { format, getDaysInMonth, startOfMonth, endOfMonth, parse } from "date-fns";

const INITIAL_BATCH_SIZE = 10;  
const MIN_API_DELAY = 200;    
const MAX_RETRIES = 3;
const MAX_BATCH_SIZE = 15;     

let apiRequestsPerMinute: { [key: string]: number } = {};
let totalRequests = 0;
let failedRequests = 0;
let totalProcessingTime = 0;
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
    // Delete existing records for clean re-ingestion
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, dateStr));

    console.log(`Processing ${dateStr} (Attempt ${retryCount + 1}/${MAX_RETRIES + 1})`);
    trackApiRequest();
    requestCount++;

    await processDailyCurtailment(dateStr);

    const verifyData = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, dateStr)
    });

    if (!verifyData) {
      throw new Error(`Data verification failed for ${dateStr}`);
    }

    console.log(`[${dateStr}] Successfully processed: ${Number(verifyData.totalCurtailedEnergy).toFixed(2)} MWh, £${Number(verifyData.totalPayment).toFixed(2)}`);

    const duration = performance.now() - startTime;
    return { requestCount, duration, success: true };

  } catch (error) {
    console.error(`Error processing ${dateStr} (attempt ${retryCount + 1}):`, error);

    if (retryCount < MAX_RETRIES) {
      const backoffDelay = Math.min(MIN_API_DELAY * Math.pow(2, retryCount), 5000);
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

export async function ingestMonthlyData(yearMonth: string, startDay?: number, endDay?: number) {
  try {
    const [year, month] = yearMonth.split('-').map(Number);
    const daysInMonth = getDaysInMonth(new Date(year, month - 1));

    const start = startDay || 1;
    const end = endDay || daysInMonth;

    if (start > end || start < 1 || end > daysInMonth) {
      throw new Error('Invalid date range specified');
    }

    console.log(`\n=== Starting Data Ingestion for ${yearMonth} (Days ${start}-${end}) ===`);

    const daysToProcess = Array.from({ length: end - start + 1 }, (_, i) => {
      const day = (i + start).toString().padStart(2, '0');
      return `${yearMonth}-${day}`;
    });

    let currentBatchSize = INITIAL_BATCH_SIZE;
    let consecutiveSuccesses = 0;
    let consecutiveFailures = 0;

    for (let i = 0; i < daysToProcess.length; i += currentBatchSize) {
      const batchStartTime = performance.now();
      const batch = daysToProcess.slice(i, i + currentBatchSize);

      console.log(`\nProcessing batch ${Math.floor(i/currentBatchSize) + 1} of ${Math.ceil(daysToProcess.length/currentBatchSize)}`);
      console.log(`Dates: ${batch.join(', ')}`);

      // Process batch in parallel with improved concurrency
      const results = await Promise.all(
        batch.map(dateStr => processDay(dateStr))
      );

      const batchDuration = performance.now() - batchStartTime;
      const batchSuccess = results.every(r => r.success);
      const avgDuration = batchDuration / results.length;

      if (batchSuccess) {
        consecutiveSuccesses++;
        consecutiveFailures = 0;
        if (consecutiveSuccesses >= 2 && avgDuration < 30000) { 
          currentBatchSize = Math.min(currentBatchSize + 1, MAX_BATCH_SIZE);
        }
      } else {
        consecutiveFailures++;
        consecutiveSuccesses = 0;
        if (consecutiveFailures >= 2) {
          currentBatchSize = Math.max(currentBatchSize - 1, 4);
        }
      }

      totalProcessingTime += batchDuration;
      successfulBatches += batchSuccess ? 1 : 0;

      // Enhanced logging
      console.log(`Batch ${Math.floor(i/currentBatchSize) + 1} completed:`, {
        successful: `${results.filter(r => r.success).length}/${results.length}`,
        duration: `${(batchDuration/1000).toFixed(1)}s`,
        avgDuration: `${(avgDuration/1000).toFixed(1)}s per day`
      });

      // Optimized delay calculation based on performance
      const optimalDelay = Math.max(MIN_API_DELAY, Math.min(avgDuration * 0.1, 3000));
      await delay(optimalDelay);
    }

    console.log(`\n=== ${yearMonth} (Days ${start}-${end}) Data Ingestion Complete ===`);

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
      payment: 0,
      daysProcessed: 0
    };

    monthlyData.forEach(day => {
      console.log(`${day.summaryDate}: ${Number(day.totalCurtailedEnergy).toFixed(2)} MWh, £${Number(day.totalPayment).toFixed(2)}`);
      monthlyTotal.energy += Number(day.totalCurtailedEnergy);
      monthlyTotal.payment += Number(day.totalPayment);
      monthlyTotal.daysProcessed++;
    });

    console.log(`\nMonthly Totals for ${yearMonth}:`);
    console.log(`Days Processed: ${monthlyTotal.daysProcessed}`);
    console.log(`Total Energy Curtailed: ${monthlyTotal.energy.toFixed(2)} MWh`);
    console.log(`Total Payment: £${monthlyTotal.payment.toFixed(2)}`);
    console.log(`Average Daily Curtailment: ${(monthlyTotal.energy / monthlyTotal.daysProcessed).toFixed(2)} MWh`);

  } catch (error) {
    console.error('Fatal error during ingestion:', error);
    throw error; // Re-throw to handle in the calling function
  }
}

// Only run if this is the main module
if (import.meta.url === new URL(import.meta.url).href) {
  const args = process.argv.slice(2);
  const yearMonth = args[0];
  const startDay = args[1] ? parseInt(args[1]) : undefined;
  const endDay = args[2] ? parseInt(args[2]) : undefined;

  if (!yearMonth || !yearMonth.match(/^\d{4}-\d{2}$/)) {
    console.error('Please provide arguments in the format: YYYY-MM [startDay] [endDay]');
    console.error('Example: npm run ingest-month 2024-08');
    console.error('Example with date range: npm run ingest-month 2024-08 1 5');
    process.exit(1);
  }

  ingestMonthlyData(yearMonth, startDay, endDay).catch(error => {
    console.error('Error:', error);
    process.exit(1);
  });
}
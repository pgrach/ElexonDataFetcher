import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries, ingestionProgress, curtailmentRecords } from "@db/schema";
import { eq, and, sql } from "drizzle-orm";
import { performance } from "perf_hooks";
import { format, getDaysInMonth, startOfMonth, endOfMonth, parse } from "date-fns";

// Reduced batch sizes and increased delays for better reliability
const INITIAL_BATCH_SIZE = 3;     // Start with smaller batches
const MIN_API_DELAY = 500;        // Increased minimum delay between requests
const MAX_RETRIES = 3;
const MAX_BATCH_SIZE = 5;         // Reduced maximum batch size
const BATCH_RETRY_ATTEMPTS = 3;    // Increased retry attempts
const REQUEST_TIMEOUT = 30000;     // 30 seconds timeout for API requests

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
  error?: string;
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

    await Promise.race([
      processDailyCurtailment(dateStr),
      new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Request timeout')), REQUEST_TIMEOUT)
      )
    ]);

    // Enhanced verification
    const verifyData = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, dateStr)
    });

    if (!verifyData || !verifyData.totalCurtailedEnergy) {
      throw new Error(`Data verification failed for ${dateStr}`);
    }

    // Log successful processing with details
    console.log(`[${dateStr}] Successfully processed: ${Number(verifyData.totalCurtailedEnergy).toFixed(2)} MWh, £${Number(verifyData.totalPayment).toFixed(2)}`);

    const duration = performance.now() - startTime;
    avgProcessingTime = (avgProcessingTime * successfulBatches + duration) / (successfulBatches + 1);
    successfulBatches++;

    return { requestCount, duration, success: true };

  } catch (error) {
    console.error(`Error processing ${dateStr} (attempt ${retryCount + 1}):`, error);

    if (retryCount < MAX_RETRIES) {
      // Exponential backoff for retries
      const backoffDelay = Math.min(MIN_API_DELAY * Math.pow(2, retryCount), 5000);
      console.log(`Retrying after ${backoffDelay}ms delay...`);
      await delay(backoffDelay);
      return processDay(dateStr, retryCount + 1);
    }

    failedRequests++;
    const duration = performance.now() - startTime;
    return { 
      requestCount, 
      duration, 
      success: false, 
      error: error instanceof Error ? error.message : 'Unknown error'
    };
  }
}

async function verifyBatchData(dates: string[]): Promise<{ success: boolean, missingDates: string[] }> {
  const results = await Promise.all(
    dates.map(async (date) => {
      const data = await db.query.dailySummaries.findFirst({
        where: eq(dailySummaries.summaryDate, date)
      });
      return { date, exists: !!data?.totalCurtailedEnergy };
    })
  );

  const missingDates = results.filter(r => !r.exists).map(r => r.date);
  if (missingDates.length > 0) {
    console.log(`Missing data for dates: ${missingDates.join(', ')}`);
    return { success: false, missingDates };
  }
  return { success: true, missingDates: [] };
}

async function processBatch(
  dates: string[], 
  batchNumber: number, 
  totalBatches: number, 
  retryAttempt = 0
): Promise<boolean> {
  console.log(`\nProcessing batch ${batchNumber} of ${totalBatches} (Attempt ${retryAttempt + 1}/${BATCH_RETRY_ATTEMPTS + 1})`);
  console.log(`Dates: ${dates.join(', ')}`);

  const batchStartTime = performance.now();

  // Process each date sequentially within the batch to avoid overwhelming the API
  const results = [];
  for (const dateStr of dates) {
    const result = await processDay(dateStr);
    results.push(result);
    // Add delay between processing days within the batch
    await delay(MIN_API_DELAY);
  }

  const batchDuration = performance.now() - batchStartTime;
  const failedDates = dates.filter((_, index) => !results[index].success);

  // Log batch results
  console.log(`Batch ${batchNumber} completed:`, {
    successful: `${results.filter(r => r.success).length}/${results.length}`,
    duration: `${(batchDuration/1000).toFixed(1)}s`,
    avgDuration: `${(batchDuration/results.length/1000).toFixed(1)}s per day`,
    failedDates: failedDates.length > 0 ? failedDates : 'None'
  });

  // Verify all dates were processed correctly
  const { success: batchSuccessful, missingDates } = await verifyBatchData(dates);

  if (!batchSuccessful && retryAttempt < BATCH_RETRY_ATTEMPTS) {
    console.log(`\nRetrying failed dates in batch ${batchNumber}...`);
    await delay(MIN_API_DELAY * 2);
    return processBatch(missingDates, batchNumber, totalBatches, retryAttempt + 1);
  }

  return batchSuccessful;
}

async function ingestMonthlyData(yearMonth: string, startDay?: number, endDay?: number) {
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
    let failedDates: string[] = [];

    // Process in smaller batches with verification
    for (let i = 0; i < daysToProcess.length; i += currentBatchSize) {
      const batch = daysToProcess.slice(i, i + currentBatchSize);
      const batchNumber = Math.floor(i/currentBatchSize) + 1;
      const totalBatches = Math.ceil(daysToProcess.length/currentBatchSize);

      const batchSuccess = await processBatch(batch, batchNumber, totalBatches);

      if (batchSuccess) {
        consecutiveSuccesses++;
        consecutiveFailures = 0;
        if (consecutiveSuccesses >= 3 && currentBatchSize < MAX_BATCH_SIZE) {
          currentBatchSize = Math.min(currentBatchSize + 1, MAX_BATCH_SIZE);
        }
      } else {
        consecutiveFailures++;
        consecutiveSuccesses = 0;
        if (consecutiveFailures >= 2) {
          currentBatchSize = Math.max(currentBatchSize - 1, INITIAL_BATCH_SIZE);
        }
        failedDates.push(...batch);
      }

      // Add delay between batches based on recent performance
      const optimalDelay = Math.max(MIN_API_DELAY * 2, Math.min(avgProcessingTime * 0.2, 3000));
      await delay(optimalDelay);
    }

    // Report any failed dates
    if (failedDates.length > 0) {
      console.log('\nWarning: The following dates failed to process:');
      console.log(failedDates.join(', '));
      throw new Error(`Failed to process ${failedDates.length} dates`);
    }

    console.log(`\n=== ${yearMonth} Data Ingestion Complete ===`);

    // Generate summary report
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
    process.exit(1);
  }
}

// Input validation and script execution
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

ingestMonthlyData(yearMonth, startDay, endDay);
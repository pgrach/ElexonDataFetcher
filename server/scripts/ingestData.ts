import { eachDayOfInterval, format, parseISO } from "date-fns";
import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries, curtailmentRecords } from "@db/schema";
import { eq, sql } from "drizzle-orm";

const CHUNK_SIZE = 1; // Process 1 day at a time to avoid timeouts
const CHUNK_DELAY = 30000; // 30 second delay between chunks
const MAX_RETRIES = 3;
const RETRY_DELAY = 10000; // 10 seconds between retries
const RATE_LIMIT_DELAY = 30000; // 30 seconds after rate limit errors

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processDay(dateStr: string, retryCount = 0): Promise<boolean> {
  try {
    await processDailyCurtailment(dateStr);
    console.log(`Successfully processed ${dateStr}`);
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

async function processChunk(days: Date[]) {
  for (const day of days) {
    const dateStr = format(day, 'yyyy-MM-dd');

    // Check if we already have data for this date
    const existingData = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, dateStr)
    });

    if (existingData) {
      console.log(`Data already exists for ${dateStr}, skipping...`);
      continue;
    }

    console.log(`\nProcessing data for ${dateStr}`);
    const success = await processDay(dateStr);

    if (success) {
      // Add delay between successful days to respect rate limits
      console.log(`Waiting 10 seconds before next day...`);
      await delay(10000);
    } else {
      // Add longer delay after failed days
      console.log(`Waiting 30 seconds before next day due to previous failure...`);
      await delay(30000);
    }
  }
}

async function ingestRemainingDays() {
  try {
    const startDate = parseISO("2025-01-19"); // Start from January 19th
    const endDate = new Date();
    const days = eachDayOfInterval({ start: startDate, end: endDate });

    console.log(`Starting data ingestion from ${format(startDate, 'yyyy-MM-dd')} to ${format(endDate, 'yyyy-MM-dd')}`);
    console.log(`Total days to process: ${days.length}`);

    // Process days in smaller chunks
    for (let i = 0; i < days.length; i += CHUNK_SIZE) {
      const chunk = days.slice(i, i + CHUNK_SIZE);
      const chunkNum = Math.floor(i/CHUNK_SIZE) + 1;
      const totalChunks = Math.ceil(days.length/CHUNK_SIZE);

      console.log(`\nProcessing chunk ${chunkNum} of ${totalChunks}`);
      console.log(`Days: ${chunk.map(d => format(d, 'yyyy-MM-dd')).join(', ')}`);

      await processChunk(chunk);

      if (i + CHUNK_SIZE < days.length) {
        console.log(`\nWaiting ${CHUNK_DELAY/1000} seconds before next chunk...`);
        await delay(CHUNK_DELAY);
      }
    }

    console.log('\nData ingestion completed successfully');
  } catch (error) {
    console.error('Fatal error during ingestion:', error);
    process.exit(1);
  }
}

async function verifyJan1stData() {
  try {
    console.log('Starting verification of January 1st, 2025 data...');

    // Clear existing data for Jan 1st to avoid duplicates
    await db.delete(dailySummaries).where(eq(dailySummaries.summaryDate, '2025-01-01'));
    await db.delete(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, '2025-01-01'));

    // Process January 1st with enhanced logging
    console.log('\nProcessing data for 2025-01-01');
    await processDailyCurtailment('2025-01-01');

    // Verify the results
    const recordTotals = await db
      .select({
        recordCount: sql`COUNT(*)`,
        totalVolume: sql`SUM(${curtailmentRecords.volume}::numeric)`,
        totalPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, '2025-01-01'));

    console.log('\nVerification Results:');
    console.log('Total Records:', recordTotals[0].recordCount);
    console.log('Total Volume:', recordTotals[0].totalVolume, 'MWh');
    console.log('Total Payment:', recordTotals[0].totalPayment, 'GBP');

    // Get period-by-period breakdown
    const periodBreakdown = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        recordCount: sql`COUNT(*)`,
        periodVolume: sql`SUM(${curtailmentRecords.volume}::numeric)`,
        periodPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, '2025-01-01'))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);

    console.log('\nPeriod-by-Period Breakdown:');
    periodBreakdown.forEach(p => {
      console.log(`Period ${p.period}: ${p.recordCount} records, ${p.periodVolume} MWh, £${p.periodPayment}`);
    });

  } catch (error) {
    console.error('Verification failed:', error);
    process.exit(1);
  }
}

// Run the ingestion for remaining days
ingestRemainingDays();

// Run the verification
verifyJan1stData();

export { ingestRemainingDays, verifyJan1stData };
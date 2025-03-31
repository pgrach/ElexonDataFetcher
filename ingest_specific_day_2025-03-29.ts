/**
 * Single Day Data Ingestion for 2025-03-29
 * 
 * This script is designed specifically to process March 29, 2025 data, using the same
 * approach as the successful ingestions for March 30 and 31. It's based on the
 * ingestMonthlyData approach but optimized for a single day.
 * 
 * Usage:
 *   npx tsx ingest_specific_day_2025-03-29.ts
 */

import { processDailyCurtailment } from "./server/services/curtailment";
import { db } from "@db";
import { dailySummaries, curtailmentRecords } from "@db/schema";
import { eq, sql } from "drizzle-orm";
import { performance } from "perf_hooks";
import fs from "fs/promises";

const TARGET_DATE = "2025-03-29";
const LOG_FILE = `ingest_specific_day_${TARGET_DATE}.log`;
const MAX_RETRIES = 5; // Increased retry attempts for problematic API
const DELAY_BETWEEN_RETRIES = 10000; // 10 seconds

// Write to both console and log file
async function log(message: string, level: "info" | "warning" | "error" | "success" = "info"): Promise<void> {
  const timestamp = new Date().toLocaleTimeString();
  const formattedMessage = `[${timestamp}] [${level.toUpperCase()}] ${message}`;
  
  console.log(formattedMessage);
  
  try {
    await fs.appendFile(LOG_FILE, formattedMessage + "\n");
  } catch (error) {
    console.error("Error writing to log file:", error);
  }
}

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processDay(dateStr: string, retryCount = 0): Promise<{
  requestCount: number;
  duration: number;
  success: boolean;
}> {
  const startTime = performance.now();
  let requestCount = 0;

  try {
    await log(`Processing ${dateStr} (Attempt ${retryCount + 1}/${MAX_RETRIES + 1})`);
    requestCount++;

    await processDailyCurtailment(dateStr);

    // Verify data for multiple periods to ensure it was processed
    const periodSamples = [1, 12, 24, 36, 48]; // Sample different times of day
    const periodCounts = await Promise.all(
      periodSamples.map(period => 
        db.select({ count: sql<number>`count(*)` })
          .from(curtailmentRecords)
          .where(
            sql`${curtailmentRecords.settlementDate} = ${dateStr} AND 
                ${curtailmentRecords.settlementPeriod} = ${period}`
          )
      )
    );
    
    const periodResults = periodSamples.map((period, i) => ({
      period,
      count: periodCounts[i][0]?.count || 0
    }));
    
    await log(`Period checks: ${periodResults.map(p => `P${p.period}: ${p.count}`).join(', ')}`);

    // Verify daily summary was created
    const verifyData = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, dateStr)
    });

    if (!verifyData) {
      if (periodResults.some(p => p.count > 0)) {
        // We have some data but no summary - this is a partial success
        await log(`Warning: Data found but no summary created for ${dateStr}. Will need manual summary creation.`, "warning");
      } else {
        // No data at all - this could be normal if there was no curtailment that day
        await log(`Note: No curtailment data found for ${dateStr}. This could be normal if no curtailment occurred.`, "info");
      }
    } else {
      await log(`Successfully processed ${dateStr}: ${Number(verifyData.totalCurtailedEnergy).toFixed(2)} MWh, £${Number(verifyData.totalPayment).toFixed(2)}`, "success");
    }

    const duration = performance.now() - startTime;
    return { requestCount, duration, success: true };

  } catch (error) {
    await log(`Error processing ${dateStr} (attempt ${retryCount + 1}): ${error}`, "error");

    if (retryCount < MAX_RETRIES) {
      await log(`Retrying in ${DELAY_BETWEEN_RETRIES/1000} seconds...`, "warning");
      await delay(DELAY_BETWEEN_RETRIES);
      return processDay(dateStr, retryCount + 1);
    }

    const duration = performance.now() - startTime;
    return { requestCount, duration, success: false };
  }
}

async function processMissingDay(): Promise<void> {
  try {
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== Processing Specific Date: ${TARGET_DATE} ===\n`);
    
    // Check if we already have data for this date
    const existingRecords = await db
      .select({ count: sql<number>`count(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
    const recordCount = Number(existingRecords[0]?.count || 0);
    
    if (recordCount > 0) {
      await log(`Found ${recordCount} existing records for ${TARGET_DATE}. Will clear and reprocess.`, "warning");
    } else {
      await log(`No existing records found for ${TARGET_DATE}.`, "info");
    }
    
    // Process the date
    const startTime = performance.now();
    const result = await processDay(TARGET_DATE);
    const endTime = performance.now();
    const processingTime = (endTime - startTime) / 1000;
    
    await log(`Processing complete for ${TARGET_DATE}`, "info");
    await log(`Time elapsed: ${processingTime.toFixed(2)} seconds`, "info");
    await log(`Status: ${result.success ? 'SUCCESS' : 'FAILED'}`, result.success ? "success" : "error");
    
    // Final verification - count records after processing
    const finalRecords = await db
      .select({ count: sql<number>`count(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
    const finalCount = Number(finalRecords[0]?.count || 0);
    
    if (finalCount > 0) {
      await log(`Final record count: ${finalCount} records in database for ${TARGET_DATE}`, "success");
      
      // Get the distribution by period to verify completeness
      const periodDistribution = await db
        .select({
          period: curtailmentRecords.settlementPeriod,
          count: sql<number>`count(*)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
        .groupBy(curtailmentRecords.settlementPeriod)
        .orderBy(curtailmentRecords.settlementPeriod);
      
      await log(`Period distribution: ${periodDistribution.map(p => `P${p.period}:${p.count}`).join(', ')}`, "info");
    } else {
      await log(`No records in database for ${TARGET_DATE} after processing. This may be normal if no curtailment occurred that day.`, "warning");
    }
    
  } catch (error) {
    await log(`Fatal error: ${error}`, "error");
    process.exit(1);
  }
}

processMissingDay();
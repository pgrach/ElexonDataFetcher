/**
 * This script reprocesses missing curtailment data for specific days in 2025
 * It focuses on March 1 and March 2, 2025, which were identified as missing.
 */

import { processDailyCurtailment } from "./server/services/curtailment";
import { format, eachDayOfInterval, parseISO } from 'date-fns';
import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { sql, eq } from "drizzle-orm";
import { reconcileDay } from "./server/services/historicalReconciliation";

const START_DATE = '2025-03-01';
const END_DATE = '2025-03-02';

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function getDatabaseStats(date: string) {
  const result = await db
    .select({
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
      totalPayment: sql<string>`SUM(payment::numeric)::text`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));

  return {
    recordCount: result[0]?.recordCount || 0,
    periodCount: result[0]?.periodCount || 0,
    totalVolume: result[0]?.totalVolume ? Number(result[0].totalVolume).toFixed(2) : '0.00',
    totalPayment: result[0]?.totalPayment ? `£${Number(result[0].totalPayment).toFixed(2)}` : '£0.00'
  };
}

async function processMissingDays() {
  try {
    console.log(`\n=== Starting Processing for Missing Days: ${START_DATE} to ${END_DATE} ===`);
    
    const days = eachDayOfInterval({
      start: parseISO(START_DATE),
      end: parseISO(END_DATE)
    });

    for (const day of days) {
      const dateStr = format(day, 'yyyy-MM-dd');
      console.log(`\n=== Processing ${dateStr} ===`);
      
      // Get initial statistics
      const beforeStats = await getDatabaseStats(dateStr);
      console.log(`Before processing: ${beforeStats.recordCount} records, ${beforeStats.periodCount} periods, ${beforeStats.totalVolume} MWh, ${beforeStats.totalPayment}`);
      
      try {
        // Process curtailment data
        await processDailyCurtailment(dateStr);
        
        // Allow some time for database operations to complete
        await delay(2000);
        
        // Run reconciliation to ensure Bitcoin calculations are up to date
        await reconcileDay(dateStr);
        
        // Get final statistics
        const afterStats = await getDatabaseStats(dateStr);
        console.log(`After processing: ${afterStats.recordCount} records, ${afterStats.periodCount} periods, ${afterStats.totalVolume} MWh, ${afterStats.totalPayment}`);
        
        if (afterStats.recordCount === 0) {
          console.warn(`[WARNING] No data was ingested for ${dateStr}. This might indicate an API issue or no curtailment activity on this date.`);
        } else {
          console.log(`[SUCCESS] Successfully processed ${dateStr}`);
        }
      } catch (error) {
        console.error(`[ERROR] Failed to process ${dateStr}:`, error);
      }
      
      // Add a delay between dates to prevent API rate limiting
      await delay(3000);
    }
    
    console.log(`\n=== Processing Complete ===`);
  } catch (error) {
    console.error('Error during processing:', error);
    process.exit(1);
  }
}

processMissingDays();
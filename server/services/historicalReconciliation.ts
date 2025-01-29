import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { format, startOfMonth, endOfMonth, parseISO, isBefore } from "date-fns";
import { processDailyCurtailment } from "./curtailment";
import { fetchBidsOffers } from "./elexon";
import { eq, and, sql } from "drizzle-orm";

const MAX_CONCURRENT_DAYS = 5;
const RECONCILIATION_HOUR = 1; // Run at 1 AM

/**
 * Check if a specific day's data needs to be reprocessed by comparing
 * a sample of periods with the Elexon API
 */
async function needsReprocessing(date: string): Promise<boolean> {
  try {
    // Check periods 1 and 24 as sample periods
    const samplePeriods = [1, 24];
    
    for (const period of samplePeriods) {
      const apiRecords = await fetchBidsOffers(date, period);
      
      // Get existing records for this period
      const existingRecords = await db
        .select({
          farmId: curtailmentRecords.farmId,
          volume: curtailmentRecords.volume,
          payment: curtailmentRecords.payment
        })
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, date),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );

      // Create maps for comparison
      const apiMap = new Map(
        apiRecords.map(r => [
          r.id,
          {
            volume: Math.abs(r.volume).toString(),
            payment: (Math.abs(r.volume) * r.originalPrice).toString()
          }
        ])
      );

      const dbMap = new Map(
        existingRecords.map(r => [
          r.farmId,
          {
            volume: r.volume,
            payment: r.payment
          }
        ])
      );

      // Compare record counts
      if (apiMap.size !== dbMap.size) {
        console.log(`[${date} P${period}] Different number of records (API: ${apiMap.size}, DB: ${dbMap.size})`);
        return true;
      }

      // Compare individual records
      for (const [farmId, apiValues] of apiMap) {
        const dbValues = dbMap.get(farmId);
        
        if (!dbValues) {
          console.log(`[${date} P${period}] New record found for ${farmId}`);
          return true;
        }

        if (apiValues.volume !== dbValues.volume || apiValues.payment !== dbValues.payment) {
          console.log(`[${date} P${period}] Data mismatch for ${farmId}`);
          return true;
        }
      }
    }

    return false;
  } catch (error) {
    console.error(`Error checking data for ${date}:`, error);
    return true; // Reprocess on error to be safe
  }
}

/**
 * Process reconciliation for a specific day
 */
async function reconcileDay(date: string): Promise<void> {
  try {
    if (await needsReprocessing(date)) {
      console.log(`[${date}] Data differences detected, reprocessing...`);
      await processDailyCurtailment(date);
      console.log(`[${date}] Successfully reprocessed data`);
    } else {
      console.log(`[${date}] No updates needed`);
    }
  } catch (error) {
    console.error(`Error reconciling data for ${date}:`, error);
    throw error;
  }
}

/**
 * Check and update data for the current month
 */
export async function reconcileCurrentMonth(): Promise<void> {
  try {
    const now = new Date();
    const currentMonth = startOfMonth(now);
    const monthEnd = endOfMonth(now);
    
    // Don't process future dates
    const endDate = isBefore(now, monthEnd) ? now : monthEnd;
    
    console.log(`Starting reconciliation for ${format(currentMonth, 'yyyy-MM')}`);

    // Get all dates in the month up to today
    const dates: string[] = [];
    let currentDate = currentMonth;
    
    while (isBefore(currentDate, endDate)) {
      dates.push(format(currentDate, 'yyyy-MM-dd'));
      currentDate = new Date(currentDate.setDate(currentDate.getDate() + 1));
    }

    // Process dates in batches
    for (let i = 0; i < dates.length; i += MAX_CONCURRENT_DAYS) {
      const batch = dates.slice(i, i + MAX_CONCURRENT_DAYS);
      await Promise.all(batch.map(date => reconcileDay(date)));
    }

    console.log(`Completed reconciliation for ${format(currentMonth, 'yyyy-MM')}`);
  } catch (error) {
    console.error('Error during monthly reconciliation:', error);
    throw error;
  }
}

/**
 * Check if reconciliation should run based on current hour
 */
export function shouldRunReconciliation(): boolean {
  const currentHour = new Date().getHours();
  return currentHour === RECONCILIATION_HOUR;
}

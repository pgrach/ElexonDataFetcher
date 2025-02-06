import { db } from "@db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "@db/schema";
import { format, startOfMonth, endOfMonth, parseISO, isBefore, subDays, subMonths } from "date-fns";
import { processDailyCurtailment } from "./curtailment";
import { fetchBidsOffers } from "./elexon";
import { eq, and, sql } from "drizzle-orm";
import { processSingleDay } from "./bitcoinService";

const MAX_CONCURRENT_DAYS = 5;
const RECONCILIATION_HOUR = 3; // Run at 3 AM to ensure all updates are captured
const SAMPLE_PERIODS = [1, 12, 24, 36, 48]; // Check more periods throughout the day
const LOOK_BACK_DAYS = 7; // Look back up to a week for potential updates
const MONTHLY_RECONCILIATION_HOUR = 2; // Run monthly reconciliation at 2 AM, before daily reconciliation

/**
 * Check if a specific day's data needs to be reprocessing by comparing
 * sample periods with the Elexon API
 */
async function needsReprocessing(date: string): Promise<boolean> {
  try {
    console.log(`Checking if ${date} needs reprocessing...`);

    // Get daily summary for comparison
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });

    console.log(`Current daily summary for ${date}:`, {
      energy: summary?.totalCurtailedEnergy ? `${Number(summary.totalCurtailedEnergy).toFixed(2)} MWh` : 'No data',
      payment: summary?.totalPayment ? `£${Number(summary.totalPayment).toFixed(2)}` : 'No data'
    });

    let totalApiVolume = 0;
    let totalApiPayment = 0;
    let totalDbVolume = 0;
    let totalDbPayment = 0;

    for (const period of SAMPLE_PERIODS) {
      const apiRecords = await fetchBidsOffers(date, period);
      console.log(`[${date} P${period}] API records: ${apiRecords.length}`);

      // Calculate API totals for this period
      const apiTotal = apiRecords.reduce((acc, record) => ({
        volume: acc.volume + Math.abs(record.volume),
        payment: acc.payment + (Math.abs(record.volume) * record.originalPrice)
      }), { volume: 0, payment: 0 });

      totalApiVolume += apiTotal.volume;
      totalApiPayment += apiTotal.payment;

      // Get existing records for this period
      const dbRecords = await db
        .select({
          totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
          totalPayment: sql<string>`SUM(ABS(${curtailmentRecords.payment}::numeric))`
        })
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, date),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );

      const dbTotal = {
        volume: Number(dbRecords[0]?.totalVolume || 0),
        payment: Number(dbRecords[0]?.totalPayment || 0)
      };

      totalDbVolume += dbTotal.volume;
      totalDbPayment += dbTotal.payment;

      // Compare totals with a small tolerance for floating point differences
      const volumeDiff = Math.abs(apiTotal.volume - dbTotal.volume);
      const paymentDiff = Math.abs(apiTotal.payment - dbTotal.payment);

      if (volumeDiff > 0.01 || paymentDiff > 0.01) {
        console.log(`[${date} P${period}] Differences detected:`, {
          volumeDiff: volumeDiff.toFixed(3),
          paymentDiff: paymentDiff.toFixed(3)
        });
        return true;
      }
    }

    // Compare total daily values
    const avgVolumeDiff = Math.abs((totalApiVolume / SAMPLE_PERIODS.length) - Number(summary?.totalCurtailedEnergy || 0));
    const avgPaymentDiff = Math.abs((totalApiPayment / SAMPLE_PERIODS.length) - Number(summary?.totalPayment || 0));

    if (avgVolumeDiff > 1 || avgPaymentDiff > 10) {
      console.log('Significant daily total differences detected:', {
        volumeDiff: avgVolumeDiff.toFixed(2),
        paymentDiff: avgPaymentDiff.toFixed(2)
      });
      return true;
    }

    return false;
  } catch (error) {
    console.error(`Error checking data for ${date}:`, error);
    return true; // Reprocess on error to be safe
  }
}

export async function reconcileDay(date: string): Promise<void> {
  try {
    if (await needsReprocessing(date)) {
      console.log(`[${date}] Data differences detected, reprocessing...`);
      await processDailyCurtailment(date);

      // Verify the update
      const summary = await db.query.dailySummaries.findFirst({
        where: eq(dailySummaries.summaryDate, date)
      });

      console.log(`[${date}] Reprocessing complete:`, {
        energy: `${Number(summary?.totalCurtailedEnergy || 0).toFixed(2)} MWh`,
        payment: `£${Number(summary?.totalPayment || 0).toFixed(2)}`
      });

      // Update Bitcoin calculations after curtailment data is updated
      console.log(`[${date}] Updating Bitcoin calculations...`);

      // Process for each miner model to maintain historical calculations
      const minerModels = ['S19J_PRO', 'S9', 'M20S'];
      for (const minerModel of minerModels) {
        await processSingleDay(date, minerModel)
          .catch(error => {
            console.error(`Error processing Bitcoin calculations for ${date} with ${minerModel}:`, error);
            // Continue with other models even if one fails
          });
      }

      console.log(`[${date}] Bitcoin calculations updated`);
    } else {
      console.log(`[${date}] Data is up to date`);
    }
  } catch (error) {
    console.error(`Error reconciling data for ${date}:`, error);
    throw error;
  }
}

export async function reconcileRecentData(): Promise<void> {
  try {
    const now = new Date();
    const startDate = subDays(now, LOOK_BACK_DAYS);
    const dates: string[] = [];

    // Add recent days
    let currentDate = startDate;
    while (isBefore(currentDate, now)) {
      dates.push(format(currentDate, 'yyyy-MM-dd'));
      currentDate = new Date(currentDate.setDate(currentDate.getDate() + 1));
    }

    console.log(`Starting reconciliation for recent days (${format(startDate, 'yyyy-MM-dd')} to ${format(now, 'yyyy-MM-dd')})`);

    // Process dates in batches
    for (let i = 0; i < dates.length; i += MAX_CONCURRENT_DAYS) {
      const batch = dates.slice(i, i + MAX_CONCURRENT_DAYS);
      await Promise.all(batch.map(date => reconcileDay(date)));
    }

    console.log('Completed reconciliation of recent data');
  } catch (error) {
    console.error('Error during recent data reconciliation:', error);
    throw error;
  }
}

export async function reconcilePreviousMonth(): Promise<void> {
  try {
    const now = new Date();
    const previousMonth = subMonths(now, 1);
    const startDate = startOfMonth(previousMonth);
    const endDate = endOfMonth(previousMonth);

    console.log(`Starting reconciliation for previous month: ${format(previousMonth, 'yyyy-MM')}`);

    // Get all dates in the previous month
    const dates: string[] = [];
    let currentDate = startDate;

    while (isBefore(currentDate, endDate)) {
      dates.push(format(currentDate, 'yyyy-MM-dd'));
      currentDate = new Date(currentDate.setDate(currentDate.getDate() + 1));
    }

    // Process dates in batches
    for (let i = 0; i < dates.length; i += MAX_CONCURRENT_DAYS) {
      const batch = dates.slice(i, i + MAX_CONCURRENT_DAYS);
      await Promise.all(batch.map(date => reconcileDay(date)));
    }

    console.log(`Completed reconciliation for ${format(previousMonth, 'yyyy-MM')}`);
  } catch (error) {
    console.error('Error during previous month reconciliation:', error);
    throw error;
  }
}

export function shouldRunReconciliation(): boolean {
  const currentHour = new Date().getHours();
  return currentHour === RECONCILIATION_HOUR;
}

export function shouldRunMonthlyReconciliation(): boolean {
  const currentHour = new Date().getHours();
  return currentHour === MONTHLY_RECONCILIATION_HOUR;
}

export async function reconcileYearlyData(): Promise<void> {
  try {
    const currentYear = new Date().getFullYear();
    console.log(`Starting yearly data reconciliation for ${currentYear}`);

    // Calculate totals from monthly summaries
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${monthlySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(ABS(${monthlySummaries.totalPayment}::numeric))`
      })
      .from(monthlySummaries)
      .where(sql`TO_DATE(${monthlySummaries.yearMonth} || '-01', 'YYYY-MM-DD')::date >= DATE_TRUNC('year', NOW())::date
            AND TO_DATE(${monthlySummaries.yearMonth} || '-01', 'YYYY-MM-DD')::date < DATE_TRUNC('year', NOW())::date + INTERVAL '1 year'`);

    if (monthlyTotals[0]?.totalCurtailedEnergy) {
      // Update yearly summary
      await db.insert(yearlySummaries).values({
        year: currentYear.toString(),
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
        totalPayment: monthlyTotals[0].totalPayment,
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [yearlySummaries.year],
        set: {
          totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
          totalPayment: monthlyTotals[0].totalPayment,
          updatedAt: new Date()
        }
      });

      console.log(`Updated yearly summary for ${currentYear}:`, {
        energy: Number(monthlyTotals[0].totalCurtailedEnergy).toFixed(2),
        payment: Number(monthlyTotals[0].totalPayment).toFixed(2)
      });
    }
  } catch (error) {
    console.error('Error during yearly reconciliation:', error);
  }
}
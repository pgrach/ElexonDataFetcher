import { db } from "@db";
import { format, subMinutes } from "date-fns";
import { eq, and, between } from "drizzle-orm";
import { sql } from "drizzle-orm";
import { fetchBidsOffers } from "./elexon";
import { curtailmentRecords, dailySummaries, monthlySummaries } from "@db/schema";
import type { ElexonBidOffer } from "../types/elexon";

const UPDATE_INTERVAL = 30 * 60 * 1000; // 30 minutes in milliseconds
let isUpdating = false;

async function getCurrentPeriod(): Promise<{ date: string; period: number }> {
  const now = new Date();
  const minutes = now.getHours() * 60 + now.getMinutes();
  const currentPeriod = Math.floor(minutes / 30) + 1;
  return {
    date: format(now, 'yyyy-MM-dd'),
    period: currentPeriod
  };
}

async function updateDailySummary(date: string) {
  // Calculate totals based on volume and original price
  const records = await db
    .select({
      totalVolume: sql<string>`COALESCE(sum(${curtailmentRecords.volume}), 0)::numeric`,
      // Payment should be negative (volume * negative_price)
      totalPayment: sql<string>`COALESCE(sum(${curtailmentRecords.volume}::numeric * ${curtailmentRecords.originalPrice}::numeric), 0)::numeric`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));

  const [totals] = records;
  console.log("Daily totals for", date, ":", totals);

  await db
    .insert(dailySummaries)
    .values({
      summaryDate: date,
      totalCurtailedEnergy: totals.totalVolume?.toString() || "0",
      totalPayment: totals.totalPayment?.toString() || "0"
    })
    .onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totals.totalVolume?.toString() || "0",
        totalPayment: totals.totalPayment?.toString() || "0"
      }
    });
}

async function updateMonthlySummary(yearMonth: string) {
  const [year, month] = yearMonth.split('-');
  const startDate = `${year}-${month}-01`;
  const endDate = `${year}-${month}-31`;

  const records = await db
    .select({
      totalVolume: sql<string>`COALESCE(sum(${curtailmentRecords.volume}), 0)::numeric`,
      // Payment should be negative (volume * negative_price)
      totalPayment: sql<string>`COALESCE(sum(${curtailmentRecords.volume}::numeric * ${curtailmentRecords.originalPrice}::numeric), 0)::numeric`
    })
    .from(curtailmentRecords)
    .where(
      and(
        between(
          curtailmentRecords.settlementDate,
          startDate,
          endDate
        )
      )
    );

  const [totals] = records;
  console.log("Monthly totals for", yearMonth, ":", totals);

  await db
    .insert(monthlySummaries)
    .values({
      yearMonth,
      totalCurtailedEnergy: totals.totalVolume?.toString() || "0",
      totalPayment: totals.totalPayment?.toString() || "0"
    })
    .onConflictDoUpdate({
      target: [monthlySummaries.yearMonth],
      set: {
        totalCurtailedEnergy: totals.totalVolume?.toString() || "0",
        totalPayment: totals.totalPayment?.toString() || "0",
        updatedAt: new Date()
      }
    });
}

async function processBidsOffers(records: ElexonBidOffer[]) {
  for (const record of records) {
    // Store volume as positive
    const volume = Math.abs(record.volume);
    // Payment is negative (volume * negative_price)
    const payment = volume * record.originalPrice;

    await db
      .insert(curtailmentRecords)
      .values({
        settlementDate: record.settlementDate,
        settlementPeriod: record.settlementPeriod,
        farmId: record.id,
        volume: volume.toString(),
        payment: payment.toString(),
        originalPrice: record.originalPrice.toString(),
        finalPrice: record.finalPrice.toString(),
        soFlag: record.soFlag,
        cadlFlag: record.cadlFlag || false
      })
      .onConflictDoNothing({
        target: [
          curtailmentRecords.settlementDate,
          curtailmentRecords.settlementPeriod,
          curtailmentRecords.farmId
        ]
      });
  }
}

async function updateLatestData() {
  if (isUpdating) {
    console.log("Update already in progress, skipping...");
    return;
  }

  isUpdating = true;
  try {
    const { date, period } = await getCurrentPeriod();

    // Fetch data for current and previous period to ensure no gaps
    const previousPeriod = period === 1 ? 48 : period - 1;
    const previousDate = period === 1 ? 
      format(subMinutes(new Date(), 30), 'yyyy-MM-dd') : 
      date;

    console.log(`Fetching data for ${date} P${period} and ${previousDate} P${previousPeriod}`);

    const [currentRecords, previousRecords] = await Promise.all([
      fetchBidsOffers(date, period),
      fetchBidsOffers(previousDate, previousPeriod)
    ]);

    // Process records
    await Promise.all([
      processBidsOffers(currentRecords),
      processBidsOffers(previousRecords)
    ]);

    // Update summaries
    await Promise.all([
      updateDailySummary(date),
      updateDailySummary(previousDate),
      updateMonthlySummary(format(new Date(date), 'yyyy-MM')),
      updateMonthlySummary(format(new Date(previousDate), 'yyyy-MM'))
    ]);

    console.log(`Successfully updated data for ${date} P${period}`);
  } catch (error) {
    console.error("Error updating latest data:", error);
  } finally {
    isUpdating = false;
  }
}

export function startDataUpdateService() {
  console.log("Starting real-time data update service...");

  // Initial update
  updateLatestData().catch(console.error);

  // Schedule periodic updates
  setInterval(updateLatestData, UPDATE_INTERVAL);
}
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
  const records = await db
    .select({
      totalVolume: sql<string>`sum(${curtailmentRecords.volume})`,
      totalPayment: sql<string>`sum(${curtailmentRecords.payment})`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));

  const [totals] = records;

  await db
    .insert(dailySummaries)
    .values({
      summaryDate: date,
      totalCurtailedEnergy: totals.totalVolume || "0",
      totalPayment: totals.totalPayment || "0"
    })
    .onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totals.totalVolume || "0",
        totalPayment: totals.totalPayment || "0"
      }
    });
}

async function updateMonthlySummary(yearMonth: string) {
  const [year, month] = yearMonth.split('-');
  const startDate = `${year}-${month}-01`;
  const endDate = `${year}-${month}-31`;

  const records = await db
    .select({
      totalVolume: sql<string>`sum(${curtailmentRecords.volume})`,
      totalPayment: sql<string>`sum(${curtailmentRecords.payment})`
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

  await db
    .insert(monthlySummaries)
    .values({
      yearMonth,
      totalCurtailedEnergy: totals.totalVolume || "0",
      totalPayment: totals.totalPayment || "0"
    })
    .onConflictDoUpdate({
      target: [monthlySummaries.yearMonth],
      set: {
        totalCurtailedEnergy: totals.totalVolume || "0",
        totalPayment: totals.totalPayment || "0"
      }
    });
}

async function processBidsOffers(records: ElexonBidOffer[]) {
  for (const record of records) {
    // Fix payment calculation: For curtailment, payment should be negative
    // If volume is negative and price is negative, payment should be negative
    const payment = Math.abs(record.volume) * record.originalPrice;

    await db
      .insert(curtailmentRecords)
      .values({
        settlementDate: record.settlementDate,
        settlementPeriod: record.settlementPeriod,
        farmId: record.id,
        volume: Math.abs(record.volume).toString(),
        payment: payment.toString(), // Remove the -1 multiplier to keep payments negative
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
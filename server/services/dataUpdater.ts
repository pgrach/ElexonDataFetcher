import { db } from "@db";
import { format, startOfToday, subMinutes } from "date-fns";
import { eq } from "drizzle-orm";
import { sql } from "drizzle-orm";
import { fetchBidsOffers } from "./elexon";
import { curtailmentRecords } from "@db/schema";
import { processDailyCurtailment } from "./curtailment";
import type { ElexonBidOffer } from "../types/elexon";
import { reconcileCurrentMonth, shouldRunReconciliation } from "./historicalReconciliation";

const UPDATE_INTERVAL = 15 * 60 * 1000; // 15 minutes in milliseconds
const RECONCILIATION_CHECK_INTERVAL = 60 * 60 * 1000; // Check every hour
let isUpdating = false;
let lastReconciliationDate: string | null = null;
let lastReconciliationCheck = 0;

async function getCurrentPeriod(): Promise<{ date: string; period: number }> {
  const now = new Date();
  const minutes = now.getHours() * 60 + now.getMinutes();
  const currentPeriod = Math.floor(minutes / 30) + 1;
  return {
    date: format(now, 'yyyy-MM-dd'),
    period: currentPeriod
  };
}

async function hasDataChanged(records: ElexonBidOffer[], date: string, period: number): Promise<boolean> {
  const existingRecords = await db
    .select({
      farmId: curtailmentRecords.farmId,
      volume: curtailmentRecords.volume,
      payment: curtailmentRecords.payment,
      originalPrice: curtailmentRecords.originalPrice,
      soFlag: curtailmentRecords.soFlag,
      cadlFlag: curtailmentRecords.cadlFlag
    })
    .from(curtailmentRecords)
    .where(
      sql`${curtailmentRecords.settlementDate} = ${date} AND 
          ${curtailmentRecords.settlementPeriod} = ${period}`
    );

  if (existingRecords.length !== records.length) {
    console.log(`[${date} P${period}] Record count changed: API=${records.length}, DB=${existingRecords.length}`);
    return true;
  }

  const newRecordsMap = new Map(
    records.map(r => [
      r.id,
      {
        volume: Math.abs(r.volume).toString(),
        payment: (Math.abs(r.volume) * r.originalPrice).toString(),
        originalPrice: r.originalPrice.toString(),
        soFlag: r.soFlag,
        cadlFlag: r.cadlFlag
      }
    ])
  );

  const hasChanges = existingRecords.some(record => {
    const newRecord = newRecordsMap.get(record.farmId);
    if (!newRecord) return true;

    return newRecord.volume !== record.volume ||
           newRecord.payment !== record.payment ||
           newRecord.originalPrice !== record.originalPrice ||
           newRecord.soFlag !== record.soFlag ||
           newRecord.cadlFlag !== record.cadlFlag;
  });

  if (hasChanges) {
    console.log(`[${date} P${period}] Data values have changed`);
  }

  return hasChanges;
}

async function upsertRecords(records: ElexonBidOffer[], date: string, period: number): Promise<void> {
  for (const record of records) {
    const volume = Math.abs(record.volume);
    const payment = volume * record.originalPrice;

    try {
      await db.insert(curtailmentRecords)
        .values({
          settlementDate: date,
          settlementPeriod: period,
          farmId: record.id,
          leadPartyName: record.leadPartyName || 'Unknown',
          volume: volume.toString(),
          payment: payment.toString(),
          originalPrice: record.originalPrice.toString(),
          finalPrice: record.finalPrice.toString(),
          soFlag: record.soFlag,
          cadlFlag: record.cadlFlag
        })
        .onConflictDoUpdate({
          target: [
            curtailmentRecords.settlementDate,
            curtailmentRecords.settlementPeriod,
            curtailmentRecords.farmId
          ],
          set: {
            volume: volume.toString(),
            payment: payment.toString(),
            originalPrice: record.originalPrice.toString(),
            finalPrice: record.finalPrice.toString(),
            soFlag: record.soFlag,
            cadlFlag: record.cadlFlag
          }
        });
    } catch (error) {
      console.error(`Error upserting record for ${record.id}:`, error);
    }
  }
}

async function updateLatestData() {
  if (isUpdating) {
    console.log("Update already in progress, skipping...");
    return;
  }

  isUpdating = true;
  try {
    const { date, period: currentPeriod } = await getCurrentPeriod();
    console.log(`Starting data refresh for ${date} up to period ${currentPeriod}`);

    let dataChanged = false;

    // Fetch all periods up to current for today
    for (let period = 1; period <= currentPeriod; period++) {
      try {
        const records = await fetchBidsOffers(date, period);
        const changed = await hasDataChanged(records, date, period);

        if (changed) {
          console.log(`Changes detected for ${date} P${period}, updating records...`);
          await upsertRecords(records, date, period);
          dataChanged = true;
        }
      } catch (error) {
        console.error(`Error processing period ${period} for ${date}:`, error);
        continue;
      }
    }

    // If any changes were detected, re-run the daily aggregation
    if (dataChanged) {
      console.log(`Re-running daily aggregation for ${date} due to data changes`);
      await processDailyCurtailment(date);
    }

    // Check if we should run historical reconciliation
    const now = Date.now();
    if (now - lastReconciliationCheck >= RECONCILIATION_CHECK_INTERVAL) {
      if (shouldRunReconciliation() && lastReconciliationDate !== format(new Date(), 'yyyy-MM-dd')) {
        console.log('Starting historical data reconciliation...');
        await reconcileCurrentMonth();
        lastReconciliationDate = format(new Date(), 'yyyy-MM-dd');
        console.log('Historical reconciliation completed');
      }
      lastReconciliationCheck = now;
    }

  } catch (error) {
    console.error("Error updating latest data:", error);
  } finally {
    isUpdating = false;
  }
}

export function startDataUpdateService() {
  console.log("Starting real-time data update service...");
  updateLatestData().catch(console.error);
  setInterval(updateLatestData, UPDATE_INTERVAL);
}
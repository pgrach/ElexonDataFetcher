import { db } from "@db";
import { format, startOfToday, subMinutes } from "date-fns";
import { eq } from "drizzle-orm";
import { sql } from "drizzle-orm";
import { fetchBidsOffers } from "./elexon";
import { curtailmentRecords } from "@db/schema";
import { processDailyCurtailment } from "./curtailment";
import type { ElexonBidOffer } from "../types/elexon";

const UPDATE_INTERVAL = 15 * 60 * 1000; // 15 minutes in milliseconds
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

async function hasDataChanged(records: ElexonBidOffer[], date: string, period: number): Promise<boolean> {
  const existingRecords = await db
    .select({
      farmId: curtailmentRecords.farmId,
      volume: curtailmentRecords.volume,
      payment: curtailmentRecords.payment
    })
    .from(curtailmentRecords)
    .where(
      sql`${curtailmentRecords.settlementDate} = ${date} AND 
          ${curtailmentRecords.settlementPeriod} = ${period}`
    );

  if (existingRecords.length !== records.length) return true;

  const newRecordsMap = new Map(
    records.map(r => [
      r.id,
      {
        volume: Math.abs(r.volume).toString(),
        payment: (Math.abs(r.volume) * r.originalPrice).toString()
      }
    ])
  );

  return existingRecords.some(record => {
    const newRecord = newRecordsMap.get(record.farmId);
    if (!newRecord) return true;
    return newRecord.volume !== record.volume || newRecord.payment !== record.payment;
  });
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

        // Check if data has changed from what we have stored
        const changed = await hasDataChanged(records, date, period);
        if (changed) {
          console.log(`Changes detected for ${date} P${period}, updating records...`);
          dataChanged = true;
        }
      } catch (error) {
        console.error(`Error fetching data for ${date} P${period}:`, error);
        continue; // Continue with next period even if one fails
      }
    }

    // If any changes were detected, re-run the daily aggregation
    if (dataChanged) {
      console.log(`Re-running daily aggregation for ${date} due to data changes`);
      await processDailyCurtailment(date);
    } else {
      console.log(`No changes detected for ${date}, skipping aggregation`);
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
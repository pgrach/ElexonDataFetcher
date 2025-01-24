import { db } from "@db";
import { format, subMinutes } from "date-fns";
import { eq } from "drizzle-orm";
import { sql } from "drizzle-orm";
import { fetchBidsOffers } from "./elexon";
import { curtailmentRecords } from "@db/schema";
import { processDailyCurtailment } from "./curtailment";
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

    // Process both dates through the main processDailyCurtailment function
    await Promise.all([
      processDailyCurtailment(date),
      previousDate !== date && processDailyCurtailment(previousDate)
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
  updateLatestData().catch(console.error);
  setInterval(updateLatestData, UPDATE_INTERVAL);
}
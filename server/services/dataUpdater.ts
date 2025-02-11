import { db } from "@db";
import { format, startOfToday, subMinutes } from "date-fns";
import { eq } from "drizzle-orm";
import { sql } from "drizzle-orm";
import { fetchBidsOffers } from "./elexon";
import { curtailmentRecords } from "@db/schema";
import { processDailyCurtailment } from "./curtailment";
import type { ElexonBidOffer } from "../types/elexon";
import { 
  reconcileRecentData, 
  reconcilePreviousMonth,
  reconcileYearlyData,
  shouldRunReconciliation, 
  shouldRunMonthlyReconciliation 
} from "./historicalReconciliation";

const UPDATE_INTERVAL = 5 * 60 * 1000; // 5 minutes
const RECONCILIATION_CHECK_INTERVAL = 30 * 60 * 1000;
let isUpdating = false;
let lastReconciliationDate: string | null = null;
let lastMonthlyReconciliationDate: string | null = null;
let lastReconciliationCheck = 0;
let serviceStartTime: Date | null = null;
let lastUpdateTime: Date | null = null;

async function getCurrentPeriod(): Promise<{ date: string; period: number }> {
  const now = new Date();
  const minutes = now.getHours() * 60 + now.getMinutes();
  const currentPeriod = Math.floor(minutes / 30) + 1;

  console.log(`\nCurrent time: ${format(now, 'yyyy-MM-dd HH:mm:ss')}`);
  console.log(`Calculated period: ${currentPeriod} for the day`);
  console.log(`Last successful update: ${lastUpdateTime?.toISOString() || 'Never'}`);

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
        volume: r.volume.toString(),
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
          volume: record.volume.toString(),
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
            volume: record.volume.toString(),
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
  const startTime = Date.now();

  try {
    const { date, period: currentPeriod } = await getCurrentPeriod();
    console.log(`\n=== Starting data refresh at ${new Date().toISOString()} ===`);
    console.log(`Service running since: ${serviceStartTime?.toISOString()}`);
    console.log(`Fetching data for ${date} up to period ${currentPeriod}`);

    // Add a verification query to check existing data
    const existingData = await db
      .select({
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    console.log(`Current data state for ${date}:`, {
      periodCount: existingData[0]?.periodCount || 0,
      totalVolume: existingData[0]?.totalVolume ? `${Number(existingData[0].totalVolume).toFixed(2)} MWh` : '0 MWh'
    });

    let dataChanged = false;
    let totalRecords = 0;

    // Fetch all periods up to current for today
    for (let period = 1; period <= currentPeriod; period++) {
      try {
        console.log(`\nFetching period ${period}...`);
        const records = await fetchBidsOffers(date, period);
        totalRecords += records.length;

        if (records.length > 0) {
          console.log(`Retrieved ${records.length} records for period ${period}`);
        }

        const changed = await hasDataChanged(records, date, period);
        if (changed) {
          console.log(`Changes detected for ${date} P${period}, updating ${records.length} records...`);
          await upsertRecords(records, date, period);
          dataChanged = true;
        } else {
          console.log(`No changes for ${date} P${period}`);
        }
      } catch (error) {
        console.error(`Error processing period ${period} for ${date}:`, error);
        continue;
      }
    }

    if (dataChanged) {
      console.log(`Re-running daily aggregation for ${date} due to data changes`);
      await processDailyCurtailment(date);
      lastUpdateTime = new Date();
    }

    const duration = (Date.now() - startTime) / 1000;
    console.log(`\n=== Update completed in ${duration.toFixed(1)}s ===`);
    console.log(`Total records processed: ${totalRecords}`);

    // Verify final state after update
    const finalState = await db
      .select({
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    console.log(`Final data state for ${date}:`, {
      periodCount: finalState[0]?.periodCount || 0,
      totalVolume: finalState[0]?.totalVolume ? `${Number(finalState[0].totalVolume).toFixed(2)} MWh` : '0 MWh'
    });

  } catch (error) {
    console.error("Error updating latest data:", error);
  } finally {
    isUpdating = false;
  }
}

export function startDataUpdateService() {
  serviceStartTime = new Date();
  console.log(`\n=== Starting real-time data update service at ${serviceStartTime.toISOString()} ===`);

  // Run immediately on startup
  updateLatestData().catch(error => {
    console.error("Error during initial data update:", error);
  });

  // Set up regular interval
  const intervalId = setInterval(async () => {
    try {
      await updateLatestData();
    } catch (error) {
      console.error("Error during scheduled update:", error);
      // Service will continue running despite errors
    }
  }, UPDATE_INTERVAL);

  // Log heartbeat every hour
  setInterval(() => {
    console.log(`\n=== Data update service heartbeat ===`);
    console.log(`Service running since: ${serviceStartTime?.toISOString()}`);
    console.log(`Current time: ${new Date().toISOString()}`);
  }, 60 * 60 * 1000);

  return intervalId;
}
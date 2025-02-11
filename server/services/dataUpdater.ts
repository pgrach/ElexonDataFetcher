import { db } from "@db";
import { format } from 'date-fns';
import { eq } from "drizzle-orm";
import { sql } from "drizzle-orm";
import { fetchBidsOffers } from "./elexon";
import { curtailmentRecords } from "@db/schema";
import { processDailyCurtailment } from "./curtailment";
import type { ElexonBidOffer } from "../types/elexon";

const UPDATE_INTERVAL = 5 * 60 * 1000; // 5 minutes
const STARTUP_DELAY = 5000; // 5 second delay before starting data updates
let isUpdating = false;
let serviceStartTime: Date | null = null;
let lastUpdateTime: Date | null = null;

async function getCurrentPeriod(): Promise<{ date: string; period: number }> {
  const now = new Date();
  const minutes = now.getHours() * 60 + now.getMinutes();
  const currentPeriod = Math.floor(minutes / 30) + 1;

  console.log(`\n=== Current Time Information ===`);
  console.log(`Time: ${format(now, 'yyyy-MM-dd HH:mm:ss')}`);
  console.log(`Period: ${currentPeriod}`);
  console.log(`Last Update: ${lastUpdateTime?.toISOString() || 'Never'}`);

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
  const startTime = Date.now();

  try {
    const { date, period: currentPeriod } = await getCurrentPeriod();
    console.log(`\n=== Starting Data Update ===`);
    console.log(`Date: ${date}, Current Period: ${currentPeriod}`);
    console.log(`Service Running Since: ${serviceStartTime?.toISOString()}`);

    let dataChanged = false;
    let totalRecords = 0;

    // Delete existing records for today to prevent duplicates
    try {
      const deleteResult = await db.delete(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date));
      console.log(`Cleared existing records for ${date}`);
    } catch (error) {
      console.error(`Error clearing existing records for ${date}:`, error);
      throw error; // Re-throw to halt the update if we can't clear existing records
    }

    // Fetch all periods up to current for today
    for (let period = 1; period <= currentPeriod; period++) {
      try {
        console.log(`\nProcessing Period ${period}...`);
        const records = await fetchBidsOffers(date, period);

        if (records.length > 0) {
          const totalVolume = records.reduce((sum, r) => sum + Math.abs(r.volume), 0);
          const totalPayment = records.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice), 0);

          console.log(`[${date} P${period}] Records: ${records.length} (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);

          for (const record of records) {
            const volume = record.volume;
            const payment = Math.abs(record.volume) * record.originalPrice;

            try {
              await db.insert(curtailmentRecords).values({
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
              });

              console.log(`[${date} P${period}] Added record for ${record.id}: ${Math.abs(volume)} MWh, £${payment}`);
              dataChanged = true;
              totalRecords++;
            } catch (error) {
              console.error(`Error inserting record for ${record.id}:`, error);
              console.error('Record data:', JSON.stringify(record, null, 2));
              throw error; // Re-throw to ensure we catch data insertion failures
            }
          }

          // Verify records were inserted for this period
          const periodCheck = await db
            .select({
              count: sql<number>`COUNT(*)`,
              totalVolume: sql<string>`SUM(ABS(volume::numeric))`
            })
            .from(curtailmentRecords)
            .where(
              sql`settlement_date = ${date} AND settlement_period = ${period}`
            );

          console.log(`[${date} P${period}] Verification:`, {
            recordsFound: periodCheck[0].count,
            totalVolume: Number(periodCheck[0].totalVolume).toFixed(2)
          });
        }
      } catch (error) {
        console.error(`Error processing period ${period}:`, error);
        throw error; // Re-throw to ensure we catch period processing failures
      }
    }

    if (dataChanged) {
      console.log(`\nRe-running daily aggregation for ${date}`);
      await processDailyCurtailment(date);
      lastUpdateTime = new Date();
    }

    const duration = (Date.now() - startTime) / 1000;
    console.log(`\n=== Update Summary ===`);
    console.log(`Duration: ${duration.toFixed(1)}s`);
    console.log(`Records Processed: ${totalRecords}`);

    // Verify final state
    const finalState = await db
      .select({
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        recordCount: sql<number>`COUNT(*)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    console.log(`Final State for ${date}:`, {
      periods: finalState[0]?.periodCount || 0,
      records: finalState[0]?.recordCount || 0,
      volume: finalState[0]?.totalVolume ? `${Number(finalState[0].totalVolume).toFixed(2)} MWh` : '0 MWh'
    });

  } catch (error) {
    console.error("Error updating latest data:", error);
    throw error; // Re-throw to ensure the error is logged and handled properly
  } finally {
    isUpdating = false;
  }
}

export function startDataUpdateService() {
  serviceStartTime = new Date();
  console.log(`\n=== Starting Data Update Service ===`);
  console.log(`Start Time: ${serviceStartTime.toISOString()}`);

  // Add startup delay to ensure server is ready
  setTimeout(() => {
    // Run initial update
    updateLatestData().catch(error => {
      console.error("Error during initial data update:", error);
    });

    // Set up regular interval
    const intervalId = setInterval(async () => {
      try {
        await updateLatestData();
      } catch (error) {
        console.error("Error during scheduled update:", error);
      }
    }, UPDATE_INTERVAL);

    // Log heartbeat every hour
    setInterval(() => {
      console.log(`\n=== Service Heartbeat ===`);
      console.log(`Running Since: ${serviceStartTime?.toISOString()}`);
      console.log(`Current Time: ${new Date().toISOString()}`);
    }, 60 * 60 * 1000);

    return intervalId;
  }, STARTUP_DELAY);
}
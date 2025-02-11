import { db } from "@db";
import { format } from 'date-fns';
import { eq, and } from "drizzle-orm";
import { sql } from "drizzle-orm";
import { fetchBidsOffers } from "./elexon";
import { curtailmentRecords } from "@db/schema";
import { processDailyCurtailment } from "./curtailment";
import type { ElexonBidOffer } from "../types/elexon";
import { processSingleDay } from "./bitcoinService";

const UPDATE_INTERVAL = 5 * 60 * 1000; // 5 minutes
const STARTUP_DELAY = 5000; // 5 second delay before starting data updates
const MAX_RETRIES = 3;
const RETRY_DELAY = 10000; // 10 seconds between retries
let isUpdating = false;
let serviceStartTime: Date | null = null;
let lastUpdateTime: Date | null = null;
let lastProcessedPeriod: number | null = null;

async function getCurrentPeriod(): Promise<{ date: string; period: number }> {
  const now = new Date();
  const minutes = now.getHours() * 60 + now.getMinutes();
  const currentPeriod = Math.floor(minutes / 30) + 1;

  console.log(`\n=== Current Time Information ===`);
  console.log(`Time: ${format(now, 'yyyy-MM-dd HH:mm:ss')}`);
  console.log(`Period: ${currentPeriod}`);
  console.log(`Last Update: ${lastUpdateTime?.toISOString() || 'Never'}`);
  console.log(`Last Processed Period: ${lastProcessedPeriod || 'None'}`);

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
  let retryCount = 0;

  try {
    const { date, period: currentPeriod } = await getCurrentPeriod();

    // Only process if we haven't processed this period yet or if it's been more than 2 minutes
    if (lastProcessedPeriod === currentPeriod && 
        lastUpdateTime && 
        (Date.now() - lastUpdateTime.getTime() < 120000)) {
      console.log(`Period ${currentPeriod} was recently processed, skipping...`);
      return;
    }

    console.log(`\n=== Starting Data Update ===`);
    console.log(`Date: ${date}, Current Period: ${currentPeriod}`);
    console.log(`Service Running Since: ${serviceStartTime?.toISOString()}`);

    while (retryCount < MAX_RETRIES) {
      try {
        let dataChanged = false;
        let totalRecords = 0;

        // Process current period only
        console.log(`\nProcessing Period ${currentPeriod}...`);
        const records = await fetchBidsOffers(date, currentPeriod);

        if (records.length > 0) {
          const totalVolume = records.reduce((sum, r) => sum + Math.abs(r.volume), 0);
          const totalPayment = records.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice), 0);

          console.log(`[${date} P${currentPeriod}] Records: ${records.length} (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);

          // Delete existing records for this period only
          await db.delete(curtailmentRecords)
            .where(
              and(
                eq(curtailmentRecords.settlementDate, date),
                eq(curtailmentRecords.settlementPeriod, currentPeriod)
              )
            );

          for (const record of records) {
            const volume = record.volume;
            const payment = Math.abs(record.volume) * record.originalPrice;

            await db.insert(curtailmentRecords).values({
              settlementDate: date,
              settlementPeriod: currentPeriod,
              farmId: record.id,
              leadPartyName: record.leadPartyName || 'Unknown',
              volume: volume.toString(),
              payment: payment.toString(),
              originalPrice: record.originalPrice.toString(),
              finalPrice: record.finalPrice.toString(),
              soFlag: record.soFlag,
              cadlFlag: record.cadlFlag
            });

            dataChanged = true;
            totalRecords++;
          }

          if (dataChanged) {
            console.log(`\nRe-running daily aggregation for ${date}`);
            await processDailyCurtailment(date);

            // Update Bitcoin calculations for all miner models
            console.log(`\nUpdating Bitcoin calculations for ${date}`);
            const minerModels = ['S19J_PRO', 'S9', 'M20S'];

            for (const minerModel of minerModels) {
              try {
                await processSingleDay(date, minerModel);
                console.log(`[${date}] Completed Bitcoin calculations for ${minerModel}`);
              } catch (error) {
                console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
                // Continue with other models even if one fails
              }
            }

            lastUpdateTime = new Date();
            lastProcessedPeriod = currentPeriod;
          }

          // Verify records were inserted
          const periodCheck = await db
            .select({
              count: sql<number>`COUNT(*)`,
              totalVolume: sql<string>`SUM(ABS(volume::numeric))`
            })
            .from(curtailmentRecords)
            .where(
              and(
                eq(curtailmentRecords.settlementDate, date),
                eq(curtailmentRecords.settlementPeriod, currentPeriod)
              )
            );

          console.log(`[${date} P${currentPeriod}] Verification:`, {
            recordsFound: periodCheck[0].count,
            totalVolume: Number(periodCheck[0].totalVolume).toFixed(2)
          });

          break; // Exit retry loop if successful
        }
      } catch (error) {
        console.error(`Error in attempt ${retryCount + 1}/${MAX_RETRIES}:`, error);
        retryCount++;
        if (retryCount < MAX_RETRIES) {
          await new Promise(resolve => setTimeout(resolve, RETRY_DELAY));
        } else {
          throw error;
        }
      }
    }

    const duration = (Date.now() - startTime) / 1000;
    console.log(`\n=== Update Summary ===`);
    console.log(`Duration: ${duration.toFixed(1)}s`);
    console.log(`Records Processed: ${totalRecords}`);

  } catch (error) {
    console.error("Error updating latest data:", error);
    throw error;
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
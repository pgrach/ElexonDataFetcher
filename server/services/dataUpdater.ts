import { db } from "@db";
import { format } from 'date-fns';
import { eq } from "drizzle-orm";
import { sql } from "drizzle-orm";
import { fetchBidsOffers } from "./elexon";
import { curtailmentRecords } from "@db/schema";
import { processDailyCurtailment } from "./curtailment";
import type { ElexonBidOffer } from "../types/elexon";
import { processSingleDay } from "./bitcoinService";
import { reconcileDay } from "./historicalReconciliation";
import { runDailyCheck } from "../../daily_reconciliation_check";

const UPDATE_INTERVAL = 5 * 60 * 1000; // 5 minutes
const STARTUP_DELAY = 5000; // 5 second delay before starting data updates
const MAX_RETRY_ATTEMPTS = 3;
let isUpdating = false;
let serviceStartTime: Date | null = null;
let lastUpdateTime: Date | null = null;
let lastSuccessfulUpdate: Date | null = null;

// Add health check function
export function getUpdateServiceStatus() {
  return {
    serviceStartTime,
    lastUpdateTime,
    lastSuccessfulUpdate,
    isCurrentlyUpdating: isUpdating
  };
}

async function getCurrentPeriod(): Promise<{ date: string; period: number }> {
  const now = new Date();
  const minutes = now.getHours() * 60 + now.getMinutes();
  const currentPeriod = Math.floor(minutes / 30) + 1;

  console.log(`\n=== Current Time Information ===`);
  console.log(`Time: ${format(now, 'yyyy-MM-dd HH:mm:ss')}`);
  console.log(`Period: ${currentPeriod}`);
  console.log(`Last Update: ${lastUpdateTime?.toISOString() || 'Never'}`);
  console.log(`Last Successful Update: ${lastSuccessfulUpdate?.toISOString() || 'Never'}`);

  return {
    date: format(now, 'yyyy-MM-dd'),
    period: currentPeriod
  };
}

/**
 * Updates latest data using the consolidated reconciliation system.
 * This function checks and updates both curtailment records and Bitcoin calculations
 * for the current date, using services from the historicalReconciliation module.
 */
async function updateLatestData(retryAttempt = 0) {
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
    console.log(`Retry Attempt: ${retryAttempt}/${MAX_RETRY_ATTEMPTS}`);

    // Step 1: Check if the date needs processing
    console.log(`Checking if ${date} needs reprocessing...`);
    
    // Get current daily summary for logging purposes
    const dailySummaryCheck = await db
      .select({
        totalEnergy: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    const currentSummary = {
      energy: `${Number(dailySummaryCheck[0]?.totalEnergy || 0).toFixed(2)} MWh`,
      payment: `£${Number(dailySummaryCheck[0]?.totalPayment || 0).toFixed(2)}`
    };
    
    console.log(`Current daily summary for ${date}:`, currentSummary);

    // Step 2: Use the reconciliation system to check and update if needed
    // This consolidates curtailment data processing and Bitcoin calculation
    await reconcileDay(date);

    // Step 3: Verify the update was successful by checking both curtailment and Bitcoin data
    const curtailmentCheck = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    // Record update results
    const updateResults = {
      curtailment: {
        records: curtailmentCheck[0]?.recordCount || 0,
        periods: curtailmentCheck[0]?.periodCount || 0,
        farms: curtailmentCheck[0]?.farmCount || 0,
        volume: Number(curtailmentCheck[0]?.totalVolume || 0).toFixed(2),
        payment: Number(curtailmentCheck[0]?.totalPayment || 0).toFixed(2)
      }
    };

    console.log(`\nVerification Check for ${date}:`, {
      records: updateResults.curtailment.records,
      periods: updateResults.curtailment.periods,
      volume: updateResults.curtailment.volume,
      payment: updateResults.curtailment.payment
    });

    lastSuccessfulUpdate = new Date();
    console.log(`Update successful at ${lastSuccessfulUpdate.toISOString()}`);

    const duration = (Date.now() - startTime) / 1000;
    console.log(`\n=== Update Summary ===`);
    console.log(`Duration: ${duration.toFixed(1)}s`);

    return updateResults;

  } catch (error) {
    console.error("Error updating latest data:", error);

    if (retryAttempt < MAX_RETRY_ATTEMPTS) {
      console.log(`Retrying update (Attempt ${retryAttempt + 1}/${MAX_RETRY_ATTEMPTS})`);
      setTimeout(() => {
        updateLatestData(retryAttempt + 1);
      }, 5000 * Math.pow(2, retryAttempt)); // Exponential backoff
    } else {
      throw error;
    }
  } finally {
    isUpdating = false;
    lastUpdateTime = new Date();
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

    // Enhanced heartbeat logging
    setInterval(() => {
      const now = new Date();
      const status = getUpdateServiceStatus();

      console.log(`\n=== Service Heartbeat ===`);
      console.log(`Running Since: ${status.serviceStartTime?.toISOString()}`);
      console.log(`Current Time: ${now.toISOString()}`);
      console.log(`Last Update Attempt: ${status.lastUpdateTime?.toISOString() || 'Never'}`);
      console.log(`Last Successful Update: ${status.lastSuccessfulUpdate?.toISOString() || 'Never'}`);
      console.log(`Update In Progress: ${status.isCurrentlyUpdating}`);

      // Alert if no successful updates in last 15 minutes
      if (status.lastSuccessfulUpdate) {
        const timeSinceUpdate = now.getTime() - status.lastSuccessfulUpdate.getTime();
        if (timeSinceUpdate > 15 * 60 * 1000) {
          console.error(`WARNING: No successful updates in the last ${Math.floor(timeSinceUpdate / 60000)} minutes`);
        }
      }
    }, 60 * 60 * 1000); // Every hour
    
    // Schedule daily reconciliation check at midnight
    const setupDailyReconciliationCheck = () => {
      const now = new Date();
      const currentHour = now.getHours();
      
      // Calculate milliseconds until next midnight
      const midnight = new Date(now);
      midnight.setHours(24, 0, 0, 0);
      const msUntilMidnight = midnight.getTime() - now.getTime();
      
      console.log(`Scheduling daily reconciliation check to run at midnight (in ${Math.round(msUntilMidnight / 3600000)} hours)`);
      
      // Schedule the first run
      setTimeout(() => {
        console.log("\n=== Running scheduled daily reconciliation check ===");
        runDailyCheck()
          .then(result => {
            console.log("Daily reconciliation check completed:", {
              datesChecked: result.dates.length,
              missingDates: result.missingDates.length,
              fixedDates: result.fixedDates.length,
              status: result.status
            });
            
            // Setup the next day's check
            setupDailyReconciliationCheck();
          })
          .catch(error => {
            console.error("Error during daily reconciliation check:", error);
            // Still setup next check even if there was an error
            setupDailyReconciliationCheck();
          });
      }, msUntilMidnight);
    };
    
    // Start the daily reconciliation check scheduling
    setupDailyReconciliationCheck();

    return intervalId;
  }, STARTUP_DELAY);
}
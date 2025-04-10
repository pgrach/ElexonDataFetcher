/**
 * Fix script for 2025-04-03 data
 * 
 * This script addresses the discrepancies found between Elexon API data
 * and what's stored in our database for 2025-04-03.
 * 
 * Specifically, it fixes issues in periods 35-38 where some records were
 * missing or had incorrect values.
 */

import { db } from "../db";
import { curtailmentRecords, dailySummaries, monthlySummaries } from "../db/schema";
import { fetchBidsOffers } from "../server/services/elexon";
import { eq, and, sql } from "drizzle-orm";
import logger from "../server/utils/logger";
import { addDays, format } from "date-fns";

// Target date for fixes
const TARGET_DATE = "2025-04-03";
const LOG_FILE = `logs/fix_april3_2025_${new Date().toISOString().replace(/:/g, '-')}.log`;

// Set up logger to record the fix process
logger.setup({
  file: LOG_FILE,
  console: true
});

// Function to process and save records from Elexon API
async function processAndSaveRecords(date: string, period: number, records: any[]) {
  // Filter to only keep wind farm records with negative volume (curtailment)
  const windRecords = records.filter(r => 
    r.leadPartyName && 
    Number(r.volume) < 0 &&
    r.id.startsWith('T_') // Wind farm IDs typically start with T_
  );
  
  // Log what we're processing
  logger.info(`[${date} P${period}] Processing ${windRecords.length} wind farm records`);
  
  // Save each record to the database
  for (const record of windRecords) {
    try {
      // Calculate absolute volume for logging
      const absVolume = Math.abs(Number(record.volume));
      
      // Calculate payment (negative because it's a payment to the generator)
      const payment = absVolume * Number(record.originalPrice) * -1;
      
      // Check if this record already exists in the database
      const existingRecord = await db
        .select({ id: curtailmentRecords.id })
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, date),
            eq(curtailmentRecords.settlementPeriod, period),
            eq(curtailmentRecords.farmId, record.id)
          )
        )
        .limit(1);
      
      // Skip if record already exists (to avoid duplicates)
      if (existingRecord.length > 0) {
        logger.info(`[${date} P${period}] Record for ${record.id} already exists, skipping`);
        continue;
      }
      
      // Insert the new record
      await db.insert(curtailmentRecords).values({
        settlementDate: date,
        settlementPeriod: period,
        farmId: record.id,
        leadPartyName: record.leadPartyName,
        volume: Number(record.volume),
        price: Number(record.originalPrice),
        payment: payment,
        created_at: new Date(),
        updated_at: new Date()
      });
      
      logger.info(`[${date} P${period}] Added record for ${record.id}: ${absVolume} MWh, £${payment}`);
    } catch (error) {
      logger.error(`[${date} P${period}] Error saving record for ${record.id}:`, error);
    }
  }
  
  // Calculate total volume and payment for this period
  const totalVolume = windRecords.reduce((sum, r) => sum + Math.abs(Number(r.volume)), 0);
  const totalPayment = windRecords.reduce((sum, r) => {
    const payment = Math.abs(Number(r.volume)) * Number(r.originalPrice) * -1;
    return sum + payment;
  }, 0);
  
  logger.info(`[${date} P${period}] Total: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
  
  return {
    count: windRecords.length,
    volume: totalVolume,
    payment: totalPayment
  };
}

// Update daily summary with the corrected values
async function updateDailySummary(date: string, volumeChange: number, paymentChange: number) {
  try {
    logger.info(`Updating daily summary for ${date}`);
    
    // Get existing summary
    const existingSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date))
      .limit(1);
    
    if (existingSummary.length === 0) {
      logger.error(`No daily summary found for ${date}`);
      return;
    }
    
    const summary = existingSummary[0];
    
    // Update with new values
    await db
      .update(dailySummaries)
      .set({
        totalCurtailedEnergy: Number(summary.totalCurtailedEnergy) + volumeChange,
        totalPayment: Number(summary.totalPayment) + paymentChange,
        lastUpdated: new Date()
      })
      .where(eq(dailySummaries.summaryDate, date));
    
    logger.info(`Updated daily summary for ${date}`);
    
    // Get the updated values for verification
    const updatedSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date))
      .limit(1);
    
    if (updatedSummary.length > 0) {
      logger.info(`New daily summary values: ${updatedSummary[0].totalCurtailedEnergy.toFixed(2)} MWh, £${updatedSummary[0].totalPayment.toFixed(2)}`);
    }
    
  } catch (error) {
    logger.error(`Error updating daily summary for ${date}:`, error);
  }
}

// Update monthly summary with the corrected values
async function updateMonthlySummary(date: string, volumeChange: number, paymentChange: number) {
  try {
    const yearMonth = date.substring(0, 7); // Format YYYY-MM
    logger.info(`Updating monthly summary for ${yearMonth}`);
    
    // Get existing summary
    const existingSummary = await db
      .select()
      .from(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, yearMonth))
      .limit(1);
    
    if (existingSummary.length === 0) {
      logger.error(`No monthly summary found for ${yearMonth}`);
      return;
    }
    
    const summary = existingSummary[0];
    
    // Update with new values
    await db
      .update(monthlySummaries)
      .set({
        totalCurtailedEnergy: Number(summary.totalCurtailedEnergy) + volumeChange,
        totalPayment: Number(summary.totalPayment) + paymentChange,
        updatedAt: new Date(),
        lastUpdated: new Date()
      })
      .where(eq(monthlySummaries.yearMonth, yearMonth));
    
    logger.info(`Updated monthly summary for ${yearMonth}`);
    
    // Get the updated values for verification
    const updatedSummary = await db
      .select()
      .from(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, yearMonth))
      .limit(1);
    
    if (updatedSummary.length > 0) {
      logger.info(`New monthly summary values: ${updatedSummary[0].totalCurtailedEnergy.toFixed(2)} MWh, £${updatedSummary[0].totalPayment.toFixed(2)}`);
    }
    
  } catch (error) {
    logger.error(`Error updating monthly summary for ${date}:`, error);
  }
}

// Main function to fix the data
async function fixData() {
  try {
    logger.info("===== FIXING 2025-04-03 DATA =====");
    
    let totalVolumeChange = 0;
    let totalPaymentChange = 0;
    
    // Fix period 35 - missing records
    logger.info(`\nFetching data for ${TARGET_DATE}, Period 35...`);
    const period35Records = await fetchBidsOffers(TARGET_DATE, 35);
    if (period35Records && period35Records.length > 0) {
      const result = await processAndSaveRecords(TARGET_DATE, 35, period35Records);
      totalVolumeChange += result.volume;
      totalPaymentChange += result.payment;
    } else {
      logger.error(`No records found for ${TARGET_DATE}, Period 35`);
    }
    
    // Fix period 36 - missing records
    logger.info(`\nFetching data for ${TARGET_DATE}, Period 36...`);
    const period36Records = await fetchBidsOffers(TARGET_DATE, 36);
    if (period36Records && period36Records.length > 0) {
      const result = await processAndSaveRecords(TARGET_DATE, 36, period36Records);
      totalVolumeChange += result.volume;
      totalPaymentChange += result.payment;
    } else {
      logger.error(`No records found for ${TARGET_DATE}, Period 36`);
    }
    
    // Fix period 37 - missing records
    logger.info(`\nFetching data for ${TARGET_DATE}, Period 37...`);
    const period37Records = await fetchBidsOffers(TARGET_DATE, 37);
    if (period37Records && period37Records.length > 0) {
      const result = await processAndSaveRecords(TARGET_DATE, 37, period37Records);
      totalVolumeChange += result.volume;
      totalPaymentChange += result.payment;
    } else {
      logger.error(`No records found for ${TARGET_DATE}, Period 37`);
    }
    
    // Fix period 38 - missing records
    logger.info(`\nFetching data for ${TARGET_DATE}, Period 38...`);
    const period38Records = await fetchBidsOffers(TARGET_DATE, 38);
    if (period38Records && period38Records.length > 0) {
      const result = await processAndSaveRecords(TARGET_DATE, 38, period38Records);
      totalVolumeChange += result.volume;
      totalPaymentChange += result.payment;
    } else {
      logger.error(`No records found for ${TARGET_DATE}, Period 38`);
    }
    
    // Update summaries if we added new data
    if (totalVolumeChange > 0) {
      logger.info(`\nTotal changes: Volume +${totalVolumeChange.toFixed(2)} MWh, Payment £${totalPaymentChange.toFixed(2)}`);
      
      // Update the daily summary
      await updateDailySummary(TARGET_DATE, totalVolumeChange, totalPaymentChange);
      
      // Update the monthly summary
      await updateMonthlySummary(TARGET_DATE, totalVolumeChange, totalPaymentChange);
      
      logger.info(`Successfully updated ${TARGET_DATE} with missing data`);
    } else {
      logger.info("No new data was added");
    }
    
    logger.info("===== FIX COMPLETE =====");
    
  } catch (error) {
    logger.error("ERROR DURING FIX:", error);
    process.exit(1);
  }
}

// Execute the fix
fixData();
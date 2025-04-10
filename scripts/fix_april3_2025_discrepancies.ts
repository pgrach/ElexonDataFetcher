/**
 * Fix script for 2025-04-03 data discrepancies
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
import * as fs from 'fs';
import * as path from 'path';

// Target date for verification
const TARGET_DATE = "2025-04-03";
const LOG_FILE = path.join('logs', `fix_april3_2025_${new Date().toISOString().replace(/:/g, '-')}.log`);

// Create log directory if it doesn't exist
if (!fs.existsSync('logs')) {
  fs.mkdirSync('logs', { recursive: true });
}

// Initialize log file
fs.writeFileSync(LOG_FILE, `=== FIX SCRIPT STARTED AT ${new Date().toISOString()} ===\n`);

// Simple logging function
function log(message: string) {
  const logMessage = `[${new Date().toISOString()}] ${message}`;
  console.log(logMessage);
  fs.appendFileSync(LOG_FILE, logMessage + '\n');
}

// Function to process and save records from Elexon API
async function processAndSaveRecords(date: string, period: number, records: any[]) {
  // Filter to only keep wind farm records with negative volume (curtailment)
  const windRecords = records.filter(r => 
    r.leadPartyName && 
    Number(r.volume) < 0 &&
    r.id.startsWith('T_') // Wind farm IDs typically start with T_
  );
  
  // Log what we're processing
  log(`[${date} P${period}] Processing ${windRecords.length} wind farm records`);
  
  // Track what we've added or updated
  let addedRecords = 0;
  let totalVolumeAdded = 0;
  let totalPaymentAdded = 0;
  
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
        log(`[${date} P${period}] Record for ${record.id} already exists, skipping`);
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
      
      log(`[${date} P${period}] Added record for ${record.id}: ${absVolume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
      
      // Update our totals
      addedRecords++;
      totalVolumeAdded += absVolume;
      totalPaymentAdded += payment;
    } catch (error) {
      log(`[${date} P${period}] Error saving record for ${record.id}: ${error}`);
    }
  }
  
  // Log the period summary
  log(`[${date} P${period}] Summary: Added ${addedRecords} records, ${totalVolumeAdded.toFixed(2)} MWh, £${totalPaymentAdded.toFixed(2)}`);
  
  return {
    count: addedRecords,
    volume: totalVolumeAdded,
    payment: totalPaymentAdded
  };
}

// Update daily summary with the corrected values
async function updateDailySummary(date: string, volumeChange: number, paymentChange: number) {
  try {
    log(`Updating daily summary for ${date}`);
    
    // Get existing summary
    const existingSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date))
      .limit(1);
    
    if (existingSummary.length === 0) {
      log(`No daily summary found for ${date}`);
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
    
    log(`Updated daily summary for ${date}`);
    
    // Get the updated values for verification
    const updatedSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date))
      .limit(1);
    
    if (updatedSummary.length > 0) {
      log(`New daily summary values: ${updatedSummary[0].totalCurtailedEnergy.toFixed(2)} MWh, £${updatedSummary[0].totalPayment.toFixed(2)}`);
    }
    
  } catch (error) {
    log(`Error updating daily summary for ${date}: ${error}`);
  }
}

// Update monthly summary with the corrected values
async function updateMonthlySummary(date: string, volumeChange: number, paymentChange: number) {
  try {
    const yearMonth = date.substring(0, 7); // Format YYYY-MM
    log(`Updating monthly summary for ${yearMonth}`);
    
    // Get existing summary
    const existingSummary = await db
      .select()
      .from(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, yearMonth))
      .limit(1);
    
    if (existingSummary.length === 0) {
      log(`No monthly summary found for ${yearMonth}`);
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
    
    log(`Updated monthly summary for ${yearMonth}`);
    
    // Get the updated values for verification
    const updatedSummary = await db
      .select()
      .from(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, yearMonth))
      .limit(1);
    
    if (updatedSummary.length > 0) {
      log(`New monthly summary values: ${updatedSummary[0].totalCurtailedEnergy.toFixed(2)} MWh, £${updatedSummary[0].totalPayment.toFixed(2)}`);
    }
    
  } catch (error) {
    log(`Error updating monthly summary for ${date}: ${error}`);
  }
}

// Main function to fix the data
async function fixData() {
  try {
    log("===== FIXING 2025-04-03 DATA =====");
    
    let totalVolumeChange = 0;
    let totalPaymentChange = 0;
    
    // Fix periods with identified discrepancies
    const periodsToFix = [35, 36, 37, 38];
    
    for (const period of periodsToFix) {
      log(`\nFetching data for ${TARGET_DATE}, Period ${period}...`);
      const records = await fetchBidsOffers(TARGET_DATE, period);
      
      if (records && records.length > 0) {
        log(`Found ${records.length} records from API for period ${period}`);
        const result = await processAndSaveRecords(TARGET_DATE, period, records);
        
        totalVolumeChange += result.volume;
        totalPaymentChange += result.payment;
      } else {
        log(`No records found for ${TARGET_DATE}, Period ${period}`);
      }
    }
    
    // Update summaries if we added new data
    if (totalVolumeChange > 0) {
      log(`\nTotal changes: Volume +${totalVolumeChange.toFixed(2)} MWh, Payment £${totalPaymentChange.toFixed(2)}`);
      
      // Update the daily summary
      await updateDailySummary(TARGET_DATE, totalVolumeChange, totalPaymentChange);
      
      // Update the monthly summary
      await updateMonthlySummary(TARGET_DATE, totalVolumeChange, totalPaymentChange);
      
      log(`Successfully updated ${TARGET_DATE} with missing data`);
    } else {
      log("No new data was added");
    }
    
    // Run a final verification query to check our work
    const verificationSummary = await db
      .select({
        recordCount: sql<number>`COUNT(*)::int`,
        totalVolume: sql<string>`SUM(ABS(volume))::text`,
        totalPayment: sql<string>`SUM(payment)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (verificationSummary.length > 0) {
      log(`\nFinal verification: ${TARGET_DATE} now has ${verificationSummary[0].recordCount} records`);
      log(`Total volume: ${Number(verificationSummary[0].totalVolume).toFixed(2)} MWh`);
      log(`Total payment: £${Number(verificationSummary[0].totalPayment).toFixed(2)}`);
    }
    
    log("===== FIX COMPLETE =====");
    
  } catch (error) {
    log(`ERROR DURING FIX: ${error}`);
    process.exit(1);
  }
}

// Execute the fix
fixData().catch(err => {
  log(`Fatal error: ${err}`);
  process.exit(1);
});
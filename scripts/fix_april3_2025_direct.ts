/**
 * Direct Fix Script for 2025-04-03 Data
 * 
 * This script directly adds the missing records from the Elexon API inspection
 * to ensure our database matches the API data exactly.
 */

import { db } from "../db";
import { curtailmentRecords, dailySummaries, monthlySummaries } from "../db/schema";
import { eq, and, sql } from "drizzle-orm";
import * as fs from 'fs';
import * as path from 'path';

// Target date for fix
const TARGET_DATE = "2025-04-03";
const LOG_FILE = path.join('logs', `fix_april3_2025_direct_${new Date().toISOString().replace(/:/g, '-')}.log`);

// Create log directory if it doesn't exist
if (!fs.existsSync('logs')) {
  fs.mkdirSync('logs', { recursive: true });
}

// Initialize log file
fs.writeFileSync(LOG_FILE, `=== DIRECT FIX SCRIPT STARTED AT ${new Date().toISOString()} ===\n`);

// Simple logging function
function log(message: string) {
  const logMessage = `[${new Date().toISOString()}] ${message}`;
  console.log(logMessage);
  fs.appendFileSync(LOG_FILE, logMessage + '\n');
}

// Missing records from API inspection - Period 35
const missingPeriod35 = [
  {
    id: 'T_NNGAO-2',
    farmId: 'T_NNGAO-2',
    leadPartyName: 'NNG Wind Farm No. 2 Limited', 
    volume: -6.379166666666666,
    originalPrice: -13.87,
    finalPrice: -13.87,
    payment: 88.48
  }
];

// Missing records from API inspection - Period 36
const missingPeriod36 = [
  {
    id: 'T_NNGAO-2',
    farmId: 'T_NNGAO-2',
    leadPartyName: 'NNG Wind Farm No. 2 Limited',
    volume: -36.42916666666667,
    originalPrice: -13.87,
    finalPrice: -13.87,
    payment: 505.27
  }
];

// Missing records from API inspection - Period 37
const missingPeriod37 = [
  {
    id: 'T_NNGAO-2',
    farmId: 'T_NNGAO-2',
    leadPartyName: 'NNG Wind Farm No. 2 Limited',
    volume: -21.025,
    originalPrice: -13.87,
    finalPrice: -13.87,
    payment: 291.62
  }
];

// Missing records from API inspection - Period 38
const missingPeriod38 = [
  {
    id: 'T_NNGAO-2',
    farmId: 'T_NNGAO-2', 
    leadPartyName: 'NNG Wind Farm No. 2 Limited',
    volume: -2.516666666666667,
    originalPrice: -13.87,
    finalPrice: -13.87,
    payment: 34.91
  }
];

// Main function to add the missing records
async function addMissingRecords() {
  try {
    log("===== ADDING MISSING RECORDS FOR 2025-04-03 =====");
    
    // Track what we've added
    let totalRecordsAdded = 0;
    let totalVolumeAdded = 0;
    let totalPaymentAdded = 0;
    
    // Process Period 35
    log("\nProcessing Period 35 missing records...");
    for (const record of missingPeriod35) {
      // Check if record already exists
      const exists = await db
        .select({ id: curtailmentRecords.id })
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, TARGET_DATE),
            eq(curtailmentRecords.settlementPeriod, 35),
            eq(curtailmentRecords.farmId, record.farmId)
          )
        )
        .limit(1);
      
      if (exists.length > 0) {
        log(`Record for Period 35, Farm ${record.farmId} already exists, skipping`);
        continue;
      }
      
      // Add the record
      await db.insert(curtailmentRecords).values({
        settlementDate: TARGET_DATE,
        settlementPeriod: 35,
        farmId: record.farmId,
        leadPartyName: record.leadPartyName,
        volume: record.volume,
        originalPrice: record.originalPrice,
        finalPrice: record.finalPrice,
        payment: record.payment,
        createdAt: new Date()
      });
      
      log(`Added record for Period 35, Farm ${record.farmId}: ${Math.abs(record.volume).toFixed(2)} MWh, £${record.payment.toFixed(2)}`);
      
      totalRecordsAdded++;
      totalVolumeAdded += Math.abs(record.volume);
      totalPaymentAdded += record.payment;
    }
    
    // Process Period 36
    log("\nProcessing Period 36 missing records...");
    for (const record of missingPeriod36) {
      // Check if record already exists
      const exists = await db
        .select({ id: curtailmentRecords.id })
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, TARGET_DATE),
            eq(curtailmentRecords.settlementPeriod, 36),
            eq(curtailmentRecords.farmId, record.farmId)
          )
        )
        .limit(1);
      
      if (exists.length > 0) {
        log(`Record for Period 36, Farm ${record.farmId} already exists, skipping`);
        continue;
      }
      
      // Add the record
      await db.insert(curtailmentRecords).values({
        settlementDate: TARGET_DATE,
        settlementPeriod: 36,
        farmId: record.farmId,
        leadPartyName: record.leadPartyName,
        volume: record.volume,
        price: record.price,
        payment: record.payment,
        created_at: new Date(),
        updated_at: new Date()
      });
      
      log(`Added record for Period 36, Farm ${record.farmId}: ${Math.abs(record.volume).toFixed(2)} MWh, £${record.payment.toFixed(2)}`);
      
      totalRecordsAdded++;
      totalVolumeAdded += Math.abs(record.volume);
      totalPaymentAdded += record.payment;
    }
    
    // Process Period 37
    log("\nProcessing Period 37 missing records...");
    for (const record of missingPeriod37) {
      // Check if record already exists
      const exists = await db
        .select({ id: curtailmentRecords.id })
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, TARGET_DATE),
            eq(curtailmentRecords.settlementPeriod, 37),
            eq(curtailmentRecords.farmId, record.farmId)
          )
        )
        .limit(1);
      
      if (exists.length > 0) {
        log(`Record for Period 37, Farm ${record.farmId} already exists, skipping`);
        continue;
      }
      
      // Add the record
      await db.insert(curtailmentRecords).values({
        settlementDate: TARGET_DATE,
        settlementPeriod: 37,
        farmId: record.farmId,
        leadPartyName: record.leadPartyName,
        volume: record.volume,
        price: record.price,
        payment: record.payment,
        created_at: new Date(),
        updated_at: new Date()
      });
      
      log(`Added record for Period 37, Farm ${record.farmId}: ${Math.abs(record.volume).toFixed(2)} MWh, £${record.payment.toFixed(2)}`);
      
      totalRecordsAdded++;
      totalVolumeAdded += Math.abs(record.volume);
      totalPaymentAdded += record.payment;
    }
    
    // Process Period 38
    log("\nProcessing Period 38 missing records...");
    for (const record of missingPeriod38) {
      // Check if record already exists
      const exists = await db
        .select({ id: curtailmentRecords.id })
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, TARGET_DATE),
            eq(curtailmentRecords.settlementPeriod, 38),
            eq(curtailmentRecords.farmId, record.farmId)
          )
        )
        .limit(1);
      
      if (exists.length > 0) {
        log(`Record for Period 38, Farm ${record.farmId} already exists, skipping`);
        continue;
      }
      
      // Add the record
      await db.insert(curtailmentRecords).values({
        settlementDate: TARGET_DATE,
        settlementPeriod: 38,
        farmId: record.farmId,
        leadPartyName: record.leadPartyName,
        volume: record.volume,
        price: record.price,
        payment: record.payment,
        created_at: new Date(),
        updated_at: new Date()
      });
      
      log(`Added record for Period 38, Farm ${record.farmId}: ${Math.abs(record.volume).toFixed(2)} MWh, £${record.payment.toFixed(2)}`);
      
      totalRecordsAdded++;
      totalVolumeAdded += Math.abs(record.volume);
      totalPaymentAdded += record.payment;
    }
    
    // Update summaries if we added records
    if (totalRecordsAdded > 0) {
      log(`\nTotal records added: ${totalRecordsAdded}`);
      log(`Total volume added: ${totalVolumeAdded.toFixed(2)} MWh`);
      log(`Total payment added: £${totalPaymentAdded.toFixed(2)}`);
      
      // Update daily summary
      log("\nUpdating daily summary...");
      const dailySummary = await db
        .select()
        .from(dailySummaries)
        .where(eq(dailySummaries.summaryDate, TARGET_DATE))
        .limit(1);
      
      if (dailySummary.length > 0) {
        const currentSummary = dailySummary[0];
        
        await db
          .update(dailySummaries)
          .set({
            totalCurtailedEnergy: Number(currentSummary.totalCurtailedEnergy) + totalVolumeAdded,
            totalPayment: Number(currentSummary.totalPayment) + totalPaymentAdded,
            lastUpdated: new Date()
          })
          .where(eq(dailySummaries.summaryDate, TARGET_DATE));
        
        log(`Updated daily summary for ${TARGET_DATE}`);
        
        // Verify the update
        const updatedDailySummary = await db
          .select()
          .from(dailySummaries)
          .where(eq(dailySummaries.summaryDate, TARGET_DATE))
          .limit(1);
        
        if (updatedDailySummary.length > 0) {
          log(`New daily summary values: ${updatedDailySummary[0].totalCurtailedEnergy.toFixed(2)} MWh, £${updatedDailySummary[0].totalPayment.toFixed(2)}`);
        }
      } else {
        log(`No daily summary found for ${TARGET_DATE}`);
      }
      
      // Update monthly summary
      log("\nUpdating monthly summary...");
      const yearMonth = TARGET_DATE.substring(0, 7); // Format YYYY-MM
      
      const monthlySummary = await db
        .select()
        .from(monthlySummaries)
        .where(eq(monthlySummaries.yearMonth, yearMonth))
        .limit(1);
      
      if (monthlySummary.length > 0) {
        const currentSummary = monthlySummary[0];
        
        await db
          .update(monthlySummaries)
          .set({
            totalCurtailedEnergy: Number(currentSummary.totalCurtailedEnergy) + totalVolumeAdded,
            totalPayment: Number(currentSummary.totalPayment) + totalPaymentAdded,
            updatedAt: new Date(),
            lastUpdated: new Date()
          })
          .where(eq(monthlySummaries.yearMonth, yearMonth));
        
        log(`Updated monthly summary for ${yearMonth}`);
        
        // Verify the update
        const updatedMonthlySummary = await db
          .select()
          .from(monthlySummaries)
          .where(eq(monthlySummaries.yearMonth, yearMonth))
          .limit(1);
        
        if (updatedMonthlySummary.length > 0) {
          log(`New monthly summary values: ${updatedMonthlySummary[0].totalCurtailedEnergy.toFixed(2)} MWh, £${updatedMonthlySummary[0].totalPayment.toFixed(2)}`);
        }
      } else {
        log(`No monthly summary found for ${yearMonth}`);
      }
    } else {
      log("No new records were added");
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
addMissingRecords().catch(err => {
  log(`Fatal error: ${err}`);
  process.exit(1);
});
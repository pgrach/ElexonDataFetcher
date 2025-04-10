/**
 * Direct Addition Script for Missing Record in 2025-04-03 Data
 * 
 * This script adds the missing wind farm record from T_NNGAO-2 that was identified
 * during verification but not present in the database.
 */

import { db } from "../db";
import { curtailmentRecords, dailySummaries, monthlySummaries } from "../db/schema";
import { eq } from "drizzle-orm";

// Target date and farm
const TARGET_DATE = "2025-04-03";
const FARM_ID = "T_NNGAO-2";
const LEAD_PARTY = "NNG Wind Farm No. 2 Limited";

// Main function to add the missing records
async function addMissingRecord() {
  try {
    console.log("===== ADDING MISSING RECORD FOR 2025-04-03 =====");
    
    // Missing records from API inspection
    const missingRecords = [
      {
        period: 35,
        volume: -6.379166666666666,
        originalPrice: -13.87,
        finalPrice: -13.87,
        payment: 88.48
      },
      {
        period: 36,
        volume: -36.42916666666667,
        originalPrice: -13.87,
        finalPrice: -13.87,
        payment: 505.27
      },
      {
        period: 37,
        volume: -21.025,
        originalPrice: -13.87,
        finalPrice: -13.87,
        payment: 291.62
      },
      {
        period: 38,
        volume: -2.516666666666667,
        originalPrice: -13.87,
        finalPrice: -13.87,
        payment: 34.91
      }
    ];
    
    let totalAdded = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Add each missing record
    for (const record of missingRecords) {
      // Check if this record already exists
      const existingRecord = await db
        .select()
        .from(curtailmentRecords)
        .where(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, record.period),
          eq(curtailmentRecords.farmId, FARM_ID)
        )
        .limit(1);
      
      if (existingRecord.length > 0) {
        console.log(`Record for period ${record.period} already exists, skipping`);
        continue;
      }
      
      // Add the record
      await db.insert(curtailmentRecords).values({
        settlementDate: TARGET_DATE,
        settlementPeriod: record.period,
        farmId: FARM_ID,
        leadPartyName: LEAD_PARTY,
        volume: record.volume,
        originalPrice: record.originalPrice,
        finalPrice: record.finalPrice,
        payment: record.payment,
        createdAt: new Date()
      });
      
      console.log(`Added record for period ${record.period}: ${Math.abs(record.volume).toFixed(2)} MWh, £${record.payment.toFixed(2)}`);
      
      totalAdded++;
      totalVolume += Math.abs(record.volume);
      totalPayment += record.payment;
    }
    
    if (totalAdded > 0) {
      console.log(`\nAdded ${totalAdded} records with total volume ${totalVolume.toFixed(2)} MWh and payment £${totalPayment.toFixed(2)}`);
      
      // Update daily summary
      console.log("Updating daily summary...");
      const dailySummary = await db
        .select()
        .from(dailySummaries)
        .where(eq(dailySummaries.summaryDate, TARGET_DATE))
        .limit(1);
      
      if (dailySummary.length > 0) {
        await db
          .update(dailySummaries)
          .set({
            totalCurtailedEnergy: Number(dailySummary[0].totalCurtailedEnergy) + totalVolume,
            totalPayment: Number(dailySummary[0].totalPayment) + totalPayment,
            lastUpdated: new Date()
          })
          .where(eq(dailySummaries.summaryDate, TARGET_DATE));
        
        console.log("Updated daily summary");
      }
      
      // Update monthly summary
      console.log("Updating monthly summary...");
      const yearMonth = TARGET_DATE.substring(0, 7); // Format YYYY-MM
      
      const monthlySummary = await db
        .select()
        .from(monthlySummaries)
        .where(eq(monthlySummaries.yearMonth, yearMonth))
        .limit(1);
      
      if (monthlySummary.length > 0) {
        await db
          .update(monthlySummaries)
          .set({
            totalCurtailedEnergy: Number(monthlySummary[0].totalCurtailedEnergy) + totalVolume,
            totalPayment: Number(monthlySummary[0].totalPayment) + totalPayment,
            updatedAt: new Date(),
            lastUpdated: new Date()
          })
          .where(eq(monthlySummaries.yearMonth, yearMonth));
        
        console.log("Updated monthly summary");
      }
    } else {
      console.log("No new records were added");
    }
    
    console.log("===== ADDITION COMPLETE =====");
    
  } catch (error) {
    console.error("ERROR:", error);
    process.exit(1);
  }
}

// Execute the function
addMissingRecord().catch(error => {
  console.error("Fatal error:", error);
  process.exit(1);
});
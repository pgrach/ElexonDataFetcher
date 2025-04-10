/**
 * Fix script for adding missing T_NNGAO-2 records for 2025-04-03
 * 
 * This script specifically adds missing data for the NNG Wind Farm No. 2 Limited (T_NNGAO-2)
 * that wasn't captured in the original data processing.
 */

import { db } from "../db";
import { curtailmentRecords, dailySummaries, monthlySummaries } from "../db/schema";
import { eq } from "drizzle-orm";

// Constants
const TARGET_DATE = "2025-04-03";
const FARM_ID = "T_NNGAO-2";
const LEAD_PARTY_NAME = "NNG Wind Farm No. 2 Limited";

// Missing records from Elexon API for T_NNGAO-2
const missingRecords = [
  {
    settlementPeriod: 35,
    volume: -6.379166666666666,
    originalPrice: -13.87,
    finalPrice: -13.87,
    payment: 88.48
  },
  {
    settlementPeriod: 36,
    volume: -36.42916666666667,
    originalPrice: -13.87,
    finalPrice: -13.87,
    payment: 505.27
  },
  {
    settlementPeriod: 37,
    volume: -21.025,
    originalPrice: -13.87,
    finalPrice: -13.87,
    payment: 291.62
  },
  {
    settlementPeriod: 38,
    volume: -2.516666666666667,
    originalPrice: -13.87,
    finalPrice: -13.87,
    payment: 34.91
  }
];

async function fixData() {
  console.log("===== ADDING MISSING T_NNGAO-2 RECORDS FOR 2025-04-03 =====");
  
  let totalAdded = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  
  // Process each missing record
  for (const record of missingRecords) {
    // First check if record already exists
    const existingRecord = await db
      .select()
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .where(eq(curtailmentRecords.settlementPeriod, record.settlementPeriod))
      .where(eq(curtailmentRecords.farmId, FARM_ID));
    
    const recordExists = existingRecord.length > 0;
    
    if (recordExists) {
      console.log(`Record for period ${record.settlementPeriod} already exists, skipping`);
      continue;
    }
    
    try {
      // Insert the record
      await db.insert(curtailmentRecords).values({
        settlementDate: TARGET_DATE,
        settlementPeriod: record.settlementPeriod,
        farmId: FARM_ID,
        leadPartyName: LEAD_PARTY_NAME,
        volume: record.volume,
        originalPrice: record.originalPrice,
        finalPrice: record.finalPrice,
        payment: record.payment,
        createdAt: new Date()
      });
      
      console.log(`Added record for period ${record.settlementPeriod}: ${Math.abs(record.volume).toFixed(2)} MWh, £${record.payment.toFixed(2)}`);
      
      totalAdded++;
      totalVolume += Math.abs(record.volume);
      totalPayment += record.payment;
    } catch (error) {
      console.error(`Error adding record for period ${record.settlementPeriod}:`, error);
    }
  }
  
  // Update summary tables if records were added
  if (totalAdded > 0) {
    console.log(`\nAdded ${totalAdded} records with total volume ${totalVolume.toFixed(2)} MWh and payment £${totalPayment.toFixed(2)}`);
    
    try {
      // Update daily summary
      console.log("\nUpdating daily summary...");
      const dailySummary = await db
        .select()
        .from(dailySummaries)
        .where(eq(dailySummaries.summaryDate, TARGET_DATE));
      
      if (dailySummary.length > 0) {
        const summary = dailySummary[0];
        
        await db
          .update(dailySummaries)
          .set({
            totalCurtailedEnergy: Number(summary.totalCurtailedEnergy) + totalVolume,
            totalPayment: Number(summary.totalPayment) + totalPayment,
            lastUpdated: new Date()
          })
          .where(eq(dailySummaries.summaryDate, TARGET_DATE));
        
        console.log(`Updated daily summary for ${TARGET_DATE}`);
        
        // Get updated summary
        const updatedSummary = await db
          .select()
          .from(dailySummaries)
          .where(eq(dailySummaries.summaryDate, TARGET_DATE));
        
        if (updatedSummary.length > 0) {
          console.log(`New daily summary: ${Number(updatedSummary[0].totalCurtailedEnergy).toFixed(2)} MWh, £${Number(updatedSummary[0].totalPayment).toFixed(2)}`);
        }
      }
      
      // Update monthly summary
      console.log("\nUpdating monthly summary...");
      const yearMonth = TARGET_DATE.substring(0, 7); // Format: YYYY-MM
      
      const monthlySummary = await db
        .select()
        .from(monthlySummaries)
        .where(eq(monthlySummaries.yearMonth, yearMonth));
      
      if (monthlySummary.length > 0) {
        const summary = monthlySummary[0];
        
        await db
          .update(monthlySummaries)
          .set({
            totalCurtailedEnergy: Number(summary.totalCurtailedEnergy) + totalVolume,
            totalPayment: Number(summary.totalPayment) + totalPayment,
            updatedAt: new Date(),
            lastUpdated: new Date()
          })
          .where(eq(monthlySummaries.yearMonth, yearMonth));
        
        console.log(`Updated monthly summary for ${yearMonth}`);
        
        // Get updated summary
        const updatedSummary = await db
          .select()
          .from(monthlySummaries)
          .where(eq(monthlySummaries.yearMonth, yearMonth));
        
        if (updatedSummary.length > 0) {
          console.log(`New monthly summary: ${Number(updatedSummary[0].totalCurtailedEnergy).toFixed(2)} MWh, £${Number(updatedSummary[0].totalPayment).toFixed(2)}`);
        }
      }
    } catch (error) {
      console.error("Error updating summary tables:", error);
    }
  } else {
    console.log("No records were added");
  }
  
  // Final verification
  console.log("\nFinal verification for 2025-04-03:");
  
  try {
    const totalRecords = await db.select({count: db.fn.count()})
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const totalVolume = await db.select({
      sum: db.fn.sum(db.fn.abs(curtailmentRecords.volume))
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const totalPayment = await db.select({
      sum: db.fn.sum(curtailmentRecords.payment)
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Total records: ${totalRecords[0].count}`);
    console.log(`Total volume: ${Number(totalVolume[0].sum).toFixed(2)} MWh`);
    console.log(`Total payment: £${Number(totalPayment[0].sum).toFixed(2)}`);
  } catch (error) {
    console.error("Error during final verification:", error);
  }
  
  console.log("\n===== FIX COMPLETE =====");
}

// Run the fix script
fixData()
  .catch(error => {
    console.error("Fatal error:", error);
    process.exit(1);
  })
  .finally(() => {
    console.log("Script execution completed");
  });
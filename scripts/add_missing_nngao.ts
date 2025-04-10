/**
 * Simple Direct Fix Script for T_NNGAO-2 records for 2025-04-03
 * 
 * This script adds the missing data for NNG Wind Farm No. 2 Limited (T_NNGAO-2)
 */

import { db } from "../db";
import { curtailmentRecords, dailySummaries, monthlySummaries } from "../db/schema";
import { eq, and, sql } from "drizzle-orm";

// Add each record directly
async function addMissingRecords() {
  console.log("Adding missing T_NNGAO-2 records for 2025-04-03...");
  
  try {
    // Period 35
    await db.insert(curtailmentRecords).values({
      settlementDate: "2025-04-03",
      settlementPeriod: 35,
      farmId: "T_NNGAO-2",
      leadPartyName: "NNG Wind Farm No. 2 Limited",
      volume: -6.379166666666666,
      originalPrice: -13.87,
      finalPrice: -13.87,
      payment: 88.48,
      createdAt: new Date()
    });
    console.log("Added Period 35 record");
    
    // Period 36
    await db.insert(curtailmentRecords).values({
      settlementDate: "2025-04-03",
      settlementPeriod: 36,
      farmId: "T_NNGAO-2",
      leadPartyName: "NNG Wind Farm No. 2 Limited",
      volume: -36.42916666666667,
      originalPrice: -13.87,
      finalPrice: -13.87,
      payment: 505.27,
      createdAt: new Date()
    });
    console.log("Added Period 36 record");
    
    // Period 37
    await db.insert(curtailmentRecords).values({
      settlementDate: "2025-04-03",
      settlementPeriod: 37,
      farmId: "T_NNGAO-2",
      leadPartyName: "NNG Wind Farm No. 2 Limited",
      volume: -21.025,
      originalPrice: -13.87,
      finalPrice: -13.87,
      payment: 291.62,
      createdAt: new Date()
    });
    console.log("Added Period 37 record");
    
    // Period 38
    await db.insert(curtailmentRecords).values({
      settlementDate: "2025-04-03",
      settlementPeriod: 38,
      farmId: "T_NNGAO-2",
      leadPartyName: "NNG Wind Farm No. 2 Limited",
      volume: -2.516666666666667,
      originalPrice: -13.87,
      finalPrice: -13.87,
      payment: 34.91,
      createdAt: new Date()
    });
    console.log("Added Period 38 record");
    
    // Calculate totals for summary updates
    const totalVolume = 6.38 + 36.43 + 21.02 + 2.52; // 66.35 MWh
    const totalPayment = 88.48 + 505.27 + 291.62 + 34.91; // 920.28 GBP
    
    console.log(`Total volume added: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment added: £${totalPayment.toFixed(2)}`);
    
    // Update daily summary
    const dailySummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, "2025-04-03"));
    
    if (dailySummary.length > 0) {
      await db
        .update(dailySummaries)
        .set({
          totalCurtailedEnergy: sql`${dailySummaries.totalCurtailedEnergy} + ${totalVolume}`,
          totalPayment: sql`${dailySummaries.totalPayment} + ${totalPayment}`,
          lastUpdated: new Date()
        })
        .where(eq(dailySummaries.summaryDate, "2025-04-03"));
      console.log("Updated daily summary");
    }
    
    // Update monthly summary
    const monthlySummary = await db
      .select()
      .from(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, "2025-04"));
    
    if (monthlySummary.length > 0) {
      await db
        .update(monthlySummaries)
        .set({
          totalCurtailedEnergy: sql`${monthlySummaries.totalCurtailedEnergy} + ${totalVolume}`,
          totalPayment: sql`${monthlySummaries.totalPayment} + ${totalPayment}`,
          updatedAt: new Date(),
          lastUpdated: new Date()
        })
        .where(eq(monthlySummaries.yearMonth, "2025-04"));
      console.log("Updated monthly summary");
    }
    
    console.log("Fix complete!");
  } catch (error) {
    console.error("Error:", error);
  }
}

// Run the script
addMissingRecords();
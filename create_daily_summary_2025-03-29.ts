/**
 * Create Daily Summary for 2025-03-29
 * 
 * This script creates the missing daily summary for March 29, 2025 
 * based on the existing curtailment records.
 * 
 * Usage:
 *   npx tsx create_daily_summary_2025-03-29.ts
 */

import { db } from "@db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "@db/schema";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = "2025-03-29";

async function createDailySummary(): Promise<void> {
  try {
    console.log(`Creating daily summary for ${TARGET_DATE}`);
    
    // Calculate totals from curtailment records
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))::text`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)::text`,
        recordCount: sql<number>`COUNT(*)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const { totalCurtailedEnergy, totalPayment, recordCount } = totals[0];
    
    console.log(`Found ${recordCount} records with totals:`, {
      energy: totalCurtailedEnergy ? Number(totalCurtailedEnergy).toFixed(2) + " MWh" : "0 MWh",
      payment: totalPayment ? "£" + Number(totalPayment).toFixed(2) : "£0.00"
    });
    
    if (!totalCurtailedEnergy || !totalPayment || recordCount === 0) {
      console.error("No valid data to create summary");
      return;
    }
    
    // Check if summary already exists
    const existingSummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE)
    });
    
    if (existingSummary) {
      console.log(`Updating existing summary for ${TARGET_DATE}`);
      
      await db.update(dailySummaries)
        .set({
          totalCurtailedEnergy,
          totalPayment
        })
        .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    } else {
      console.log(`Creating new summary for ${TARGET_DATE}`);
      
      await db.insert(dailySummaries).values({
        summaryDate: TARGET_DATE,
        totalCurtailedEnergy,
        totalPayment
      });
    }
    
    console.log("Daily summary created/updated successfully");
    
    // Update monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7);
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)::text`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)::text`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${TARGET_DATE}::date)`);
    
    if (monthlyTotals[0].totalCurtailedEnergy && monthlyTotals[0].totalPayment) {
      console.log(`Updating monthly summary for ${yearMonth}`);
      
      await db.insert(monthlySummaries).values({
        yearMonth,
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
        totalPayment: monthlyTotals[0].totalPayment,
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [monthlySummaries.yearMonth],
        set: {
          totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
          totalPayment: monthlyTotals[0].totalPayment,
          updatedAt: new Date()
        }
      });
    }
    
    // Update yearly summary
    const year = TARGET_DATE.substring(0, 4);
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)::text`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)::text`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${TARGET_DATE}::date)`);
    
    if (yearlyTotals[0].totalCurtailedEnergy && yearlyTotals[0].totalPayment) {
      console.log(`Updating yearly summary for ${year}`);
      
      await db.insert(yearlySummaries).values({
        year,
        totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
        totalPayment: yearlyTotals[0].totalPayment,
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [yearlySummaries.year],
        set: {
          totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
          totalPayment: yearlyTotals[0].totalPayment,
          updatedAt: new Date()
        }
      });
    }
    
    console.log("All summaries updated successfully");
    
  } catch (error) {
    console.error("Error creating daily summary:", error);
    process.exit(1);
  }
}

createDailySummary();
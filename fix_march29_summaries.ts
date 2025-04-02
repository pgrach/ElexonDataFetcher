/**
 * Fix Summaries for March 29, 2025
 * 
 * This script corrects the daily, monthly, and yearly summaries for March 29, 2025
 * by recalculating them directly from the curtailment_records table.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-29';
const TARGET_MONTH = '2025-03';
const TARGET_YEAR = '2025';

async function fixSummaries() {
  console.log(`=== Starting Summary Correction for ${TARGET_DATE} ===`);
  
  try {
    // Step 1: Get the correct totals from curtailment_records
    console.log("Calculating correct totals from curtailment_records...");
    const correctedTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(volume)::numeric)`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));

    if (!correctedTotals[0]) {
      throw new Error(`No curtailment records found for ${TARGET_DATE}`);
    }

    const totalCurtailedEnergy = correctedTotals[0].totalCurtailedEnergy;
    const totalPayment = correctedTotals[0].totalPayment;

    // Get current values for comparison
    const currentSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));

    console.log("Current Daily Summary:");
    console.log(`- Energy: ${currentSummary[0]?.totalCurtailedEnergy || 'N/A'} MWh`);
    console.log(`- Payment: £${currentSummary[0]?.totalPayment || 'N/A'}`);
    
    console.log("Corrected Values:");
    console.log(`- Energy: ${totalCurtailedEnergy} MWh`);
    console.log(`- Payment: £${totalPayment}`);

    // Step 2: Update the daily summary
    console.log(`\nUpdating daily summary for ${TARGET_DATE}...`);
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy,
      totalPayment,
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy,
        totalPayment,
        lastUpdated: new Date()
      }
    });
    console.log("Daily summary updated successfully");

    // Step 3: Recalculate and update monthly summary for March 2025
    console.log(`\nRecalculating monthly summary for ${TARGET_MONTH}...`);
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${TARGET_MONTH + '-01'}::date)`);

    if (monthlyTotals[0].totalCurtailedEnergy && monthlyTotals[0].totalPayment) {
      await db.insert(monthlySummaries).values({
        yearMonth: TARGET_MONTH,
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
      console.log("Monthly summary updated successfully");
      console.log(`- New Monthly Energy: ${monthlyTotals[0].totalCurtailedEnergy} MWh`);
      console.log(`- New Monthly Payment: £${monthlyTotals[0].totalPayment}`);
    }

    // Step 4: Recalculate and update yearly summary for 2025
    console.log(`\nRecalculating yearly summary for ${TARGET_YEAR}...`);
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${TARGET_YEAR + '-01-01'}::date)`);

    if (yearlyTotals[0].totalCurtailedEnergy && yearlyTotals[0].totalPayment) {
      await db.insert(yearlySummaries).values({
        year: TARGET_YEAR,
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
      console.log("Yearly summary updated successfully");
      console.log(`- New Yearly Energy: ${yearlyTotals[0].totalCurtailedEnergy} MWh`);
      console.log(`- New Yearly Payment: £${yearlyTotals[0].totalPayment}`);
    }

    // Step 5: Verify the changes
    const updatedSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));

    console.log("\nVerification:");
    console.log(`- Updated Daily Energy: ${updatedSummary[0]?.totalCurtailedEnergy || 'N/A'} MWh`);
    console.log(`- Updated Daily Payment: £${updatedSummary[0]?.totalPayment || 'N/A'}`);

    console.log("\n=== Summary Correction Completed ===");
  } catch (error) {
    console.error("Error fixing summaries:", error);
    throw error;
  }
}

// Run the function
fixSummaries()
  .then(() => {
    console.log("Script completed successfully!");
    process.exit(0);
  })
  .catch((error) => {
    console.error("Script failed:", error);
    process.exit(1);
  });
/**
 * Update Summary Tables for 2025-03-24
 * 
 * This script updates the daily, monthly, and yearly summary tables
 * after curtailment records have been updated.
 */

import { db } from "../../db/index.js";
import { dailySummaries, monthlySummaries, yearlySummaries, curtailmentRecords } from "../../db/schema.js";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-24';
const YEAR_MONTH = '2025-03';
const YEAR = '2025';

async function updateSummaries() {
  try {
    console.log('\n============================================');
    console.log('UPDATING SUMMARY TABLES FOR 2025-03-24');
    console.log('============================================\n');
    
    const startTime = Date.now();
    
    // Step 1: Update daily summary
    console.log(`\n=== Updating Daily Summary for ${TARGET_DATE} ===`);
    
    // Calculate totals from curtailment records
    const totals = await db
      .select({
        totalCurtailedEnergy: sql`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (!totals[0] || !totals[0].totalCurtailedEnergy) {
      console.log('No curtailment records found for this date, setting summary to zero values');
      totals[0] = {
        totalCurtailedEnergy: '0',
        totalPayment: '0'
      };
    }
    
    // Update daily summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totals[0].totalCurtailedEnergy?.toString() || '0',
      totalPayment: totals[0].totalPayment?.toString() || '0'
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totals[0].totalCurtailedEnergy?.toString() || '0',
        totalPayment: totals[0].totalPayment?.toString() || '0'
      }
    });
    
    console.log('Daily summary updated:', {
      energy: `${Number(totals[0].totalCurtailedEnergy || 0).toFixed(2)} MWh`,
      payment: `£${Number(totals[0].totalPayment || 0).toFixed(2)}`
    });
    
    // Step A: Update monthly summary
    console.log(`\n=== Updating Monthly Summary for ${YEAR_MONTH} ===`);
    
    // Calculate monthly totals from daily summaries
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${TARGET_DATE}::date)`);
    
    if (!monthlyTotals[0] || !monthlyTotals[0].totalCurtailedEnergy) {
      console.log('No daily summaries found for this month, setting monthly summary to zero values');
      monthlyTotals[0] = {
        totalCurtailedEnergy: '0',
        totalPayment: '0'
      };
    }
    
    // Update monthly summary
    await db.insert(monthlySummaries).values({
      yearMonth: YEAR_MONTH,
      totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy?.toString() || '0',
      totalPayment: monthlyTotals[0].totalPayment?.toString() || '0',
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [monthlySummaries.yearMonth],
      set: {
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy?.toString() || '0',
        totalPayment: monthlyTotals[0].totalPayment?.toString() || '0',
        updatedAt: new Date()
      }
    });
    
    console.log('Monthly summary updated:', {
      month: YEAR_MONTH,
      energy: `${Number(monthlyTotals[0].totalCurtailedEnergy || 0).toFixed(2)} MWh`,
      payment: `£${Number(monthlyTotals[0].totalPayment || 0).toFixed(2)}`
    });
    
    // Step 3: Update yearly summary
    console.log(`\n=== Updating Yearly Summary for ${YEAR} ===`);
    
    // Calculate yearly totals from daily summaries
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${TARGET_DATE}::date)`);
    
    if (!yearlyTotals[0] || !yearlyTotals[0].totalCurtailedEnergy) {
      console.log('No daily summaries found for this year, setting yearly summary to zero values');
      yearlyTotals[0] = {
        totalCurtailedEnergy: '0',
        totalPayment: '0'
      };
    }
    
    // Update yearly summary
    await db.insert(yearlySummaries).values({
      year: YEAR,
      totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy?.toString() || '0',
      totalPayment: yearlyTotals[0].totalPayment?.toString() || '0',
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [yearlySummaries.year],
      set: {
        totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy?.toString() || '0',
        totalPayment: yearlyTotals[0].totalPayment?.toString() || '0',
        updatedAt: new Date()
      }
    });
    
    console.log('Yearly summary updated:', {
      year: YEAR,
      energy: `${Number(yearlyTotals[0].totalCurtailedEnergy || 0).toFixed(2)} MWh`,
      payment: `£${Number(yearlyTotals[0].totalPayment || 0).toFixed(2)}`
    });
    
    const endTime = Date.now();
    console.log('\n============================================');
    console.log('SUMMARY TABLES UPDATE COMPLETED');
    console.log(`Duration: ${((endTime - startTime) / 1000).toFixed(2)} seconds`);
    console.log('============================================\n');
  } catch (error) {
    console.error('Error updating summary tables:', error);
    process.exit(1);
  }
}

updateSummaries().then(() => {
  console.log('Successfully updated all summary tables.');
  process.exit(0);
}).catch(error => {
  console.error('Failed to update summary tables:', error);
  process.exit(1);
});
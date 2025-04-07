/**
 * Update Summaries for March 25, 2025
 * 
 * This script will update the monthly and yearly summaries
 * based on the already processed curtailment data.
 */

import { db } from './db';
import { dailySummaries, monthlySummaries, yearlySummaries } from './db/schema';
import { eq, sql } from 'drizzle-orm';

async function updateSummaries(date: string): Promise<void> {
  try {
    console.log(`\n=== Updating Summaries for ${date} ===\n`);
    
    // Get the daily summary to confirm it exists
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });
    
    if (!summary) {
      console.error(`No daily summary found for ${date}`);
      return;
    }
    
    console.log(`Daily summary for ${date}:`, {
      energy: `${Number(summary.totalCurtailedEnergy).toFixed(2)} MWh`,
      payment: `£${Number(summary.totalPayment).toFixed(2)}`
    });
    
    // Update monthly summary
    const yearMonth = date.substring(0, 7);
    console.log(`\nUpdating monthly summary for ${yearMonth}...`);
    
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${date}::date)`);
    
    if (monthlyTotals[0].totalCurtailedEnergy && monthlyTotals[0].totalPayment) {
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
      
      console.log(`Monthly summary updated for ${yearMonth}:`, {
        energy: `${Number(monthlyTotals[0].totalCurtailedEnergy).toFixed(2)} MWh`,
        payment: `£${Number(monthlyTotals[0].totalPayment).toFixed(2)}`
      });
    }
    
    // Update yearly summary
    const year = date.substring(0, 4);
    console.log(`\nUpdating yearly summary for ${year}...`);
    
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${monthlySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${monthlySummaries.totalPayment}::numeric)`
      })
      .from(monthlySummaries)
      .where(sql`${monthlySummaries.yearMonth} LIKE ${year + '-%'}`);
    
    if (yearlyTotals[0].totalCurtailedEnergy && yearlyTotals[0].totalPayment) {
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
      
      console.log(`Yearly summary updated for ${year}:`, {
        energy: `${Number(yearlyTotals[0].totalCurtailedEnergy).toFixed(2)} MWh`,
        payment: `£${Number(yearlyTotals[0].totalPayment).toFixed(2)}`
      });
    }
    
    console.log(`\n=== Summary Updates Complete for ${date} ===\n`);
  } catch (error) {
    console.error('Error updating summaries:', error);
    throw error;
  }
}

async function main() {
  try {
    const date = '2025-03-25';
    await updateSummaries(date);
  } catch (error) {
    console.error('Error in update_summaries_for_march_25:', error);
    process.exit(1);
  }
}

main();
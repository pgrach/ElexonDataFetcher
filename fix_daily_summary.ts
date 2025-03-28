/**
 * Fix Daily Summary Script
 * 
 * This script updates the daily_summaries table for 2025-03-27 with the correct
 * totals from the curtailment_records table.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import { dailySummaries, curtailmentRecords, monthlySummaries, yearlySummaries } from './db/schema';
import { eq } from 'drizzle-orm';

async function fixDailySummary(date: string): Promise<void> {
  try {
    console.log(`Fixing daily summary for ${date}...`);
    
    // Get totals from curtailment_records
    const totals = await db
      .select({
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    if (!totals[0] || !totals[0].totalVolume) {
      console.error(`No curtailment records found for ${date}`);
      return;
    }
    
    const totalVolume = Number(totals[0].totalVolume);
    const totalPayment = Number(totals[0].totalPayment);
    
    console.log(`Found totals from curtailment_records:`, {
      totalVolume: `${totalVolume.toFixed(2)} MWh`,
      totalPayment: `£${totalPayment.toFixed(2)}`
    });
    
    // Get current data from daily_summaries
    const currentSummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });
    
    if (currentSummary) {
      console.log(`Current daily_summaries record:`, {
        totalCurtailedEnergy: `${Number(currentSummary.totalCurtailedEnergy).toFixed(2)} MWh`,
        totalPayment: `£${Number(currentSummary.totalPayment).toFixed(2)}`
      });
    } else {
      console.log(`No existing daily_summaries record for ${date}`);
    }
    
    // Update daily_summaries
    await db.insert(dailySummaries).values({
      summaryDate: date,
      totalCurtailedEnergy: totalVolume.toString(),
      totalPayment: totalPayment.toString(),
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: totalPayment.toString(),
        lastUpdated: new Date()
      }
    });
    
    console.log(`Updated daily_summaries for ${date}`);
    
    // Update monthly summary
    const yearMonth = date.substring(0, 7);
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
      
      console.log(`Updated monthly_summaries for ${yearMonth}`);
    }
    
    // Update yearly summary
    const year = date.substring(0, 4);
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${date}::date)`);
    
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
      
      console.log(`Updated yearly_summaries for ${year}`);
    }
    
    // Verify the update
    const updatedSummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, date)
    });
    
    console.log(`Verified daily_summaries after update:`, {
      totalCurtailedEnergy: `${Number(updatedSummary?.totalCurtailedEnergy || 0).toFixed(2)} MWh`,
      totalPayment: `£${Number(updatedSummary?.totalPayment || 0).toFixed(2)}`
    });
    
    console.log(`Successfully fixed daily summary for ${date}`);
  } catch (error) {
    console.error(`Error fixing daily summary for ${date}:`, error);
  }
}

const dateToFix = '2025-03-27';
fixDailySummary(dateToFix)
  .then(() => {
    console.log('Done');
    process.exit(0);
  })
  .catch(error => {
    console.error('Unhandled error:', error);
    process.exit(1);
  });
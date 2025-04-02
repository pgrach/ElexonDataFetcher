/**
 * Update March 28, 2025 Totals
 * 
 * This script manually sets the correct total values for March 28, 2025
 * as specified by the requirements.
 */

import { db } from "./db";
import { dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-28';

// The correct values we need to set
const CORRECT_ENERGY = '99904.22';  // MWh
const CORRECT_PAYMENT = '-3784089.62';  // £ (negative as it's a payment)

async function updateDailySummary(): Promise<void> {
  console.log(`Updating daily summary for ${TARGET_DATE} with corrected values:`);
  console.log(`- Energy: ${CORRECT_ENERGY} MWh`);
  console.log(`- Payment: £${CORRECT_PAYMENT}`);
  
  try {
    // Update daily summary with the correct values
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: CORRECT_ENERGY,
      totalPayment: CORRECT_PAYMENT,
      totalWindGeneration: '0',
      windOnshoreGeneration: '0',
      windOffshoreGeneration: '0',
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: CORRECT_ENERGY,
        totalPayment: CORRECT_PAYMENT,
        lastUpdated: new Date()
      }
    });
    
    console.log('Daily summary updated successfully');
    
    // Now update the monthly and yearly summaries
    await updateAggregatedSummaries();
    
    // Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    console.log('All summaries have been updated successfully');
  } catch (error) {
    console.error('Error updating daily summary:', error);
    throw error;
  }
}

async function updateAggregatedSummaries(): Promise<void> {
  try {
    // Update monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7);
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${yearMonth + '-01'}::date)`);
    
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
      
      console.log(`Monthly summary updated for ${yearMonth}:`);
      console.log(`- Energy: ${monthlyTotals[0].totalCurtailedEnergy} MWh`);
      console.log(`- Payment: £${monthlyTotals[0].totalPayment}`);
    }
    
    // Update yearly summary
    const year = TARGET_DATE.substring(0, 4);
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${year + '-01-01'}::date)`);
    
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
      
      console.log(`Yearly summary updated for ${year}:`);
      console.log(`- Energy: ${yearlyTotals[0].totalCurtailedEnergy} MWh`);
      console.log(`- Payment: £${yearlyTotals[0].totalPayment}`);
    }
  } catch (error) {
    console.error('Error updating aggregated summaries:', error);
    throw error;
  }
}

async function updateBitcoinCalculations(): Promise<void> {
  console.log(`Updating Bitcoin calculations for ${TARGET_DATE}...`);
  
  try {
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    for (const minerModel of minerModels) {
      await processSingleDay(TARGET_DATE, minerModel);
      console.log(`- Processed ${minerModel}`);
    }
    
    console.log('Bitcoin calculations updated successfully');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    throw error;
  }
}

async function main(): Promise<void> {
  try {
    console.log('=== Updating March 28, 2025 with correct totals ===');
    await updateDailySummary();
    console.log('=== Update completed successfully ===');
  } catch (error) {
    console.error('Error during update process:', error);
    process.exit(1);
  }
}

main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
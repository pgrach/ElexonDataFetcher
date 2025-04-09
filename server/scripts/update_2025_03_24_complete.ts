/**
 * Complete Update Script for 2025-03-24
 * 
 * This script performs a full data update for 2025-03-24 including:
 * 1. Reingesting curtailment records from Elexon API
 * 2. Updating daily summary
 * 3. Updating monthly summary (March 2025)
 * 4. Updating yearly summary (2025)
 * 5. Updating Bitcoin calculation tables
 */

import { db } from "@db";
import { dailySummaries, monthlySummaries, yearlySummaries, curtailmentRecords } from "@db/schema";
import { eq, sql } from "drizzle-orm";
import { format } from "date-fns";
import { processSingleDay } from "../services/bitcoinService";

// Import the reingestion function from our other script
import { reingestCurtailmentRecords } from "./reingest_2025_03_24";

const TARGET_DATE = '2025-03-24';
const YEAR_MONTH = '2025-03';
const YEAR = '2025';

/**
 * Update the daily summary record for 2025-03-24
 */
async function updateDailySummary(): Promise<void> {
  console.log(`\n=== Updating Daily Summary for ${TARGET_DATE} ===`);
  
  try {
    // Calculate totals from curtailment records
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume})::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
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
      totalCurtailedEnergy: totals[0].totalCurtailedEnergy,
      totalPayment: totals[0].totalPayment
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totals[0].totalCurtailedEnergy,
        totalPayment: totals[0].totalPayment
      }
    });
    
    // Verify the update
    const summary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE)
    });
    
    console.log('Daily summary updated:', {
      energy: summary?.totalCurtailedEnergy ? `${Number(summary.totalCurtailedEnergy).toFixed(2)} MWh` : '0.00 MWh',
      payment: summary?.totalPayment ? `£${Number(summary.totalPayment).toFixed(2)}` : '£0.00'
    });
  } catch (error) {
    console.error('Error updating daily summary:', error);
    throw error;
  }
}

/**
 * Update the monthly summary for March 2025
 */
async function updateMonthlySummary(): Promise<void> {
  console.log(`\n=== Updating Monthly Summary for ${YEAR_MONTH} ===`);
  
  try {
    // Calculate monthly totals from daily summaries
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
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
    
    // Verify the update
    const summary = await db.query.monthlySummaries.findFirst({
      where: eq(monthlySummaries.yearMonth, YEAR_MONTH)
    });
    
    console.log('Monthly summary updated:', {
      month: YEAR_MONTH,
      energy: summary?.totalCurtailedEnergy ? `${Number(summary.totalCurtailedEnergy).toFixed(2)} MWh` : '0.00 MWh',
      payment: summary?.totalPayment ? `£${Number(summary.totalPayment).toFixed(2)}` : '£0.00'
    });
  } catch (error) {
    console.error('Error updating monthly summary:', error);
    throw error;
  }
}

/**
 * Update the yearly summary for 2025
 */
async function updateYearlySummary(): Promise<void> {
  console.log(`\n=== Updating Yearly Summary for ${YEAR} ===`);
  
  try {
    // Calculate yearly totals from daily summaries
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
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
    
    // Verify the update
    const summary = await db.query.yearlySummaries.findFirst({
      where: eq(yearlySummaries.year, YEAR)
    });
    
    console.log('Yearly summary updated:', {
      year: YEAR,
      energy: summary?.totalCurtailedEnergy ? `${Number(summary.totalCurtailedEnergy).toFixed(2)} MWh` : '0.00 MWh',
      payment: summary?.totalPayment ? `£${Number(summary.totalPayment).toFixed(2)}` : '£0.00'
    });
  } catch (error) {
    console.error('Error updating yearly summary:', error);
    throw error;
  }
}

/**
 * Update Bitcoin calculations for all standard miner models
 */
async function updateBitcoinCalculations(): Promise<void> {
  console.log(`\n=== Updating Bitcoin Calculations for ${TARGET_DATE} ===`);
  
  const minerModels = ['S19J_PRO', 'S9', 'M20S'];
  
  for (const minerModel of minerModels) {
    try {
      console.log(`Processing Bitcoin calculations for ${minerModel}...`);
      await processSingleDay(TARGET_DATE, minerModel);
      console.log(`✓ Completed Bitcoin calculations for ${minerModel}`);
    } catch (error) {
      console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
      // Continue with other models even if one fails
    }
  }
}

/**
 * Run the complete update process for 2025-03-24
 */
async function runFullUpdate(): Promise<void> {
  const startTime = Date.now();
  
  console.log(`\n=====================================`);
  console.log(`STARTING FULL UPDATE FOR ${TARGET_DATE}`);
  console.log(`=====================================\n`);
  
  try {
    // Step 1: Reingest curtailment records
    await reingestCurtailmentRecords();
    console.log('✓ Completed reingestion of curtailment records');
    
    // Step 2: Update daily summary
    await updateDailySummary();
    console.log('✓ Completed daily summary update');
    
    // Step 3: Update monthly summary
    await updateMonthlySummary();
    console.log('✓ Completed monthly summary update');
    
    // Step 4: Update yearly summary
    await updateYearlySummary();
    console.log('✓ Completed yearly summary update');
    
    // Step 5: Update Bitcoin calculations
    await updateBitcoinCalculations();
    console.log('✓ Completed Bitcoin calculations update');
    
    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000;
    
    console.log(`\n=====================================`);
    console.log(`FULL UPDATE COMPLETED FOR ${TARGET_DATE}`);
    console.log(`Duration: ${duration.toFixed(2)} seconds`);
    console.log(`=====================================\n`);
  } catch (error) {
    console.error(`\nERROR DURING FULL UPDATE:`, error);
    throw error;
  }
}

// Execute the full update process
(async () => {
  try {
    await runFullUpdate();
    process.exit(0);
  } catch (error) {
    console.error('Update process failed:', error);
    process.exit(1);
  }
})();
/**
 * Update Summary Tables for March 21, 2025
 * 
 * This script calculates and updates the daily, monthly, and yearly summaries
 * based on the curtailment records for March 21, 2025 that have been ingested.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-21';
const EXPECTED_TOTAL_PAYMENT = 1240439.58; // Expected total in GBP

async function updateSummaries(): Promise<void> {
  try {
    console.log(`Updating summary records for ${TARGET_DATE}...`);
    
    // Calculate totals from curtailment records
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (!totals[0] || !totals[0].totalCurtailedEnergy) {
      console.error('Error: No curtailment records found to create summary');
      return;
    }
    
    console.log('Raw totals from database:');
    console.log('- Energy:', totals[0].totalCurtailedEnergy, 'MWh');
    console.log('- Payment:', totals[0].totalPayment);
    
    // Update daily summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totals[0].totalCurtailedEnergy,
      totalPayment: totals[0].totalPayment,
      totalWindGeneration: '0',
      windOnshoreGeneration: '0',
      windOffshoreGeneration: '0',
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totals[0].totalCurtailedEnergy,
        totalPayment: totals[0].totalPayment,
        lastUpdated: new Date()
      }
    });
    
    console.log(`Daily summary updated for ${TARGET_DATE}:`);
    console.log(`- Energy: ${totals[0].totalCurtailedEnergy} MWh`);
    console.log(`- Payment: £${totals[0].totalPayment}`);
    
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
    console.error('Error updating summaries:', error);
    throw error;
  }
}

// Update Bitcoin calculations
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
  console.log(`=== Updating Summaries for March 21, 2025 ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    // Step 1: Update all summary tables
    await updateSummaries();
    
    // Step 2: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 3: Verify the final state
    const finalStatus = await db
      .select({
        periodCount: sql`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        recordCount: sql`COUNT(*)`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nFinal Status for ${TARGET_DATE}:`);
    console.log(`- Settlement Periods present: ${finalStatus[0].periodCount}/48`);
    console.log(`- Records: ${finalStatus[0].recordCount}`);
    console.log(`- Total Volume: ${finalStatus[0].totalVolume} MWh`);
    console.log(`- Total Payment: £${finalStatus[0].totalPayment}`);
    
    // Check if payment matches expected amount
    const paymentTotal = parseFloat(finalStatus[0].totalPayment);
    if (Math.abs(paymentTotal - EXPECTED_TOTAL_PAYMENT) > 100) {
      console.log(`WARNING: Final payment total £${paymentTotal.toFixed(2)} differs from expected £${EXPECTED_TOTAL_PAYMENT.toFixed(2)}`);
      console.log(`Difference: £${Math.abs(paymentTotal - EXPECTED_TOTAL_PAYMENT).toFixed(2)}`);
    } else {
      console.log(`SUCCESS: Final payment total £${paymentTotal.toFixed(2)} matches expected total (within £100 margin)`);
    }
    
    // Verify daily summary
    const dailySummary = await db
      .select({
        totalCurtailedEnergy: dailySummaries.totalCurtailedEnergy,
        totalPayment: dailySummaries.totalPayment
      })
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    if (dailySummary[0]) {
      console.log(`\nVerified Daily Summary for ${TARGET_DATE}:`);
      console.log(`- Energy: ${dailySummary[0].totalCurtailedEnergy} MWh`);
      console.log(`- Payment: £${dailySummary[0].totalPayment}`);
    } else {
      console.log(`ERROR: Daily summary not found for ${TARGET_DATE}`);
    }
    
    console.log(`\nUpdate completed successfully at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Error during update process:', error);
    process.exit(1);
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
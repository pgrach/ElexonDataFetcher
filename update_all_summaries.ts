/**
 * Update All Summaries
 * 
 * This script updates all summary tables including daily, monthly, and yearly summaries,
 * as well as Bitcoin calculations, based on the reprocessed data.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-28';

// Step 1: Create or update daily summary
async function updateDailySummary(): Promise<void> {
  console.log(`\nStep 1: Creating/updating daily summary for ${TARGET_DATE}...`);
  
  try {
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
    
    // Create or update daily summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totals[0].totalCurtailedEnergy,
      totalPayment: totals[0].totalPayment,
      totalWindGeneration: '0', // We don't have this data yet
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
    
    console.log(`Created/updated daily summary for ${TARGET_DATE}:`);
    console.log(`- Energy: ${totals[0].totalCurtailedEnergy} MWh`);
    console.log(`- Payment: £${totals[0].totalPayment}`);
  } catch (error) {
    console.error('Error creating/updating daily summary:', error);
    throw error;
  }
}

// Step 2: Update monthly summary
async function updateMonthlySummary(yearMonth: string): Promise<void> {
  console.log(`\nStep 2: Updating monthly summary for ${yearMonth}...`);
  
  try {
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
      
      console.log(`Updated monthly summary for ${yearMonth}:`);
      console.log(`- Energy: ${monthlyTotals[0].totalCurtailedEnergy} MWh`);
      console.log(`- Payment: £${monthlyTotals[0].totalPayment}`);
    }
  } catch (error) {
    console.error(`Error updating monthly summary for ${yearMonth}:`, error);
    throw error;
  }
}

// Step 3: Update yearly summary
async function updateYearlySummary(year: string): Promise<void> {
  console.log(`\nStep 3: Updating yearly summary for ${year}...`);
  
  try {
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
      
      console.log(`Updated yearly summary for ${year}:`);
      console.log(`- Energy: ${yearlyTotals[0].totalCurtailedEnergy} MWh`);
      console.log(`- Payment: £${yearlyTotals[0].totalPayment}`);
    }
  } catch (error) {
    console.error(`Error updating yearly summary for ${year}:`, error);
    throw error;
  }
}

// Step 4: Update Bitcoin calculations
async function updateBitcoinCalculations(): Promise<void> {
  console.log(`\nStep 4: Updating Bitcoin calculations...`);
  
  try {
    // Clear existing Bitcoin calculations first
    try {
      const { historicalBitcoinCalculations } = await import('./db/schema');
      await db.delete(historicalBitcoinCalculations)
        .where(eq(historicalBitcoinCalculations.calculationDate, TARGET_DATE));
      console.log(`Cleared existing Bitcoin calculations for ${TARGET_DATE}`);
    } catch (error) {
      console.warn('Note: Error clearing Bitcoin calculations. Will try to create new ones anyway.');
    }
    
    // Update Bitcoin calculations for each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    for (const minerModel of minerModels) {
      await processSingleDay(TARGET_DATE, minerModel);
      console.log(`- Processed Bitcoin calculations for ${minerModel}`);
    }
    
    console.log('Bitcoin calculations updated successfully');
    
    // Update monthly and yearly Bitcoin summaries
    try {
      const { updateMonthlyBitcoinSummary, updateYearlyBitcoinSummary } = await import('./server/services/bitcoinService');
      const yearMonth = TARGET_DATE.substring(0, 7);
      const year = TARGET_DATE.substring(0, 4);
      
      for (const minerModel of minerModels) {
        if (typeof updateMonthlyBitcoinSummary === 'function') {
          await updateMonthlyBitcoinSummary(yearMonth, minerModel);
          console.log(`- Updated monthly Bitcoin summary for ${yearMonth} and ${minerModel}`);
        }
        
        if (typeof updateYearlyBitcoinSummary === 'function') {
          await updateYearlyBitcoinSummary(year, minerModel);
          console.log(`- Updated yearly Bitcoin summary for ${year} and ${minerModel}`);
        }
      }
    } catch (error) {
      console.warn('Note: Could not update monthly/yearly Bitcoin summaries. This will need to be done separately.');
      console.error(error);
    }
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    throw error;
  }
}

// Step 5: Verify data
async function verifyData(): Promise<void> {
  console.log(`\nStep 5: Verifying data...`);
  
  try {
    // Check curtailment records
    const recordCount = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Check number of periods
    const periodCount = await db
      .select({ count: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Check totals from curtailment records
    const curtailmentTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Check daily summary
    const dailySummary = await db
      .select({
        totalCurtailedEnergy: dailySummaries.totalCurtailedEnergy,
        totalPayment: dailySummaries.totalPayment
      })
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`Verification Results:`);
    console.log(`- Total Records: ${recordCount[0].count}`);
    console.log(`- Unique Periods: ${periodCount[0].count}/48`);
    
    console.log(`\nCurtailment Records Totals:`);
    console.log(`- Total Energy: ${curtailmentTotals[0].totalCurtailedEnergy} MWh`);
    console.log(`- Total Payment: £${curtailmentTotals[0].totalPayment}`);
    
    console.log(`\nDaily Summary Values:`);
    if (dailySummary.length > 0) {
      console.log(`- Energy: ${dailySummary[0].totalCurtailedEnergy} MWh`);
      console.log(`- Payment: £${dailySummary[0].totalPayment}`);
    } else {
      console.log('Daily summary not found!');
    }
    
    // Compare with expected values
    const expectedVolume = 99904.22;
    const expectedPayment = -3784089.62;
    
    const volumeDiff = Math.abs(expectedVolume - parseFloat(curtailmentTotals[0].totalCurtailedEnergy));
    const paymentDiff = Math.abs(expectedPayment - parseFloat(curtailmentTotals[0].totalPayment));
    
    console.log(`\nComparison to Expected Values:`);
    console.log(`- Expected Energy: ${expectedVolume} MWh (Diff: ${volumeDiff.toFixed(2)} MWh, ${((volumeDiff / expectedVolume) * 100).toFixed(2)}%)`);
    console.log(`- Expected Payment: £${expectedPayment} (Diff: £${paymentDiff.toFixed(2)}, ${((paymentDiff / Math.abs(expectedPayment)) * 100).toFixed(2)}%)`);
  } catch (error) {
    console.error('Error verifying data:', error);
    throw error;
  }
}

// Main function
async function main(): Promise<void> {
  console.log(`=== Updating Summaries for ${TARGET_DATE} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    // Step 1: Update daily summary
    await updateDailySummary();
    
    // Step 2: Update monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7);
    await updateMonthlySummary(yearMonth);
    
    // Step 3: Update yearly summary
    const year = TARGET_DATE.substring(0, 4);
    await updateYearlySummary(year);
    
    // Step 4: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 5: Verify data
    await verifyData();
    
    console.log(`\nSummary updates completed successfully at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Error during summary updates:', error);
    process.exit(1);
  }
}

// Execute main function
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
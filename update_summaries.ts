/**
 * Update Summary Tables
 * 
 * This script calculates and updates the daily, monthly, and yearly summaries
 * based on the curtailment records for a specific date. By default, it uses
 * March 28, 2025 if no date is provided.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq } from "drizzle-orm";
import { sql } from "drizzle-orm/sql";

// Get date from command line arguments or use default
const DEFAULT_DATE = '2025-03-28';
const args = process.argv.slice(2);
const DATE_ARG = args.length > 0 ? args[0] : DEFAULT_DATE;

/**
 * Update all summary tables: daily, monthly, and yearly
 * 
 * @export Can be imported by other modules
 * @param targetDate Optional date to update, defaults to command line argument or default date
 */
export async function updateSummaries(targetDate: string = DATE_ARG): Promise<void> {
  try {
    // Validate date format (YYYY-MM-DD)
    if (!targetDate.match(/^\d{4}-\d{2}-\d{2}$/)) {
      console.error(`Invalid date format: ${targetDate}. Expected format: YYYY-MM-DD`);
      return;
    }
    
    console.log(`Updating summary records for ${targetDate}...`);
    
    // Calculate totals from curtailment records
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, targetDate));
    
    if (!totals[0] || !totals[0].totalCurtailedEnergy) {
      console.error('Error: No curtailment records found to create summary');
      return;
    }
    
    // Update daily summary
    await db.insert(dailySummaries).values({
      summaryDate: targetDate,
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
    
    console.log(`Daily summary updated for ${targetDate}:`);
    console.log(`- Energy: ${totals[0].totalCurtailedEnergy} MWh`);
    console.log(`- Payment: £${totals[0].totalPayment}`);
    
    // Update monthly summary
    const yearMonth = targetDate.substring(0, 7);
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
    const year = targetDate.substring(0, 4);
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

/**
 * Update Bitcoin mining calculations for the date
 * 
 * @export Can be imported by other modules
 * @param targetDate Optional date to update, defaults to command line argument or default date
 */
export async function updateBitcoinCalculations(targetDate: string = DATE_ARG): Promise<void> {
  // Validate date format (YYYY-MM-DD)
  if (!targetDate.match(/^\d{4}-\d{2}-\d{2}$/)) {
    console.error(`Invalid date format: ${targetDate}. Expected format: YYYY-MM-DD`);
    return;
  }
  
  console.log(`\nUpdating Bitcoin calculations for ${targetDate}...`);
  
  try {
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    for (const minerModel of minerModels) {
      await processSingleDay(targetDate, minerModel);
      console.log(`- Processed ${minerModel} model calculations`);
    }
    
    console.log('Bitcoin calculations updated successfully');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    throw error;
  }
}

/**
 * Main function to execute the script
 */
async function main(): Promise<void> {
  console.log(`=== Updating Summaries for ${DATE_ARG} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    // Update the summary tables
    await updateSummaries(DATE_ARG);
    
    // Update Bitcoin calculations
    await updateBitcoinCalculations(DATE_ARG);
    
    console.log(`\nSummary updates completed successfully at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Error during summary update process:', error);
    process.exit(1);
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
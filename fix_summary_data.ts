/**
 * Fix Summary Data for Date Range
 * 
 * This script corrects daily, monthly, and yearly summaries for a specified date range
 * by recalculating them directly from the curtailment_records table.
 * 
 * Usage: 
 *   npx tsx fix_summary_data.ts <start_date> <end_date>
 * 
 * Example:
 *   npx tsx fix_summary_data.ts 2025-03-28 2025-03-31
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, sql, between } from "drizzle-orm";

// Get dates from command line arguments or use defaults
const startDate = process.argv[2] || '2025-03-28';
const endDate = process.argv[3] || '2025-03-29';

// Set of months and years that need to be updated
const affectedMonths = new Set<string>();
const affectedYears = new Set<string>();

/**
 * Format a date string as YYYY-MM-DD
 */
function formatDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

/**
 * Generate an array of dates between start and end date
 */
function generateDateRange(start: string, end: string): string[] {
  const dateArray: string[] = [];
  let currentDate = new Date(start);
  const endDateObj = new Date(end);
  
  while (currentDate <= endDateObj) {
    dateArray.push(formatDate(currentDate));
    currentDate.setDate(currentDate.getDate() + 1);
  }
  
  return dateArray;
}

/**
 * Fix a single day's summary
 */
async function fixDailySummary(date: string): Promise<void> {
  console.log(`\n=== Processing ${date} ===`);
  
  try {
    // Get the correct totals from curtailment_records
    console.log(`Calculating correct totals for ${date}...`);
    const correctedTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(volume)::numeric)`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    if (!correctedTotals[0]?.totalCurtailedEnergy) {
      console.log(`No curtailment records found for ${date}, skipping`);
      return;
    }

    const totalCurtailedEnergy = correctedTotals[0].totalCurtailedEnergy;
    const totalPayment = correctedTotals[0].totalPayment;

    // Get current values for comparison
    const currentSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date));

    console.log("Current Summary vs Correct Values:");
    console.log(`- Energy: ${currentSummary[0]?.totalCurtailedEnergy || 'N/A'} => ${totalCurtailedEnergy} MWh`);
    console.log(`- Payment: £${currentSummary[0]?.totalPayment || 'N/A'} => £${totalPayment}`);

    // Update the daily summary
    await db.insert(dailySummaries).values({
      summaryDate: date,
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
    console.log(`Daily summary for ${date} updated successfully`);
    
    // Add the month and year to be updated later
    const month = date.substring(0, 7);
    const year = date.substring(0, 4);
    affectedMonths.add(month);
    affectedYears.add(year);
  } catch (error) {
    console.error(`Error fixing daily summary for ${date}:`, error);
    throw error;
  }
}

/**
 * Update monthly summaries for all affected months
 */
async function updateMonthlySummaries(): Promise<void> {
  for (const month of affectedMonths) {
    console.log(`\n=== Updating Monthly Summary for ${month} ===`);
    
    try {
      const monthlyTotals = await db
        .select({
          totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
          totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
        })
        .from(dailySummaries)
        .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${month + '-01'}::date)`);

      if (!monthlyTotals[0]?.totalCurtailedEnergy) {
        console.log(`No daily summaries found for ${month}, skipping`);
        continue;
      }

      await db.insert(monthlySummaries).values({
        yearMonth: month,
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
      
      console.log(`Monthly summary for ${month} updated successfully`);
      console.log(`- New Monthly Energy: ${monthlyTotals[0].totalCurtailedEnergy} MWh`);
      console.log(`- New Monthly Payment: £${monthlyTotals[0].totalPayment}`);
    } catch (error) {
      console.error(`Error updating monthly summary for ${month}:`, error);
      throw error;
    }
  }
}

/**
 * Update yearly summaries for all affected years
 */
async function updateYearlySummaries(): Promise<void> {
  for (const year of affectedYears) {
    console.log(`\n=== Updating Yearly Summary for ${year} ===`);
    
    try {
      const yearlyTotals = await db
        .select({
          totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
          totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
        })
        .from(dailySummaries)
        .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${year + '-01-01'}::date)`);

      if (!yearlyTotals[0]?.totalCurtailedEnergy) {
        console.log(`No daily summaries found for ${year}, skipping`);
        continue;
      }

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
      
      console.log(`Yearly summary for ${year} updated successfully`);
      console.log(`- New Yearly Energy: ${yearlyTotals[0].totalCurtailedEnergy} MWh`);
      console.log(`- New Yearly Payment: £${yearlyTotals[0].totalPayment}`);
    } catch (error) {
      console.error(`Error updating yearly summary for ${year}:`, error);
      throw error;
    }
  }
}

/**
 * Main function to fix all summary data
 */
async function fixSummaryData(): Promise<void> {
  console.log(`Starting summary data fix for dates from ${startDate} to ${endDate}`);
  
  try {
    // Generate array of dates between start and end date
    const dates = generateDateRange(startDate, endDate);
    console.log(`Will process ${dates.length} days: ${dates.join(', ')}`);
    
    // First, update all daily summaries
    for (const date of dates) {
      await fixDailySummary(date);
    }
    
    // Then update all affected monthly summaries
    await updateMonthlySummaries();
    
    // Finally, update all affected yearly summaries
    await updateYearlySummaries();
    
    console.log("\n=== Summary Data Fix Completed Successfully ===");
  } catch (error) {
    console.error("Error fixing summary data:", error);
    throw error;
  }
}

// Run the function
fixSummaryData()
  .then(() => {
    console.log("Script completed successfully!");
    process.exit(0);
  })
  .catch((error) => {
    console.error("Script failed:", error);
    process.exit(1);
  });
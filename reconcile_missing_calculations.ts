/**
 * This script identifies and fixes specific time periods with missing Bitcoin calculations.
 * It targets the time periods identified in our analysis and processes them in a targeted way.
 */

import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { sql, and, eq, between, desc } from "drizzle-orm";
import { format, parseISO, eachMonthOfInterval, addDays, subDays, isValid } from "date-fns";
import pLimit from "p-limit";
import fs from "fs/promises";
import path from "path";
import { fixAllMissingCalculations, getMonthSummary, getDatesThatNeedFixing } from "./fix_all_bitcoin_calculations";

// Configuration
const TARGET_PERIODS = [
  // Missing periods (2022)
  { year: 2022, month: 4 },
  { year: 2022, month: 6 },
  { year: 2022, month: 7 },
  { year: 2022, month: 8 },
  { year: 2022, month: 9 },
  { year: 2022, month: 10 },
  { year: 2022, month: 11 },
  
  // Missing periods (2023)
  { year: 2023, month: 1 },
  { year: 2023, month: 2 },
  { year: 2023, month: 3 },
  { year: 2023, month: 4 },
  { year: 2023, month: 5 },
  { year: 2023, month: 6 },
  { year: 2023, month: 7 },
  { year: 2023, month: 8 },
  { year: 2023, month: 9 },
  { year: 2023, month: 10 },
  { year: 2023, month: 11 },
  { year: 2023, month: 12 },
  
  // Incomplete periods (2022)
  { year: 2022, month: 1 },
  { year: 2022, month: 2 },
  { year: 2022, month: 3 },
  { year: 2022, month: 5 },
  { year: 2022, month: 12 },
  
  // Incomplete periods (2024-2025)
  { year: 2024, month: 9 },
  { year: 2024, month: 12 },
  { year: 2025, month: 1 },
  { year: 2025, month: 2 }
];

interface PeriodSummary {
  yearMonth: string;
  startDate: string;
  endDate: string;
  curtailmentCount: number;
  bitcoinCount: number | null;
  status: 'Missing' | 'Incomplete' | 'Complete';
}

/**
 * Get information about a specific year-month period
 */
async function getPeriodInfo(year: number, month: number): Promise<PeriodSummary> {
  // Format year-month string
  const yearMonth = `${year}-${month.toString().padStart(2, '0')}`;
  
  // Determine start and end dates
  const startDate = `${yearMonth}-01`;
  
  // Get the last day of the month
  const nextMonth = month === 12 ? new Date(year + 1, 0, 1) : new Date(year, month, 1);
  const lastDay = new Date(nextMonth.getTime() - 1).getDate();
  const endDate = `${yearMonth}-${lastDay.toString().padStart(2, '0')}`;
  
  // Get counts from database
  const curtailmentCount = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(curtailmentRecords)
    .where(and(
      sql`settlement_date >= ${startDate}`,
      sql`settlement_date <= ${endDate}`
    ));
  
  const bitcoinCount = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(historicalBitcoinCalculations)
    .where(and(
      sql`settlement_date >= ${startDate}`,
      sql`settlement_date <= ${endDate}`,
      sql`miner_model = 'S19J_PRO'`
    ));
  
  const totalCurtailment = curtailmentCount[0]?.count || 0;
  const totalBitcoin = bitcoinCount[0]?.count || 0;
  
  // Determine status
  let status: 'Missing' | 'Incomplete' | 'Complete';
  if (totalBitcoin === 0) {
    status = 'Missing';
  } else if (totalBitcoin < totalCurtailment) {
    status = 'Incomplete';
  } else {
    status = 'Complete';
  }
  
  return {
    yearMonth,
    startDate,
    endDate,
    curtailmentCount: totalCurtailment,
    bitcoinCount: totalBitcoin,
    status
  };
}

/**
 * Get a list of dates within a specific period that need fixing
 */
async function getDatesInPeriod(startDate: string, endDate: string): Promise<string[]> {
  try {
    const result = await db.execute(sql`
      WITH curtailment_dates AS (
        SELECT DISTINCT settlement_date::text
        FROM curtailment_records
        WHERE settlement_date BETWEEN ${startDate} AND ${endDate}
        ORDER BY settlement_date
      )
      SELECT settlement_date as date
      FROM curtailment_dates
    `);
    
    return result.rows.map(row => row.date);
  } catch (error) {
    console.error("Error getting dates in period:", error);
    return [];
  }
}

/**
 * Main function to reconcile missing calculations for target periods
 */
async function reconcileTargetPeriods() {
  console.log("=== Starting Targeted Bitcoin Calculation Reconciliation ===");
  
  // Process each target period
  for (const period of TARGET_PERIODS) {
    const { year, month } = period;
    console.log(`\nProcessing period: ${year}-${month.toString().padStart(2, '0')}`);
    
    // Get information about this period
    const periodInfo = await getPeriodInfo(year, month);
    console.log(`Status: ${periodInfo.status}`);
    console.log(`Date range: ${periodInfo.startDate} to ${periodInfo.endDate}`);
    console.log(`Curtailment records: ${periodInfo.curtailmentCount}`);
    console.log(`Bitcoin calculations: ${periodInfo.bitcoinCount || 0}`);
    
    if (periodInfo.status === 'Complete') {
      console.log("This period is already complete. Skipping.");
      continue;
    }
    
    // Get all dates in this period
    const dates = await getDatesInPeriod(periodInfo.startDate, periodInfo.endDate);
    console.log(`Found ${dates.length} dates with curtailment records in this period.`);
    
    if (dates.length === 0) {
      console.log("No dates found for this period. Skipping.");
      continue;
    }
    
    // Use the comprehensive fix script to process these dates
    console.log(`Fixing calculations for ${year}-${month.toString().padStart(2, '0')}...`);
    await fixAllMissingCalculations(); // This will handle all the details of fixing
    
    // Verify that the fix was successful
    const updatedInfo = await getPeriodInfo(year, month);
    console.log(`\nAfter processing:`);
    console.log(`Status: ${updatedInfo.status}`);
    console.log(`Bitcoin calculations: ${updatedInfo.bitcoinCount || 0} / ${updatedInfo.curtailmentCount}`);
    
    if (updatedInfo.status !== 'Complete') {
      console.log(`Warning: Period is still ${updatedInfo.status.toLowerCase()} after processing.`);
    }
  }
  
  console.log("\n=== Targeted Reconciliation Complete ===");
  console.log("Running a final verification of all months...");
  
  // Get a summary of all months to verify our progress
  const finalSummary = await getMonthSummary();
  
  // Count the different statuses
  const missingMonths = finalSummary.filter(m => m.status === 'Missing').length;
  const incompleteMonths = finalSummary.filter(m => m.status === 'Incomplete').length;
  const completeMonths = finalSummary.filter(m => m.status === 'Complete').length;
  
  console.log(`\nFinal status:`);
  console.log(`- ${missingMonths} months with missing calculations`);
  console.log(`- ${incompleteMonths} months with incomplete calculations`);
  console.log(`- ${completeMonths} months with complete calculations`);
  
  if (missingMonths === 0 && incompleteMonths === 0) {
    console.log("\nSuccess! All Bitcoin calculations are now complete.");
  } else {
    console.log("\nSome periods still need attention. Consider running the main fix script again.");
  }
}

/**
 * Entry point for the script
 */
async function main() {
  try {
    await reconcileTargetPeriods();
    process.exit(0);
  } catch (error) {
    console.error("Fatal error:", error);
    process.exit(1);
  }
}

// Run the script if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}
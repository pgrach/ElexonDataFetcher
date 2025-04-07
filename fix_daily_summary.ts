/**
 * Fix Daily Summary
 * 
 * This script fixes the daily summary for a specific date based on the
 * hourly records, ensuring the totals match.
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { eq } from 'drizzle-orm';
import { format } from 'date-fns';

/**
 * Fix the daily summary for a specific date
 */
async function fixDailySummary(date: string): Promise<{
  date: string;
  totalRecords: number;
  periodsPresent: number[];
  totalVolume: number;
  totalPayment: number;
  summaryUpdated: boolean;
}> {
  console.log(`\n=== Fixing Daily Summary for ${date} ===\n`);
  
  // Get all records for this date
  const records = await db.query.curtailmentRecords.findMany({
    where: eq(curtailmentRecords.settlementDate, date)
  });
  
  console.log(`Found ${records.length} records for ${date}`);
  
  // Get unique periods
  const periodsPresent = Array.from(new Set(records.map(r => r.settlementPeriod))).sort((a, b) => a - b);
  
  // Calculate totals
  const totalVolume = records.reduce((sum, r) => sum + Math.abs(parseFloat(r.volume.toString())), 0);
  const totalPayment = records.reduce((sum, r) => sum + parseFloat(r.payment.toString()), 0);
  
  console.log(`Periods Present: ${periodsPresent.length}/48`);
  console.log(`Total Volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total Payment: £${totalPayment.toFixed(2)}`);
  
  // Get existing daily summary
  const existingSummary = await db.query.dailySummaries.findFirst({
    where: eq(dailySummaries.summaryDate, date)
  });
  
  let summaryUpdated = false;
  
  if (existingSummary) {
    // Update existing summary
    await db
      .update(dailySummaries)
      .set({
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: totalPayment.toString(),
        lastUpdated: new Date()
      })
      .where(eq(dailySummaries.summaryDate, date));
    
    console.log(`Updated existing daily summary for ${date}`);
    summaryUpdated = true;
  } else {
    // Create new summary
    await db.insert(dailySummaries).values({
      summaryDate: date,
      totalCurtailedEnergy: totalVolume.toString(),
      totalPayment: totalPayment.toString(),
      lastUpdated: new Date()
    });
    
    console.log(`Created new daily summary for ${date}`);
    summaryUpdated = true;
  }
  
  return {
    date,
    totalRecords: records.length,
    periodsPresent,
    totalVolume,
    totalPayment,
    summaryUpdated
  };
}

/**
 * Main function
 */
async function main() {
  try {
    // Get the date from command-line arguments or use default
    const dateToFix = process.argv[2] || format(new Date(), 'yyyy-MM-dd');
    
    // Fix the daily summary for the date
    await fixDailySummary(dateToFix);
    
    console.log(`\n=== Daily Summary Fixed for ${dateToFix} ===\n`);
  } catch (error) {
    console.error(`Error in main process:`, error);
    process.exit(1);
  }
}

main();
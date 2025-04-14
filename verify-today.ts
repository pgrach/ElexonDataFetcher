/**
 * Verify Bitcoin Table Updates for Today
 * 
 * This script validates that all four Bitcoin tables have been properly updated.
 */

import { db } from "./db";
import { 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries 
} from "./db/schema";
import { format } from "date-fns";
import { eq, sql } from "drizzle-orm";

/**
 * Verify that all four Bitcoin tables were updated
 * @param date - The settlement date
 * @returns Object with counts of records in each table
 */
async function verifyTableUpdates(date: string): Promise<{
  historical: number,
  daily: number,
  monthly: number,
  yearly: number
}> {
  const yearMonth = date.substring(0, 7); // YYYY-MM format
  const year = date.substring(0, 4);      // YYYY format
  
  // Check historicalBitcoinCalculations table
  const historicalCount = await db.select({
    count: sql<number>`count(*)`.as('count')
  })
  .from(historicalBitcoinCalculations)
  .where(eq(historicalBitcoinCalculations.settlementDate, date));
  
  // Check bitcoinDailySummaries table
  const dailyCount = await db.select({
    count: sql<number>`count(*)`.as('count')
  })
  .from(bitcoinDailySummaries)
  .where(eq(bitcoinDailySummaries.summaryDate, date));
  
  // Check bitcoinMonthlySummaries table
  const monthlyCount = await db.select({
    count: sql<number>`count(*)`.as('count')
  })
  .from(bitcoinMonthlySummaries)
  .where(eq(bitcoinMonthlySummaries.yearMonth, yearMonth));
  
  // Check bitcoinYearlySummaries table
  const yearlyCount = await db.select({
    count: sql<number>`count(*)`.as('count')
  })
  .from(bitcoinYearlySummaries)
  .where(eq(bitcoinYearlySummaries.year, year));
  
  return {
    historical: Number(historicalCount[0]?.count || 0),
    daily: Number(dailyCount[0]?.count || 0),
    monthly: Number(monthlyCount[0]?.count || 0),
    yearly: Number(yearlyCount[0]?.count || 0)
  };
}

async function verifyTodaysTables() {
  try {
    // Get today's date in YYYY-MM-DD format
    const today = format(new Date(), 'yyyy-MM-dd');
    
    console.log(`\n=== Verifying Bitcoin Tables for ${today} ===`);
    
    const counts = await verifyTableUpdates(today);
    
    console.log(`\nTable record counts for ${today}:`);
    console.log(`- Historical: ${counts.historical} records ${counts.historical > 0 ? '✓' : '❌'}`);
    console.log(`- Daily: ${counts.daily} records ${counts.daily > 0 ? '✓' : '❌'}`);
    console.log(`- Monthly: ${counts.monthly} records ${counts.monthly > 0 ? '✓' : '❌'} (for ${today.substring(0, 7)})`);
    console.log(`- Yearly: ${counts.yearly} records ${counts.yearly > 0 ? '✓' : '❌'} (for ${today.substring(0, 4)})`);
    
    // Alert if any tables have no records
    if (counts.historical === 0 || counts.daily === 0 || 
        counts.monthly === 0 || counts.yearly === 0) {
      console.log(`\n⚠️ WARNING: Some tables may not have been updated properly for ${today}`);
    } else {
      console.log(`\n✅ All four tables have been properly updated for ${today}`);
    }
    
    return counts;
  } catch (error) {
    console.error('Error verifying tables:', error);
    return null;
  }
}

// Execute the verification
verifyTodaysTables();
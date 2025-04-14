/**
 * Bitcoin Tables Verification Script
 * 
 * This script verifies that all four Bitcoin tables are properly updated
 * and shows data for the most recent dates.
 */

import { db } from "./db";
import { 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries 
} from "./db/schema";
import { format, subDays } from "date-fns";
import { desc, sql } from "drizzle-orm";

// Number of days to check
const DAYS_TO_CHECK = 7;

/**
 * Get the count of records in each table
 */
async function getTotalCounts() {
  const historicalCount = await db.select({
    count: sql<number>`count(*)`.as('count')
  }).from(historicalBitcoinCalculations);

  const dailyCount = await db.select({
    count: sql<number>`count(*)`.as('count')
  }).from(bitcoinDailySummaries);

  const monthlyCount = await db.select({
    count: sql<number>`count(*)`.as('count')
  }).from(bitcoinMonthlySummaries);

  const yearlyCount = await db.select({
    count: sql<number>`count(*)`.as('count')
  }).from(bitcoinYearlySummaries);

  return {
    historical: Number(historicalCount[0]?.count || 0),
    daily: Number(dailyCount[0]?.count || 0),
    monthly: Number(monthlyCount[0]?.count || 0),
    yearly: Number(yearlyCount[0]?.count || 0)
  };
}

/**
 * Get the most recent dates in each table
 */
async function getRecentDates() {
  const historicalDates = await db.select({
    date: historicalBitcoinCalculations.settlementDate
  })
  .from(historicalBitcoinCalculations)
  .orderBy(desc(historicalBitcoinCalculations.settlementDate))
  .limit(1);

  const dailyDates = await db.select({
    date: bitcoinDailySummaries.summaryDate
  })
  .from(bitcoinDailySummaries)
  .orderBy(desc(bitcoinDailySummaries.summaryDate))
  .limit(1);

  const monthlyDates = await db.select({
    yearMonth: bitcoinMonthlySummaries.yearMonth
  })
  .from(bitcoinMonthlySummaries)
  .orderBy(desc(bitcoinMonthlySummaries.yearMonth))
  .limit(1);

  const yearlyDates = await db.select({
    year: bitcoinYearlySummaries.year
  })
  .from(bitcoinYearlySummaries)
  .orderBy(desc(bitcoinYearlySummaries.year))
  .limit(1);

  return {
    historical: historicalDates[0]?.date || 'No data',
    daily: dailyDates[0]?.date || 'No data',
    monthly: monthlyDates[0]?.yearMonth || 'No data',
    yearly: yearlyDates[0]?.year || 'No data'
  };
}

/**
 * Check Bitcoin data for recent days
 */
async function checkRecentDays() {
  const today = new Date();
  const results = [];

  // Check the last DAYS_TO_CHECK days
  for (let i = 0; i < DAYS_TO_CHECK; i++) {
    const date = format(subDays(today, i), 'yyyy-MM-dd');
    const yearMonth = date.substring(0, 7); // YYYY-MM format
    const year = date.substring(0, 4);      // YYYY format

    // Check historical records for this day
    const historicalCount = await db.select({
      count: sql<number>`count(*)`.as('count')
    })
    .from(historicalBitcoinCalculations)
    .where(sql`settlement_date = ${date}`);

    // Check daily summaries for this day
    const dailyCount = await db.select({
      count: sql<number>`count(*)`.as('count')
    })
    .from(bitcoinDailySummaries)
    .where(sql`summary_date = ${date}`);

    // Check monthly summaries for this month
    const monthlyCount = await db.select({
      count: sql<number>`count(*)`.as('count')
    })
    .from(bitcoinMonthlySummaries)
    .where(sql`year_month = ${yearMonth}`);

    // Check yearly summaries for this year
    const yearlyCount = await db.select({
      count: sql<number>`count(*)`.as('count')
    })
    .from(bitcoinYearlySummaries)
    .where(sql`year = ${year}`);

    // Sum Bitcoin for this day from daily summaries
    const bitcoinSum = await db.select({
      sum: sql<number>`sum(bitcoin_mined)`.as('sum')
    })
    .from(bitcoinDailySummaries)
    .where(sql`summary_date = ${date}`);

    results.push({
      date,
      yearMonth,
      year,
      counts: {
        historical: Number(historicalCount[0]?.count || 0),
        daily: Number(dailyCount[0]?.count || 0),
        monthly: Number(monthlyCount[0]?.count || 0),
        yearly: Number(yearlyCount[0]?.count || 0)
      },
      bitcoinMined: Number(bitcoinSum[0]?.sum || 0)
    });
  }

  return results;
}

/**
 * Main verification function
 */
async function verifyBitcoinTables() {
  try {
    console.log('\n=== Bitcoin Tables Verification ===');
    
    // Get total counts
    const counts = await getTotalCounts();
    console.log('\nTotal record counts:');
    console.log(`- Historical calculations: ${counts.historical} records`);
    console.log(`- Daily summaries: ${counts.daily} records`);
    console.log(`- Monthly summaries: ${counts.monthly} records`);
    console.log(`- Yearly summaries: ${counts.yearly} records`);
    
    // Get most recent dates
    const dates = await getRecentDates();
    console.log('\nMost recent dates:');
    console.log(`- Historical calculations: ${dates.historical}`);
    console.log(`- Daily summaries: ${dates.daily}`);
    console.log(`- Monthly summaries: ${dates.monthly}`);
    console.log(`- Yearly summaries: ${dates.yearly}`);
    
    // Check recent days
    const recentData = await checkRecentDays();
    console.log(`\nChecking the last ${DAYS_TO_CHECK} days:`);
    
    for (const day of recentData) {
      const allValid = day.counts.daily === 3 && day.counts.historical > 0 && 
                      day.counts.monthly > 0 && day.counts.yearly > 0;
      
      console.log(`\n${day.date} ${allValid ? '✅' : '❌'}`);
      console.log(`- Historical: ${day.counts.historical} records`);
      console.log(`- Daily: ${day.counts.daily}/3 summaries`);
      console.log(`- Monthly (${day.yearMonth}): ${day.counts.monthly} summaries`);
      console.log(`- Yearly (${day.year}): ${day.counts.yearly} summaries`);
      
      if (day.bitcoinMined > 0) {
        console.log(`- Bitcoin mined: ${day.bitcoinMined.toFixed(8)} BTC`);
      }
      
      if (!allValid && day.counts.historical > 0 && day.counts.daily === 0) {
        console.log('  ⚠️ Missing daily summaries even though historical records exist');
      }
    }
    
    console.log('\n=== Verification Complete ===');
  } catch (error) {
    console.error('Error during verification:', error);
  }
}

// Run the verification
verifyBitcoinTables();
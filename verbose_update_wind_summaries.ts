/**
 * Verbose Wind Generation Summaries Update Script
 * 
 * This script provides detailed logging when updating the wind generation summary tables
 * from the core wind_generation_data table, addressing the issue where summary tables
 * were showing zero values despite data being available.
 * 
 * Usage:
 *   npx tsx verbose_update_wind_summaries.ts
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import { format } from 'date-fns';
import { logger } from './server/utils/logger';
import { recalculateAllSummaries } from './server/services/windSummaryService';

async function analyzeWindSummaryData() {
  try {
    // Check how much data we have in the main wind_generation_data table
    const dataCount = await db.execute(sql`
      SELECT COUNT(*) as count, 
        MIN(settlement_date) as min_date, 
        MAX(settlement_date) as max_date,
        COUNT(DISTINCT settlement_date) as unique_dates
      FROM wind_generation_data
    `);
    
    console.log('Wind Generation Data Stats:');
    console.log(`Total Records: ${dataCount[0]?.count || (dataCount as any)[0]?.count}`);
    console.log(`Date Range: ${dataCount[0]?.min_date || (dataCount as any)[0]?.min_date} to ${dataCount[0]?.max_date || (dataCount as any)[0]?.max_date}`);
    console.log(`Unique Dates: ${dataCount[0]?.unique_dates || (dataCount as any)[0]?.unique_dates}`);

    // Check daily summaries
    const dailyCount = await db.execute(sql`
      SELECT COUNT(*) as count,
        SUM(CASE WHEN total_wind_generation IS NOT NULL AND total_wind_generation != '0' THEN 1 ELSE 0 END) as with_wind_data
      FROM daily_summaries
    `);

    console.log('\nDaily Summaries:');
    console.log(`Total Records: ${dailyCount[0]?.count || (dailyCount as any)[0]?.count}`);
    console.log(`Records with Wind Data: ${dailyCount[0]?.with_wind_data || (dailyCount as any)[0]?.with_wind_data}`);

    // Check monthly summaries
    const monthlyCount = await db.execute(sql`
      SELECT COUNT(*) as count,
        SUM(CASE WHEN total_wind_generation IS NOT NULL AND total_wind_generation != '0' THEN 1 ELSE 0 END) as with_wind_data
      FROM monthly_summaries
    `);

    console.log('\nMonthly Summaries:');
    console.log(`Total Records: ${monthlyCount[0]?.count || (monthlyCount as any)[0]?.count}`);
    console.log(`Records with Wind Data: ${monthlyCount[0]?.with_wind_data || (monthlyCount as any)[0]?.with_wind_data}`);

    // Check yearly summaries
    const yearlyCount = await db.execute(sql`
      SELECT COUNT(*) as count,
        SUM(CASE WHEN total_wind_generation IS NOT NULL AND total_wind_generation != '0' THEN 1 ELSE 0 END) as with_wind_data
      FROM yearly_summaries
    `);

    console.log('\nYearly Summaries:');
    console.log(`Total Records: ${yearlyCount[0]?.count || (yearlyCount as any)[0]?.count}`);
    console.log(`Records with Wind Data: ${yearlyCount[0]?.with_wind_data || (yearlyCount as any)[0]?.with_wind_data}`);

    // Sample some recent wind generation data 
    const recentData = await db.execute(sql`
      SELECT 
        settlement_date, 
        settlement_period, 
        total_wind, 
        wind_onshore, 
        wind_offshore
      FROM wind_generation_data
      ORDER BY settlement_date DESC, settlement_period DESC
      LIMIT 5
    `);

    console.log('\nSample Recent Wind Generation Data:');
    console.table(recentData);

    // Sample some daily summaries
    const dailySummaries = await db.execute(sql`
      SELECT 
        summary_date, 
        total_wind_generation, 
        wind_onshore_generation, 
        wind_offshore_generation
      FROM daily_summaries
      ORDER BY summary_date DESC
      LIMIT 5
    `);

    console.log('\nSample Daily Summaries:');
    console.table(dailySummaries);

    // Now run the recalculation
    console.log('\nRunning full recalculation of all wind generation summaries...');
    await recalculateAllSummaries();
    
    // Check the updated state
    const updatedDailyCount = await db.execute(sql`
      SELECT COUNT(*) as count,
        SUM(CASE WHEN total_wind_generation IS NOT NULL AND total_wind_generation != '0' THEN 1 ELSE 0 END) as with_wind_data
      FROM daily_summaries
    `);

    const updatedMonthlySummaries = await db.execute(sql`
      SELECT 
        year_month, 
        total_wind_generation, 
        wind_onshore_generation, 
        wind_offshore_generation
      FROM monthly_summaries
      WHERE total_wind_generation IS NOT NULL AND total_wind_generation != '0'
      ORDER BY year_month DESC
      LIMIT 5
    `);

    console.log('\nAfter Update:');
    console.log(`Daily Summaries with Wind Data: ${updatedDailyCount[0]?.with_wind_data || (updatedDailyCount as any)[0]?.with_wind_data}`);
    
    console.log('\nUpdated Monthly Summaries:');
    console.table(updatedMonthlySummaries);

    console.log('\nRecalculation completed successfully.');
    process.exit(0);
  } catch (error) {
    console.error('Error during analysis:', error);
    process.exit(1);
  }
}

analyzeWindSummaryData();
/**
 * Check Wind Generation Data Summaries
 * 
 * This script verifies the wind generation data in the summary tables
 * and outputs a comprehensive overview of the data's completeness.
 * 
 * Usage:
 *   npx tsx check_wind_data_summaries.ts
 */

import { db } from './db';
import { logger } from './server/utils/logger';

async function checkWindDataSummaries() {
  try {
    logger.info('Starting wind generation data summary verification', {
      module: 'check_wind_data_summaries'
    });

    console.log('=== WIND GENERATION DATA SUMMARY VERIFICATION ===\n');

    // Check the wind_generation_data table
    const rawDataStats = await db.query.raw(`
      SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT settlement_date) as unique_dates,
        MIN(settlement_date) as first_date,
        MAX(settlement_date) as last_date,
        SUM(wind_onshore) as total_onshore,
        SUM(wind_offshore) as total_offshore,
        SUM(total_wind) as total_wind
      FROM wind_generation_data
    `);

    console.log('Wind Generation Raw Data:');
    console.log('------------------------');
    console.log(`Total records: ${rawDataStats[0].record_count}`);
    console.log(`Unique dates: ${rawDataStats[0].unique_dates}`);
    console.log(`Date range: ${rawDataStats[0].first_date} to ${rawDataStats[0].last_date}`);
    console.log(`Total wind generation: ${Math.round(rawDataStats[0].total_wind).toLocaleString()} MWh`);
    console.log(`  - Onshore: ${Math.round(rawDataStats[0].total_onshore).toLocaleString()} MWh`);
    console.log(`  - Offshore: ${Math.round(rawDataStats[0].total_offshore).toLocaleString()} MWh`);
    console.log();

    // Check yearly summaries
    const yearlyStats = await db.query.raw(`
      SELECT 
        COUNT(*) as record_count,
        COUNT(CASE WHEN total_wind_generation IS NOT NULL AND total_wind_generation > 0 THEN 1 END) as with_wind_data,
        MIN(year) as first_year,
        MAX(year) as last_year
      FROM yearly_summaries
    `);

    console.log('Yearly Summaries:');
    console.log('-----------------');
    console.log(`Total records: ${yearlyStats[0].record_count}`);
    console.log(`Records with wind data: ${yearlyStats[0].with_wind_data} (${Math.round((yearlyStats[0].with_wind_data / yearlyStats[0].record_count) * 100)}%)`);
    console.log(`Year range: ${yearlyStats[0].first_year} to ${yearlyStats[0].last_year}`);
    console.log();

    // Get yearly details
    const yearlyDetails = await db.query.raw(`
      SELECT 
        year, 
        total_wind_generation, 
        wind_onshore_generation, 
        wind_offshore_generation
      FROM yearly_summaries
      WHERE total_wind_generation IS NOT NULL AND total_wind_generation > 0
      ORDER BY year
    `);

    console.log('Yearly Wind Generation Breakdown:');
    console.log('--------------------------------');
    yearlyDetails.forEach((row: any) => {
      console.log(`${row.year}: ${Math.round(row.total_wind_generation).toLocaleString()} MWh (Onshore: ${Math.round(row.wind_onshore_generation).toLocaleString()} MWh, Offshore: ${Math.round(row.wind_offshore_generation).toLocaleString()} MWh)`);
    });
    console.log();

    // Check monthly summaries
    const monthlyStats = await db.query.raw(`
      SELECT 
        COUNT(*) as record_count,
        COUNT(CASE WHEN total_wind_generation IS NOT NULL AND total_wind_generation > 0 THEN 1 END) as with_wind_data,
        MIN(year_month) as first_month,
        MAX(year_month) as last_month
      FROM monthly_summaries
    `);

    console.log('Monthly Summaries:');
    console.log('------------------');
    console.log(`Total records: ${monthlyStats[0].record_count}`);
    console.log(`Records with wind data: ${monthlyStats[0].with_wind_data} (${Math.round((monthlyStats[0].with_wind_data / monthlyStats[0].record_count) * 100)}%)`);
    console.log(`Month range: ${monthlyStats[0].first_month} to ${monthlyStats[0].last_month}`);
    console.log();

    // Check monthly breakdown for the current year
    const currentYear = new Date().getFullYear();
    const currentYearMonthly = await db.query.raw(`
      SELECT 
        year_month, 
        total_wind_generation, 
        wind_onshore_generation, 
        wind_offshore_generation
      FROM monthly_summaries
      WHERE year_month LIKE '${currentYear}%' AND total_wind_generation IS NOT NULL
      ORDER BY year_month
    `);

    console.log(`${currentYear} Monthly Wind Generation::`);
    console.log('--------------------------------');
    if (currentYearMonthly.length === 0) {
      console.log(`No wind generation data available for ${currentYear}`);
    } else {
      currentYearMonthly.forEach((row: any) => {
        console.log(`${row.year_month}: ${Math.round(row.total_wind_generation).toLocaleString()} MWh (Onshore: ${Math.round(row.wind_onshore_generation).toLocaleString()} MWh, Offshore: ${Math.round(row.wind_offshore_generation).toLocaleString()} MWh)`);
      });
    }
    console.log();

    // Check daily summaries for the current month
    const currentMonth = `${currentYear}-${(new Date().getMonth() + 1).toString().padStart(2, '0')}`;
    const currentMonthDaily = await db.query.raw(`
      SELECT 
        date, 
        total_wind_generation, 
        wind_onshore_generation, 
        wind_offshore_generation
      FROM daily_summaries
      WHERE date::text LIKE '${currentMonth}%' AND total_wind_generation IS NOT NULL
      ORDER BY date DESC
      LIMIT 5
    `);

    console.log(`Recent Daily Wind Generation (${currentMonth})::`);
    console.log('--------------------------------');
    if (currentMonthDaily.length === 0) {
      console.log(`No wind generation data available for ${currentMonth}`);
    } else {
      currentMonthDaily.forEach((row: any) => {
        console.log(`${row.date}: ${Math.round(row.total_wind_generation).toLocaleString()} MWh (Onshore: ${Math.round(row.wind_onshore_generation).toLocaleString()} MWh, Offshore: ${Math.round(row.wind_offshore_generation).toLocaleString()} MWh)`);
      });
    }
    console.log();

    console.log('=== VERIFICATION COMPLETE ===');

    process.exit(0);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Error during verification: ${errorMessage}`, {
      module: 'check_wind_data_summaries'
    });
    console.error(`Error: ${errorMessage}`);
    process.exit(1);
  }
}

// Run the script
checkWindDataSummaries();
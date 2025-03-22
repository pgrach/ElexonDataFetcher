/**
 * Wind Generation Summaries Fix Script
 * 
 * This script specifically targets dates with known issues in wind generation summaries,
 * such as 2024-09-07, and updates the daily, monthly, and yearly summaries for those dates.
 * 
 * Usage:
 *   npx tsx fix_wind_summaries.ts
 */

import { updateDailySummary, updateMonthlySummary, updateYearlySummary } from './server/services/windSummaryService';
import { db } from './db';
import { sql } from 'drizzle-orm';
import { format } from 'date-fns';

// List of dates known to have wind data but zero values in summaries
const KNOWN_ISSUE_DATES = [
  '2024-04-07',
  '2024-01-07',
  '2025-03-21'
];

// List of months with potential issues
const KNOWN_ISSUE_MONTHS = [
  '2024-04',
  '2024-01',
  '2025-03'
];

// List of years to update
const YEARS_TO_UPDATE = [
  '2024',
  '2025'
];

async function fixSpecificWindSummaries() {
  try {
    console.log('Starting fix for specific wind generation summaries...');
    
    // Get dates with wind generation data
    console.log('\nChecking for dates with wind generation data...');
    
    // First, check our specific dates of interest
    for (const date of KNOWN_ISSUE_DATES) {
      console.log(`\nChecking wind generation data for ${date}...`);
      const dateDataResult = await db.execute(sql`
        SELECT 
          settlement_date::text as date,
          COUNT(*) as record_count,
          SUM(total_wind) as total_wind
        FROM wind_generation_data
        WHERE settlement_date = ${date}::date
        GROUP BY settlement_date
      `);
      
      if (dateDataResult.length > 0) {
        const row = dateDataResult[0];
        console.log(`Found ${row.record_count} records with total wind: ${row.total_wind} MWh`);
      } else {
        console.log(`No wind generation data found for ${date}`);
      }
    }
    
    // Get the most recent data
    console.log('\nGetting most recent wind generation data...');
    const windDataResult = await db.execute(sql`
      SELECT 
        settlement_date::text as date,
        COUNT(*) as record_count,
        SUM(total_wind) as total_wind
      FROM wind_generation_data
      GROUP BY settlement_date
      ORDER BY settlement_date DESC
      LIMIT 20
    `);
    
    if (Array.isArray(windDataResult) && windDataResult.length > 0) {
      console.log(`\nFound ${windDataResult.length} dates with wind generation data. Recent examples:`);
      for (let i = 0; i < Math.min(5, windDataResult.length); i++) {
        const row = windDataResult[i];
        console.log(`- ${row.date}: ${row.record_count} records, Total: ${row.total_wind} MWh`);
      }
    }
    
    // Fix known issue dates
    console.log('\n1. Processing daily summaries for known issue dates:');
    for (const date of KNOWN_ISSUE_DATES) {
      console.log(`- Processing ${date}...`);
      try {
        await updateDailySummary(date);
        console.log(`  ✅ Successfully updated daily summary for ${date}`);
      } catch (error) {
        console.error(`  ❌ Error updating daily summary for ${date}: ${error}`);
      }
    }
    
    // Fix known issue months
    console.log('\n2. Processing monthly summaries for known issue months:');
    for (const yearMonth of KNOWN_ISSUE_MONTHS) {
      console.log(`- Processing ${yearMonth}...`);
      try {
        await updateMonthlySummary(yearMonth);
        console.log(`  ✅ Successfully updated monthly summary for ${yearMonth}`);
      } catch (error) {
        console.error(`  ❌ Error updating monthly summary for ${yearMonth}: ${error}`);
      }
    }
    
    // Fix years
    console.log('\n3. Processing yearly summaries:');
    for (const year of YEARS_TO_UPDATE) {
      console.log(`- Processing ${year}...`);
      try {
        await updateYearlySummary(year);
        console.log(`  ✅ Successfully updated yearly summary for ${year}`);
      } catch (error) {
        console.error(`  ❌ Error updating yearly summary for ${year}: ${error}`);
      }
    }
    
    console.log('\nWind generation summary fixes completed!');
    process.exit(0);
  } catch (error) {
    console.error(`\nUnexpected error: ${error}`);
    process.exit(1);
  }
}

// Execute the fix
fixSpecificWindSummaries();
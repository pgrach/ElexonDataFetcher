/**
 * Update Daily Summary Wind Data
 * 
 * This script updates the daily_summaries table with wind generation data from the wind_generation_data table.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

async function updateDailySummaryWindData(date: string): Promise<void> {
  console.log(`Updating daily summary wind data for ${date}...`);

  try {
    // Direct SQL approach to update the daily_summaries table
    const result = await db.execute(sql`
      WITH wind_data AS (
        SELECT 
          ${date}::date as summary_date,
          COALESCE(SUM(wind_onshore) / 48, 0) as total_onshore, 
          COALESCE(SUM(wind_offshore) / 48, 0) as total_offshore,
          COALESCE(SUM(total_wind) / 48, 0) as total_wind
        FROM wind_generation_data
        WHERE settlement_date = ${date}::date
        GROUP BY summary_date
      )
      UPDATE daily_summaries
      SET 
        total_wind_generation = wind_data.total_wind,
        wind_onshore_generation = wind_data.total_onshore,
        wind_offshore_generation = wind_data.total_offshore,
        last_updated = NOW()
      FROM wind_data
      WHERE daily_summaries.summary_date = wind_data.summary_date
    `);

    console.log(`Successfully updated daily summary for ${date} with wind generation data`);
    
    // Verify the update
    const updatedData = await db.execute(sql`
      SELECT 
        summary_date, 
        total_wind_generation,
        wind_onshore_generation,
        wind_offshore_generation
      FROM daily_summaries 
      WHERE summary_date = ${date}::date
    `);
    
    if (updatedData[0]) {
      console.log('Updated values:');
      console.log(`  Date: ${updatedData[0].summary_date}`);
      console.log(`  Total wind: ${updatedData[0].total_wind_generation} MW`);
      console.log(`  Onshore: ${updatedData[0].wind_onshore_generation} MW`);
      console.log(`  Offshore: ${updatedData[0].wind_offshore_generation} MW`);
    } else {
      console.error('Failed to retrieve updated data');
    }
  } catch (error) {
    console.error(`Error updating wind data for ${date}:`, error);
    throw error;
  }
}

async function main() {
  try {
    // Update for 2025-04-01
    await updateDailySummaryWindData('2025-04-01');
    
    console.log('Wind data update completed successfully');
    process.exit(0);
  } catch (error) {
    console.error('Error in main function:', error);
    process.exit(1);
  }
}

// Run the main function
main();
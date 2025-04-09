/**
 * Update Daily Summary Wind Data
 * 
 * This script updates the daily_summaries table with wind generation data from the wind_generation_data table.
 */

import { db } from './db';
import { eq, sql } from 'drizzle-orm';
import { dailySummaries } from './db/schema';

async function updateDailySummaryWindData(date: string): Promise<void> {
  console.log(`Updating daily summary wind data for ${date}...`);

  // First, let's check if we have wind data for this date
  const checkResult = await db.execute(sql`
    SELECT COUNT(*) as count
    FROM wind_generation_data
    WHERE settlement_date = ${date}
  `);
  
  console.log('Check result:', checkResult);
  
  if (!checkResult[0] || checkResult[0].count === 0) {
    console.error(`No wind generation data found for ${date}`);
    return;
  }

  // Calculate total wind generation for the date
  const totalWindResult = await db.execute(sql`
    SELECT 
      COALESCE(SUM(wind_onshore), 0) / 48 as onshore_total,
      COALESCE(SUM(wind_offshore), 0) / 48 as offshore_total,
      COALESCE(SUM(total_wind), 0) / 48 as total
    FROM wind_generation_data
    WHERE settlement_date = ${date}
  `);

  console.log('Query result:', totalWindResult);

  if (!totalWindResult[0]) {
    console.error(`No results from wind generation query for ${date}`);
    return;
  }

  const onshore_total = Number(totalWindResult[0].onshore_total || 0);
  const offshore_total = Number(totalWindResult[0].offshore_total || 0);
  const total = Number(totalWindResult[0].total || 0);

  console.log(`Wind generation data for ${date}:`);
  console.log(`  Total wind: ${total.toFixed(2)} MW`);
  console.log(`  Onshore: ${onshore_total.toFixed(2)} MW`);
  console.log(`  Offshore: ${offshore_total.toFixed(2)} MW`);

  // Update the daily summary
  // Use raw SQL to handle the data types correctly
  await db.execute(sql`
    UPDATE daily_summaries
    SET 
      total_wind_generation = ${total},
      wind_onshore_generation = ${onshore_total},
      wind_offshore_generation = ${offshore_total},
      last_updated = NOW()
    WHERE summary_date = ${date}
  `);

  console.log(`Successfully updated daily summary for ${date} with wind generation data`);
}

async function main() {
  try {
    // Update for 2025-04-01
    await updateDailySummaryWindData('2025-04-01');
    
    console.log('Wind data update completed successfully');
  } catch (error) {
    console.error('Error updating wind data:', error);
    process.exit(1);
  } finally {
    process.exit(0);
  }
}

// Run the main function
main();
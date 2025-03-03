/**
 * Materialized View Population Script
 * 
 * This script populates the materialized view tables with data from existing
 * historical_bitcoin_calculations and curtailment_records data.
 * 
 * It serves as an initial population script after the database schema has been updated.
 */

import { db } from "./db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations 
} from "./db/schema";
import { format, subDays } from "date-fns";
import { sql, and, eq } from "drizzle-orm";
import { refreshMaterializedViews } from "./server/services/miningPotentialService";

const DEFAULT_DAYS_TO_POPULATE = 30;

/**
 * Populate materialized views for recent dates
 */
async function populateRecentMaterializedViews(days: number = DEFAULT_DAYS_TO_POPULATE) {
  console.log(`Populating materialized views for the last ${days} days...`);
  
  try {
    // Get unique dates from curtailment records
    const result = await db
      .select({
        dates: sql<string[]>`ARRAY_AGG(DISTINCT settlement_date::text ORDER BY settlement_date DESC)`
      })
      .from(curtailmentRecords)
      .where(
        sql`settlement_date >= CURRENT_DATE - INTERVAL '${days} days'`
      );
    
    const dates = result[0]?.dates || [];
    
    if (dates.length === 0) {
      console.log('No recent dates found with curtailment records');
      return;
    }
    
    console.log(`Found ${dates.length} dates to process: ${dates.slice(0, 5).join(', ')}${dates.length > 5 ? '...' : ''}`);
    
    // Process each date
    for (const date of dates) {
      console.log(`Processing date: ${date}`);
      await refreshMaterializedViews(date);
    }
    
    console.log('Materialized view population completed successfully');
  } catch (error) {
    console.error('Error during materialized view population:', error);
  }
}

/**
 * Populate materialized views for a specific date range
 */
async function populateDateRangeMaterializedViews(startDate: string, endDate: string) {
  console.log(`Populating materialized views from ${startDate} to ${endDate}...`);
  
  try {
    // Get unique dates from curtailment records in the specified range
    const result = await db
      .select({
        dates: sql<string[]>`ARRAY_AGG(DISTINCT settlement_date::text ORDER BY settlement_date)`
      })
      .from(curtailmentRecords)
      .where(
        and(
          sql`settlement_date >= ${startDate}::date`,
          sql`settlement_date <= ${endDate}::date`
        )
      );
    
    const dates = result[0]?.dates || [];
    
    if (dates.length === 0) {
      console.log(`No curtailment records found between ${startDate} and ${endDate}`);
      return;
    }
    
    console.log(`Found ${dates.length} dates to process: ${dates.slice(0, 5).join(', ')}${dates.length > 5 ? '...' : ''}`);
    
    // Process each date
    for (const date of dates) {
      console.log(`Processing date: ${date}`);
      await refreshMaterializedViews(date);
    }
    
    console.log('Materialized view population completed successfully');
  } catch (error) {
    console.error('Error during materialized view population:', error);
  }
}

// Main execution based on command line arguments
async function main() {
  const args = process.argv.slice(2);
  
  if (args.length === 0 || args[0] === 'recent') {
    // Default to recent data
    const days = args[1] ? parseInt(args[1], 10) : DEFAULT_DAYS_TO_POPULATE;
    await populateRecentMaterializedViews(days);
  } else if (args[0] === 'range' && args.length >= 3) {
    // Process a specific date range
    await populateDateRangeMaterializedViews(args[1], args[2]);
  } else {
    console.log(`
Usage:
  npx tsx populate_materialized_views.ts [command] [options]
  
Commands:
  recent [days]       - Populate views for recent days (default: ${DEFAULT_DAYS_TO_POPULATE} days)
  range START END     - Populate views for a specific date range (format: YYYY-MM-DD)
    `);
  }
  
  // Exit after completion
  process.exit(0);
}

main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
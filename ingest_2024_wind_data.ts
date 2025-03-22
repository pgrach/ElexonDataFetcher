/**
 * Ingest Wind Generation Data for 2024
 * 
 * This script retrieves and processes all wind generation data for 2024 from Elexon's B1630 API
 * and stores it in the wind_generation_data table. It processes the data month by month to stay
 * within API limits and provides detailed logging.
 */

import { processDateRange } from './server/services/windGenerationService';
import { info, error } from './server/utils/logger';
import { format, addMonths } from 'date-fns';
import { db } from './db';
import { sql } from 'drizzle-orm';

const START_DATE = '2024-01-01';
const END_DATE = '2024-12-31';

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Process all months in 2024 sequentially
 */
async function ingest2024Data() {
  try {
    info('Starting ingestion of wind generation data for 2024', { module: 'ingest2024Data' });
    
    // First, check if we already have data for 2024
    const existingCount = await db.execute(
      sql`SELECT COUNT(*) as count FROM wind_generation_data WHERE settlement_date >= '2024-01-01' AND settlement_date <= '2024-12-31'`
    );
    
    const count = Number(existingCount.rows[0]?.count || 0);
    info(`Found ${count} existing wind generation records for 2024`, { module: 'ingest2024Data' });
    
    if (count > 0) {
      const proceed = process.argv.includes('--force');
      if (!proceed) {
        info('Data for 2024 already exists. Run with --force to reprocess. Exiting.', { module: 'ingest2024Data' });
        return;
      }
      info('Force flag detected. Continuing with data ingestion despite existing records.', { module: 'ingest2024Data' });
    }
    
    // Process each month separately to stay within API limits
    let currentDate = new Date(START_DATE);
    const endDate = new Date(END_DATE);
    
    let monthCount = 0;
    
    while (currentDate <= endDate) {
      const monthStart = format(currentDate, 'yyyy-MM-dd');
      const nextMonth = addMonths(currentDate, 1);
      const lastDayOfMonth = new Date(nextMonth);
      lastDayOfMonth.setDate(0); // Set to last day of current month
      
      const monthEnd = format(lastDayOfMonth, 'yyyy-MM-dd');
      
      info(`Processing month ${monthCount + 1}/12: ${monthStart} to ${monthEnd}`, { module: 'ingest2024Data' });
      
      try {
        // Process this month
        const recordCount = await processDateRange(monthStart, monthEnd);
        info(`Successfully processed ${recordCount} records for ${format(currentDate, 'MMMM yyyy')}`, { module: 'ingest2024Data' });
        
        // Add a delay between API calls to avoid rate limiting
        await delay(2000);
      } catch (err) {
        error(`Failed to process month ${format(currentDate, 'MMMM yyyy')}: ${err.message}`, { module: 'ingest2024Data' });
        // Continue with next month despite errors
      }
      
      // Move to next month
      currentDate = nextMonth;
      monthCount++;
    }
    
    // Verify the ingestion
    const finalCount = await db.execute(
      sql`SELECT COUNT(*) as count FROM wind_generation_data WHERE settlement_date >= '2024-01-01' AND settlement_date <= '2024-12-31'`
    );
    
    const recordCount = Number(finalCount.rows[0]?.count || 0);
    info(`Completed ingestion of wind generation data for 2024. Total records: ${recordCount}`, { module: 'ingest2024Data' });
    
  } catch (err) {
    error(`Error during 2024 data ingestion: ${err.message}`, { module: 'ingest2024Data' });
    process.exit(1);
  }
}

// Run the ingestion
ingest2024Data()
  .then(() => {
    info('Ingestion process completed successfully.', { module: 'ingest2024Data' });
    process.exit(0);
  })
  .catch((err) => {
    error(`Ingestion process failed with error: ${err.message}`, { module: 'ingest2024Data' });
    process.exit(1);
  });
/**
 * Ingest Wind Generation Data for 2024
 * 
 * This script retrieves and processes all wind generation data for 2024 from Elexon's B1630 API
 * and stores it in the wind_generation_data table. It processes the data month by month to stay
 * within API limits and provides detailed logging.
 * 
 * Usage:
 *   npx tsx ingest_2024_wind_data.ts [--force] [--start=MM] [--end=MM]
 *   
 * Options:
 *   --force        Process even if data exists
 *   --start=MM     Start processing from month MM (1-12)
 *   --end=MM       End processing at month MM (1-12)
 */

import { processDateRange } from './server/services/windGenerationService';
import { info, error } from './server/utils/logger';
import { format, addMonths, parse } from 'date-fns';
import { db } from './db';
import { sql } from 'drizzle-orm';

const DEFAULT_START_DATE = '2024-01-01';
const DEFAULT_END_DATE = '2024-12-31';

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Process all months in 2024 sequentially
 */
async function ingest2024Data() {
  try {
    info('Starting ingestion of wind generation data for 2024', { module: 'ingest2024Data' });
    
    // Parse command line arguments
    const startMonthArg = process.argv.find(arg => arg.startsWith('--start='));
    const endMonthArg = process.argv.find(arg => arg.startsWith('--end='));
    
    let startMonth = 1;
    let endMonth = 12;
    
    if (startMonthArg) {
      const month = parseInt(startMonthArg.split('=')[1]);
      if (month >= 1 && month <= 12) {
        startMonth = month;
        info(`Starting from month ${startMonth} (${new Date(2024, startMonth-1, 1).toLocaleString('default', { month: 'long' })})`, { module: 'ingest2024Data' });
      }
    }
    
    if (endMonthArg) {
      const month = parseInt(endMonthArg.split('=')[1]);
      if (month >= 1 && month <= 12) {
        endMonth = month;
        info(`Ending at month ${endMonth} (${new Date(2024, endMonth-1, 1).toLocaleString('default', { month: 'long' })})`, { module: 'ingest2024Data' });
      }
    }
    
    // Start date with specified month
    const START_DATE = startMonth === 1 ? DEFAULT_START_DATE : `2024-${startMonth.toString().padStart(2, '0')}-01`;
    
    // End date with specified month
    const lastDayOfEndMonth = new Date(2024, endMonth, 0);
    const END_DATE = endMonth === 12 ? DEFAULT_END_DATE : format(lastDayOfEndMonth, 'yyyy-MM-dd');
    
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
    let totalMonths = endMonth - startMonth + 1;
    
    while (currentDate <= endDate) {
      const monthStart = format(currentDate, 'yyyy-MM-dd');
      const nextMonth = addMonths(currentDate, 1);
      const lastDayOfMonth = new Date(nextMonth);
      lastDayOfMonth.setDate(0); // Set to last day of current month
      
      const monthEnd = format(lastDayOfMonth, 'yyyy-MM-dd');
      
      info(`Processing month ${monthCount + 1}/${totalMonths}: ${monthStart} to ${monthEnd}`, { module: 'ingest2024Data' });
      
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
      sql`SELECT COUNT(*) as count FROM wind_generation_data WHERE settlement_date >= '${START_DATE}' AND settlement_date <= '${END_DATE}'`
    );
    
    const recordCount = Number(finalCount.rows[0]?.count || 0);
    info(`Completed ingestion of wind generation data for specified period. Total records: ${recordCount}`, { module: 'ingest2024Data' });
    
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
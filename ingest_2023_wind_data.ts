/**
 * Ingest Wind Generation Data for 2023
 * 
 * This script retrieves and processes all wind generation data for 2023 from Elexon's B1630 API
 * and stores it in the wind_generation_data table. It processes the data month by month to stay
 * within API limits and provides detailed logging.
 * 
 * Usage:
 *   npx tsx ingest_2023_wind_data.ts [--force] [--start=MM] [--end=MM]
 *   
 * Options:
 *   --force        Process even if data exists
 *   --start=MM     Start processing from month MM (1-12)
 *   --end=MM       End processing at month MM (1-12)
 */

import * as fs from 'fs';
import { db } from './db';
import { sql } from 'drizzle-orm';
import { processDateRange } from './server/services/windGenerationService';
import { logger, LogLevel } from './server/utils/logger';

const log = (message: string, level: 'info' | 'error' = 'info') => {
  logger.log(message, { 
    level: level === 'info' ? LogLevel.INFO : LogLevel.ERROR, 
    module: 'ingest2023Data' 
  });
  console.log(`[${level.toUpperCase()}] ${message}`);
};

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Process all months in 2023 sequentially
 */
async function ingest2023Data() {
  try {
    log('Starting ingestion of wind generation data for 2023');

    // Parse command line arguments
    const args = process.argv.slice(2);
    const forceProcess = args.includes('--force');
    
    const startMonthArg = args.find(arg => arg.startsWith('--start='));
    const endMonthArg = args.find(arg => arg.startsWith('--end='));
    
    const startMonth = startMonthArg ? parseInt(startMonthArg.split('=')[1], 10) : 1;
    const endMonth = endMonthArg ? parseInt(endMonthArg.split('=')[1], 10) : 12;
    
    log(`Starting from month ${startMonth} (${new Date(2023, startMonth - 1, 1).toLocaleString('en-US', { month: 'long' })})`);
    log(`Ending at month ${endMonth} (${new Date(2023, endMonth - 1, 1).toLocaleString('en-US', { month: 'long' })})`);

    // Check if we already have data for 2023
    const existingRecords = await db.execute(sql`
      SELECT COUNT(*) as count
      FROM wind_generation_data
      WHERE EXTRACT(YEAR FROM settlement_date) = 2023
    `);

    const recordCount = existingRecords[0]?.count ? parseInt(existingRecords[0].count as string, 10) : 0;
    log(`Found ${recordCount} existing wind generation records for 2023`);

    if (recordCount > 0 && !forceProcess) {
      log('Data for 2023 already exists. Use --force to overwrite.');
      return;
    }

    if (recordCount > 0 && forceProcess) {
      log('Force flag detected. Continuing with data ingestion despite existing records.');
    }

    // Process each month
    for (let month = startMonth; month <= endMonth; month++) {
      const daysInMonth = new Date(2023, month, 0).getDate();
      
      const startDate = `2023-${month.toString().padStart(2, '0')}-01`;
      const endDate = `2023-${month.toString().padStart(2, '0')}-${daysInMonth.toString().padStart(2, '0')}`;
      
      log(`Processing month ${month - startMonth + 1}/${endMonth - startMonth + 1}: ${startDate} to ${endDate}`);
      
      const recordsInserted = await processDateRange(startDate, endDate);
      
      const monthName = new Date(2023, month - 1, 1).toLocaleString('en-US', { month: 'long' });
      log(`Successfully processed ${recordsInserted} records for ${monthName} 2023`);
      
      // Add a delay between months to avoid rate limiting
      if (month < endMonth) {
        await delay(2000);
      }
    }

    log('Wind generation data ingestion for 2023 completed successfully');
  } catch (error) {
    log(`Error during 2023 data ingestion: ${(error as Error).message}`, 'error');
  }
}

// Execute the ingestion process
ingest2023Data();
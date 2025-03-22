/**
 * Wind Generation Summaries Update Script
 * 
 * This script recalculates all wind generation summary data (daily, monthly, yearly)
 * from the wind_generation_data table to ensure consistent values across all tables.
 * 
 * Usage:
 *   npx tsx update_wind_summaries.ts
 */

import { recalculateAllSummaries } from './server/services/windSummaryService';
import { logger } from './server/utils/logger';

async function updateWindSummaries() {
  try {
    console.log('Starting wind generation summary recalculation...');
    console.log('This process will update all daily, monthly, and yearly summaries');
    console.log('based on data in the wind_generation_data table.');
    console.log('It may take several minutes to complete.');
    
    await recalculateAllSummaries();
    
    console.log('Wind generation summary recalculation completed successfully!');
    process.exit(0);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.error(`Error recalculating wind summaries: ${errorMessage}`);
    process.exit(1);
  }
}

// Execute the update
updateWindSummaries();
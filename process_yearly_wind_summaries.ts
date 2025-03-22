/**
 * Process Yearly Wind Generation Summaries
 * 
 * This script updates yearly summaries for a specific year
 * to incorporate wind generation data.
 * 
 * Usage:
 *   npx tsx process_yearly_wind_summaries.ts <YYYY>
 */

import { updateYearlySummary } from './server/services/windSummaryService';
import { logger } from './server/utils/logger';

const targetYear = process.argv[2] || '2022';

async function processYearSummary() {
  try {
    logger.info(`Processing wind generation summary for year: ${targetYear}`, {
      module: 'process_yearly_wind_summaries'
    });

    await updateYearlySummary(targetYear);

    logger.info(`Successfully updated wind generation summary for year: ${targetYear}`, {
      module: 'process_yearly_wind_summaries' 
    });
    process.exit(0);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Error processing wind generation summary: ${errorMessage}`, {
      module: 'process_yearly_wind_summaries'
    });
    process.exit(1);
  }
}

// Run the script
processYearSummary();
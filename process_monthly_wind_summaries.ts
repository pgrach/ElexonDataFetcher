/**
 * Process Monthly Wind Generation Summaries
 * 
 * This script updates monthly summaries for a specific month
 * to incorporate wind generation data.
 * 
 * Usage:
 *   npx tsx process_monthly_wind_summaries.ts <YYYY-MM>
 */

import { updateMonthlySummary } from './server/services/windSummaryService';
import { logger } from './server/utils/logger';

const targetMonth = process.argv[2] || '2022-01';

async function processMonthSummary() {
  try {
    logger.info(`Processing wind generation summary for month: ${targetMonth}`, {
      module: 'process_monthly_wind_summaries'
    });

    await updateMonthlySummary(targetMonth);

    logger.info(`Successfully updated wind generation summary for month: ${targetMonth}`, {
      module: 'process_monthly_wind_summaries' 
    });
    process.exit(0);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Error processing wind generation summary: ${errorMessage}`, {
      module: 'process_monthly_wind_summaries'
    });
    process.exit(1);
  }
}

// Run the script
processMonthSummary();
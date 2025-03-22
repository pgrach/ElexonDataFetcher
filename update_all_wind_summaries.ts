/**
 * Update All Wind Generation Summaries
 * 
 * This script recalculates all wind generation summaries using the fixed query functions
 * that properly cast date parameters for accurate data retrieval.
 * 
 * Usage:
 *   npx tsx update_all_wind_summaries.ts
 */

import { recalculateAllSummaries } from './server/services/windSummaryService';
import { logger } from './server/utils/logger';

async function updateAllSummaries() {
  try {
    logger.info('Starting wind generation summary update process', {
      module: 'update_all_wind_summaries'
    });

    // Run the recalculation
    await recalculateAllSummaries();

    logger.info('Successfully updated all wind generation summaries', {
      module: 'update_all_wind_summaries'
    });
    process.exit(0);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Error updating wind generation summaries: ${errorMessage}`, {
      module: 'update_all_wind_summaries'
    });
    process.exit(1);
  }
}

// Run the update
updateAllSummaries();
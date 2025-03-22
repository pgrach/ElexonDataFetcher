/**
 * Update Yearly Wind Generation Summaries
 * 
 * This script updates the yearly wind generation summaries for 2022-2025
 * to ensure all historical data is properly reflected in the summary tables.
 * 
 * Usage:
 *   npx tsx update_yearly_wind_summaries.ts
 */

import { updateYearlySummary } from './server/services/windSummaryService';
import { logger } from './server/utils/logger';

// Define the years to process
const years = ['2022', '2023', '2024', '2025'];

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function updateHistoricalYearSummaries() {
  try {
    logger.info('Starting update of yearly wind generation summaries', {
      module: 'update_yearly_wind_summaries'
    });

    // Process yearly summaries
    for (const year of years) {
      try {
        logger.info(`Processing yearly summary for ${year}`, {
          module: 'update_yearly_wind_summaries'
        });
        
        await updateYearlySummary(year);
        
        // Add a small delay to prevent database overload
        await sleep(500);
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.warning(`Error processing yearly summary for ${year}: ${errorMessage}`, {
          module: 'update_yearly_wind_summaries'
        });
        // Continue with next year despite errors
      }
    }

    logger.info('Successfully completed update of yearly wind generation summaries', {
      module: 'update_yearly_wind_summaries'
    });
    process.exit(0);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Fatal error in update process: ${errorMessage}`, {
      module: 'update_yearly_wind_summaries'
    });
    process.exit(1);
  }
}

// Run the script
updateHistoricalYearSummaries();
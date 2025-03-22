/**
 * Historical Wind Generation Summaries Update Script
 * 
 * This script updates the wind generation summaries for 2022, 2023, and 2024
 * to ensure all historical data is properly reflected in the summary tables.
 * 
 * Usage:
 *   npx tsx update_historical_wind_summaries.ts
 */

import { 
  updateMonthlySummary, 
  updateYearlySummary 
} from './server/services/windSummaryService';
import { logger } from './server/utils/logger';

// Define the years and months to process
const years = ['2022', '2023', '2024', '2025'];
const months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'];

async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function updateHistoricalWindSummaries() {
  try {
    logger.info('Starting update of historical wind generation summaries', {
      module: 'update_historical_wind_summaries'
    });

    // First, process all monthly summaries
    for (const year of years) {
      for (const month of months) {
        const yearMonth = `${year}-${month}`;
        try {
          logger.info(`Processing monthly summary for ${yearMonth}`, {
            module: 'update_historical_wind_summaries'
          });
          
          await updateMonthlySummary(yearMonth);
          
          // Add a small delay to prevent database overload
          await sleep(500);
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          logger.warning(`Error processing monthly summary for ${yearMonth}: ${errorMessage}`, {
            module: 'update_historical_wind_summaries'
          });
          // Continue with next month despite errors
        }
      }
    }

    // Then process yearly summaries
    for (const year of years) {
      try {
        logger.info(`Processing yearly summary for ${year}`, {
          module: 'update_historical_wind_summaries'
        });
        
        await updateYearlySummary(year);
        
        // Add a small delay to prevent database overload
        await sleep(500);
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        logger.warning(`Error processing yearly summary for ${year}: ${errorMessage}`, {
          module: 'update_historical_wind_summaries'
        });
        // Continue with next year despite errors
      }
    }

    logger.info('Successfully completed update of historical wind generation summaries', {
      module: 'update_historical_wind_summaries'
    });
    process.exit(0);
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Fatal error in update process: ${errorMessage}`, {
      module: 'update_historical_wind_summaries'
    });
    process.exit(1);
  }
}

// Run the script
updateHistoricalWindSummaries();
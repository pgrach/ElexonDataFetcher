/**
 * Process Single Period Script
 * 
 * This script processes a single period for March 28, 2025 using the 
 * optimized critical date processor. It takes a command line argument
 * for the period number.
 * 
 * Usage:
 *   npx tsx process_single_period.ts <period_number>
 * 
 * Examples:
 *   npx tsx process_single_period.ts 11
 *   npx tsx process_single_period.ts 25
 *   npx tsx process_single_period.ts 37
 */

import { processDate } from './optimized_critical_date_processor';

const TARGET_DATE = '2025-03-28';
const DEFAULT_PERIOD = 11;

// Get period from command line, default to 11 if none provided
const periodArg = process.argv[2];
const PERIOD = periodArg ? parseInt(periodArg, 10) : DEFAULT_PERIOD;

if (isNaN(PERIOD) || PERIOD < 1 || PERIOD > 48) {
  console.error('Invalid period number. Must be between 1 and 48.');
  process.exit(1);
}

/**
 * Process a single period
 */
async function processSinglePeriod(): Promise<void> {
  console.log(`\n=== Processing Period ${PERIOD} for ${TARGET_DATE} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    await processDate(TARGET_DATE, PERIOD, PERIOD);
    console.log(`\n=== Completed processing period ${PERIOD} at ${new Date().toISOString()} ===`);
  } catch (error) {
    console.error(`Error processing period ${PERIOD}:`, error);
    process.exit(1);
  }
}

// Execute the main function
processSinglePeriod().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
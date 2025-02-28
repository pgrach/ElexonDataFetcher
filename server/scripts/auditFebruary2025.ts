/**
 * This script audits Bitcoin calculation data for February 2025.
 * It leverages the centralized reconciliation functions from historicalReconciliation service.
 */
import { format, eachDayOfInterval, parseISO } from 'date-fns';
import { 
  reconcileDateRange,
  auditAndFixBitcoinCalculations
} from "../services/historicalReconciliation";

// Configuration
const START_DATE = '2025-02-01';
const END_DATE = '2025-02-28';
const BATCH_SIZE = 3;
const API_RATE_LIMIT = 250;

/**
 * Delay execution for the specified milliseconds
 */
async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Audit Bitcoin calculations for February 2025
 */
async function auditBitcoinCalculations() {
  try {
    console.log(`\n=== Starting February 2025 Bitcoin Calculations Audit ===\n`);
    console.log(`Date Range: ${START_DATE} to ${END_DATE}\n`);

    // Option 1: Process the entire date range at once (faster but less detailed)
    // This uses the consolidated reconcileDateRange function
    console.log('Processing entire date range...');
    const rangeResults = await reconcileDateRange(START_DATE, END_DATE);
    
    console.log('\n=== Range Processing Results ===');
    console.log(`Total days processed: ${rangeResults.processedDates}`);
    console.log(`Days updated: ${rangeResults.updatedDates}`);
    console.log(`Days with errors: ${rangeResults.errors.length}`);
    
    if (rangeResults.errors.length > 0) {
      console.log('\nDetailed analysis of dates with errors:');
    }

    // Option 2: Process dates with errors individually
    // This gives more detailed information about what failed
    const dates = eachDayOfInterval({
      start: parseISO(START_DATE),
      end: parseISO(END_DATE)
    }).map(date => format(date, 'yyyy-MM-dd'));

    let updatedDates: string[] = [];
    let errorDates: string[] = [];

    // Process dates that had errors during range processing
    const datesToCheck = rangeResults.errors.length > 0 
      ? rangeResults.errors.map(e => e.date)
      : [];
      
    if (datesToCheck.length > 0) {
      console.log(`\n=== Detailed Processing of ${datesToCheck.length} Problematic Dates ===\n`);

      // Process dates in smaller batches
      for (let i = 0; i < datesToCheck.length; i += BATCH_SIZE) {
        const batchDates = datesToCheck.slice(i, i + BATCH_SIZE);

        // Process each date in the batch
        const results = await Promise.all(
          batchDates.map(async (date) => {
            try {
              console.log(`\n- Auditing ${date}...`);
              
              // Use the consolidated audit and fix function
              const result = await auditAndFixBitcoinCalculations(date);
              
              if (result.success) {
                if (result.fixed) {
                  console.log(`[${date}] ✓ Fixed: ${result.message}`);
                  updatedDates.push(date);
                  return { date, status: 'fixed' };
                } else {
                  console.log(`[${date}] ✓ Already complete: ${result.message}`);
                  return { date, status: 'complete' };
                }
              } else {
                console.log(`[${date}] × Failed: ${result.message}`);
                errorDates.push(date);
                return { date, status: 'error' };
              }
            } catch (error) {
              console.error(`× Error processing ${date}:`, error);
              errorDates.push(date);
              return { date, status: 'error' };
            }
          })
        );

        // Print progress
        const progress = ((i + batchDates.length) / datesToCheck.length * 100).toFixed(1);
        console.log(`\nProgress: ${progress}% (${i + batchDates.length}/${datesToCheck.length} days)`);

        // Add delay between batches
        if (i + BATCH_SIZE < datesToCheck.length) {
          await delay(API_RATE_LIMIT);
        }
      }
    }

    // Print final summary
    console.log('\n=== Final Audit Summary ===');
    console.log(`Total days in February 2025: ${dates.length}`);
    console.log(`Days with complete calculations: ${dates.length - updatedDates.length - errorDates.length}`);
    console.log(`Days updated/fixed: ${updatedDates.length}`);
    console.log(`Days with persistent errors: ${errorDates.length}`);

    if (updatedDates.length > 0) {
      console.log('\nFixed dates:', updatedDates.join(', '));
    }
    if (errorDates.length > 0) {
      console.log('\nPersistent error dates:', errorDates.join(', '));
    }

    return {
      totalDays: dates.length,
      completeDays: dates.length - updatedDates.length - errorDates.length,
      updatedDays: updatedDates.length, 
      errorDays: errorDates.length,
      updatedDates,
      errorDates
    };
  } catch (error) {
    console.error('Error during Bitcoin calculations audit:', error);
    throw error;
  }
}

// Run the audit if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  auditBitcoinCalculations()
    .then(results => {
      console.log('\n=== Audit Complete ===');
      process.exit(0);
    })
    .catch(error => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

export { auditBitcoinCalculations };
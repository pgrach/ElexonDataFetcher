/**
 * This script processes missing or incomplete Bitcoin calculations.
 * It uses the centralized reconciliation functions from historicalReconciliation service.
 */
import { 
  findMissingDates, 
  auditAndFixBitcoinCalculations,
  reconcileDateRange
} from '../services/historicalReconciliation';
import { fetch2024Difficulties } from '../services/bitcoinService';
import { format, parse, addDays, subDays } from 'date-fns';

// Settings
const START_DATE = '2024-01-01';
const END_DATE = format(new Date(), 'yyyy-MM-dd');  // Today's date
const BATCH_SIZE = 5; // Process this many dates in parallel
const MAX_DATES = 0;  // 0 means process all dates

async function processMissingDates() {
  try {
    console.log('\n=== Processing Missing Historical Bitcoin Data ===');
    console.log(`Date Range: ${START_DATE} to ${END_DATE}\n`);

    // Pre-fetch difficulties
    console.log('Pre-fetching difficulties...');
    await fetch2024Difficulties();
    console.log('Difficulties pre-fetch complete\n');

    // Get missing dates using the consolidated function
    console.log('Finding dates with missing or incomplete calculations...');
    const missingData = await findMissingDates(START_DATE, END_DATE);
    console.log(`Found ${missingData.length} dates with missing or incomplete data\n`);

    if (missingData.length === 0) {
      console.log('✓ No missing data found, all dates are complete');
      return;
    }

    // Limit the number of dates to process if needed
    const datesToProcess = MAX_DATES > 0 
      ? missingData.slice(0, MAX_DATES) 
      : missingData;
    
    console.log(`Will process ${datesToProcess.length} dates\n`);

    // Process dates in batches using the reconciliation functions
    let processed = 0;
    const results = {
      success: [] as string[],
      failed: [] as { date: string; error: string }[]
    };

    // Process dates in smaller batches for better visibility
    for (let i = 0; i < datesToProcess.length; i += BATCH_SIZE) {
      const batch = datesToProcess.slice(i, i + BATCH_SIZE);
      
      console.log(`\n=== Processing Batch ${Math.floor(i/BATCH_SIZE) + 1}/${Math.ceil(datesToProcess.length/BATCH_SIZE)} ===`);
      
      // Process each date in the batch with detailed information
      const batchResults = await Promise.allSettled(
        batch.map(async (dateInfo: any) => {
          // Cast all properties to appropriate types to satisfy TypeScript
          const date = String(dateInfo.date);
          const missingModels = Array.isArray(dateInfo.missingModels) 
            ? dateInfo.missingModels.map(String) 
            : [];
          const requiredPeriodCount = Number(dateInfo.requiredPeriodCount) || 0;
          const minCalculatedPeriods = Number(dateInfo.minCalculatedPeriods) || 0;
          
          console.log(`\n- Processing Date: ${date}`);
          console.log(`  Status: ${3 - missingModels.length}/3 miner models, ` +
                     `${minCalculatedPeriods}/${requiredPeriodCount} periods processed`);
          console.log(`  Missing models: ${missingModels.join(', ')}`);
          
          // Use the consolidated audit and fix function
          const result = await auditAndFixBitcoinCalculations(date);
          
          console.log(`  Result: ${result.success ? '✓' : '×'} ${result.message}`);
          return { date, result };
        })
      );
      
      // Track results
      batchResults.forEach((result, index) => {
        if (result.status === 'fulfilled') {
          const dateInfo = result.value as { 
            date: string; 
            result: { 
              success: boolean; 
              message: string;
            }
          };
          
          if (dateInfo.result.success) {
            results.success.push(dateInfo.date);
          } else {
            results.failed.push({
              date: dateInfo.date,
              error: dateInfo.result.message
            });
          }
        } else {
          // If the promise was rejected, use the date from the original batch
          const dateInfo = batch[index] as any;
          const date = String(dateInfo.date);
          
          results.failed.push({
            date,
            error: String(result.reason)
          });
        }
      });
      
      // Update progress
      processed += batch.length;
      const progress = ((processed / datesToProcess.length) * 100).toFixed(1);
      console.log(`\nOverall Progress: ${progress}% (${processed}/${datesToProcess.length})`);
      console.log(`Success: ${results.success.length}, Failed: ${results.failed.length}`);
    }

    console.log('\n=== Missing Data Processing Complete ===');
    console.log(`Total dates processed: ${processed}`);
    console.log(`Successfully processed: ${results.success.length}`);
    console.log(`Failed processing: ${results.failed.length}`);

    if (results.failed.length > 0) {
      console.log('\nFailed Dates:');
      results.failed.forEach(f => console.log(`- ${f.date}: ${f.error}`));
    }

  } catch (error) {
    console.error('Error processing missing dates:', error);
    process.exit(1);
  }
}

// Start processing
processMissingDates()
  .then(() => {
    console.log('Script completed successfully');
    process.exit(0);
  })
  .catch(error => {
    console.error('Script failed:', error);
    process.exit(1);
  });
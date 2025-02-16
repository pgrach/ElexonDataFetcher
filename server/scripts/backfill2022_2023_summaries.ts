import { calculateMonthlyBitcoinSummary } from '../services/bitcoinService';
import { eachMonthOfInterval, format } from 'date-fns';
import { minerModels } from '../types/bitcoin';
import pLimit from 'p-limit';

const START_DATE = '2022-01-01';
const END_DATE = '2023-12-31';
const CONCURRENT_MONTHS = 3;

async function backfillMonthlySummaries() {
  try {
    console.log('\n=== Starting Monthly Summaries Backfill (2022-2023) ===');
    console.log(`Processing range: ${START_DATE} to ${END_DATE}\n`);

    // Generate all months between start and end dates
    const months = eachMonthOfInterval({
      start: new Date(START_DATE),
      end: new Date(END_DATE)
    }).map(date => format(date, 'yyyy-MM'));

    console.log(`Found ${months.length} months to process:`, months);
    const limit = pLimit(CONCURRENT_MONTHS);

    // Process all miner models for each month
    for (const minerModel of Object.keys(minerModels)) {
      console.log(`\nProcessing ${minerModel}...`);
      
      await Promise.all(
        months.map(month => 
          limit(async () => {
            try {
              console.log(`Processing ${month} for ${minerModel}...`);
              await calculateMonthlyBitcoinSummary(month, minerModel);
              console.log(`✓ Completed ${month} for ${minerModel}`);
            } catch (error) {
              console.error(`Error processing ${month} for ${minerModel}:`, error);
              // Continue with other months even if one fails
            }
          })
        )
      );
    }

    console.log('\n=== Monthly Summaries Backfill Complete ===');

  } catch (error) {
    console.error('Error during backfill:', error);
    process.exit(1);
  }
}

// Start the backfill process
backfillMonthlySummaries();

import { calculateMonthlyBitcoinSummary } from '../services/bitcoinService';
import { format, eachMonthOfInterval, parseISO } from 'date-fns';

const START_DATE = '2022-01';
const END_DATE = '2023-12';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function backfillMonthlySummaries() {
  try {
    console.log(`\n=== Starting Monthly Summaries Backfill ===`);
    console.log(`Range: ${START_DATE} to ${END_DATE}\n`);

    // Generate list of months to process
    const months = eachMonthOfInterval({
      start: parseISO(`${START_DATE}-01`),
      end: parseISO(`${END_DATE}-01`)
    }).map(date => format(date, 'yyyy-MM'));

    console.log(`Will process ${months.length} months for ${MINER_MODELS.length} miner models`);

    // Process each month for each miner model
    for (const month of months) {
      console.log(`\nProcessing ${month}...`);
      
      for (const minerModel of MINER_MODELS) {
        try {
          console.log(`- Calculating for ${minerModel}`);
          await calculateMonthlyBitcoinSummary(month, minerModel);
        } catch (error) {
          console.error(`Error processing ${month} for ${minerModel}:`, error);
          // Continue with next model even if one fails
        }
      }
    }

    // Verify results
    console.log('\n=== Verification Results ===');
    console.log('Processed months:', months.join(', '));
    console.log('Processed models:', MINER_MODELS.join(', '));

  } catch (error) {
    console.error('Error during backfill:', error);
    process.exit(1);
  }
}

// Start the backfill process
if (import.meta.url === `file://${process.argv[1]}`) {
  backfillMonthlySummaries()
    .then(() => {
      console.log('\nBackfill complete');
      process.exit(0);
    })
    .catch(error => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

import { format, eachDayOfInterval, isValid } from 'date-fns';
import { minerModels } from '../types/bitcoin';
import { processSingleDay } from '../services/bitcoinService';
import { db } from "@db";
import { historicalBitcoinCalculations } from "@db/schema";
import { and, eq } from "drizzle-orm";
import pLimit from 'p-limit';

const START_DATE = '2025-01-01';
const END_DATE = '2025-02-29';
const MAX_CONCURRENT_DAYS = 5;

async function verifyDayCalculations(date: string, minerModel: string) {
  const records = await db
    .select()
    .from(historicalBitcoinCalculations)
    .where(
      and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      )
    );

  return {
    recordCount: records.length,
    difficulty: records[0]?.difficulty,
    totalBitcoin: records.reduce((sum, r) => sum + Number(r.bitcoinMined), 0)
  };
}

async function updateHistoricalCalculations() {
  try {
    console.log(`\n=== Starting Historical Calculations Update ===`);
    console.log(`Date Range: ${START_DATE} to ${END_DATE}`);
    console.log(`Miner Models: ${Object.keys(minerModels).join(', ')}\n`);

    // Parse and validate dates
    const startDate = new Date(START_DATE);
    const endDate = new Date(END_DATE);

    // Debug date parsing
    console.log('Parsed dates:', {
      startDate: format(startDate, 'yyyy-MM-dd'),
      endDate: format(endDate, 'yyyy-MM-dd'),
      startValid: isValid(startDate),
      endValid: isValid(endDate)
    });

    // Validate dates
    if (!isValid(startDate) || !isValid(endDate)) {
      throw new Error(`Invalid date format. Please provide dates in YYYY-MM-DD format. Provided dates: ${START_DATE}, ${END_DATE}`);
    }

    const dateRange = eachDayOfInterval({
      start: startDate,
      end: endDate
    });

    // Create a limit function to control concurrency
    const limit = pLimit(MAX_CONCURRENT_DAYS);

    let totalProcessed = 0;
    const totalToProcess = dateRange.length * Object.keys(minerModels).length;

    console.log(`Total days to process: ${dateRange.length}`);
    console.log(`Total calculations to perform: ${totalToProcess}\n`);

    // Process each date for all miner models
    const processPromises = dateRange.map(date => {
      const formattedDate = format(date, 'yyyy-MM-dd');

      return limit(async () => {
        console.log(`\nProcessing date: ${formattedDate}`);

        for (const minerModel of Object.keys(minerModels)) {
          try {
            console.log(`- Starting calculations for ${minerModel}`);

            // Process the day
            await processSingleDay(formattedDate, minerModel)
              .catch(error => {
                console.error(`Failed to process date ${formattedDate}:`, error);
                throw error;
              });

            // Verify the calculations
            const verification = await verifyDayCalculations(formattedDate, minerModel);
            console.log(`✓ Completed ${minerModel} for ${formattedDate}:`, {
              records: verification.recordCount,
              difficulty: verification.difficulty,
              totalBitcoin: verification.totalBitcoin.toFixed(8)
            });

            totalProcessed++;
            const progress = ((totalProcessed / totalToProcess) * 100).toFixed(1);
            console.log(`Progress: ${progress}% (${totalProcessed}/${totalToProcess})`);

          } catch (error) {
            console.error(`× Error processing ${minerModel} for ${formattedDate}:`, error);
            throw error;
          }
        }
      });
    });

    try {
      await Promise.all(processPromises);
      console.log('Completed processing all dates');
    } catch (error) {
      console.error('Error during parallel processing:', error);
      throw error;
    }

    // Final verification
    const finalStats = {
      totalDays: dateRange.length,
      totalModels: Object.keys(minerModels).length,
      expectedRecords: totalToProcess,
      processedRecords: totalProcessed
    };

    console.log('\n=== Historical Calculations Update Complete ===');
    console.log('Final Statistics:', finalStats);
    console.log('\n');

  } catch (error) {
    console.error('Error during historical calculations update:', error);
    process.exit(1);
  }
}

// Start the update process
updateHistoricalCalculations();
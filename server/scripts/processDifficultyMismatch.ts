import { getDifficultyData } from '../services/dynamodbService';
import { db } from "@db";
import { historicalBitcoinCalculations } from "@db/schema";
import { and, eq, sql } from "drizzle-orm";
import { minerModels } from '../types/bitcoin';
import { processSingleDay, fetch2024Difficulties } from '../services/bitcoinService';
import { format, subDays, parseISO } from 'date-fns';

const START_DATE = '2025-02-10';
const BATCH_SIZE = 5; // Process 5 dates at a time
const BATCH_DELAY = 30000; // 30 seconds between batches

async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function findDatesWithMismatchedDifficulty() {
  const query = sql`
    WITH distinct_dates AS (
      SELECT DISTINCT settlement_date
      FROM historical_bitcoin_calculations
      WHERE settlement_date <= ${START_DATE}
      ORDER BY settlement_date DESC
    ),
    difficulty_check AS (
      SELECT 
        settlement_date,
        MIN(difficulty::numeric) as saved_difficulty,
        MAX(difficulty::numeric) as max_difficulty
      FROM historical_bitcoin_calculations
      GROUP BY settlement_date
    )
    SELECT 
      settlement_date::text as date,
      saved_difficulty
    FROM difficulty_check
    WHERE settlement_date <= ${START_DATE}
    ORDER BY settlement_date DESC;
  `;

  const result = await db.execute(query);
  return result.rows;
}

async function processSingleDateDifficulty(date: string) {
  try {
    console.log(`\n=== Processing ${date} ===`);

    // Fetch difficulty from DynamoDB
    console.log('\nFetching difficulty from DynamoDB...');
    const dynamoDbDifficulty = await getDifficultyData(date);
    console.log(`DynamoDB difficulty: ${dynamoDbDifficulty}`);

    // Get current difficulty from database
    const currentData = await db
      .select({
        difficulty: sql<string>`MIN(difficulty)`,
        recordCount: sql<number>`COUNT(*)`,
        minerModels: sql<string[]>`array_agg(DISTINCT miner_model)`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, date));

    const savedDifficulty = currentData[0]?.difficulty;
    console.log(`Current saved difficulty: ${savedDifficulty}`);

    if (savedDifficulty === dynamoDbDifficulty.toString()) {
      console.log('✓ Difficulties match, skipping update');
      return true;
    }

    // Process each miner model
    const MINER_MODEL_LIST = Object.keys(minerModels);
    console.log(`\nProcessing ${MINER_MODEL_LIST.length} miner models...`);

    for (const minerModel of MINER_MODEL_LIST) {
      console.log(`\n- Processing ${minerModel}`);

      // Delete existing records for this date/model
      await db.delete(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );

      // Process the day with new difficulty
      await processSingleDay(date, minerModel);

      // Verify records were created
      const records = await db
        .select({
          count: sql<number>`COUNT(*)::int`,
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`,
          avgDifficulty: sql<string>`AVG(difficulty::numeric)::text`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );

      console.log(`✓ ${minerModel} processed:`, {
        recordCount: records[0].count,
        totalBitcoin: records[0].totalBitcoin,
        difficulty: records[0].avgDifficulty
      });
    }

    return true;
  } catch (error) {
    console.error(`Error processing ${date}:`, error);
    return false;
  }
}

async function processDifficultyMismatch() {
  try {
    console.log('\n=== Processing Historical Difficulty Mismatches ===');
    console.log(`Starting from: ${START_DATE}\n`);

    // Pre-fetch difficulties
    console.log('Pre-fetching difficulties...');
    await fetch2024Difficulties();
    console.log('Difficulties pre-fetch complete\n');

    // Get dates with mismatched difficulties
    const dates = await findDatesWithMismatchedDifficulty();
    console.log(`Found ${dates.length} dates to check\n`);

    let processed = 0;
    const results = {
      success: [] as string[],
      failed: [] as { date: string; reason: string }[]
    };

    // Process dates in batches
    for (let i = 0; i < dates.length; i += BATCH_SIZE) {
      const batch = dates.slice(i, i + BATCH_SIZE);
      console.log(`\n=== Processing batch ${Math.floor(i/BATCH_SIZE) + 1}/${Math.ceil(dates.length/BATCH_SIZE)} ===`);

      for (const { date, saved_difficulty } of batch) {
        console.log(`\n=== Date ${++processed}/${dates.length} ===`);
        console.log(`Processing: ${date}`);
        console.log(`Current saved difficulty: ${saved_difficulty}`);

        const success = await processSingleDateDifficulty(date);

        if (success) {
          results.success.push(date);
        } else {
          results.failed.push({
            date,
            reason: 'Failed to process date'
          });
        }

        // Progress update
        const progress = ((processed / dates.length) * 100).toFixed(1);
        console.log(`\nOverall Progress: ${progress}% (${processed}/${dates.length})`);
        console.log(`Success: ${results.success.length}, Failed: ${results.failed.length}`);

        // Small delay between dates within a batch
        if (processed < dates.length) {
          await sleep(5000); // 5 second delay between dates
        }
      }

      // Delay between batches
      if (i + BATCH_SIZE < dates.length) {
        console.log(`\nWaiting ${BATCH_DELAY/1000} seconds before next batch...`);
        await sleep(BATCH_DELAY);
      }
    }

    console.log('\n=== Difficulty Update Complete ===');
    console.log(`Total dates processed: ${dates.length}`);
    console.log(`Successfully processed: ${results.success.length}`);
    console.log(`Failed processing: ${results.failed.length}`);

    if (results.failed.length > 0) {
      console.log('\nFailed Dates:', results.failed);
    }

  } catch (error) {
    console.error('Error processing difficulty mismatches:', error);
    process.exit(1);
  }
}

// Start processing
processDifficultyMismatch();
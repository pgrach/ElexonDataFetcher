import { getDifficultyData } from '../services/dynamodbService';
import { db } from "@db";
import { historicalBitcoinCalculations } from "@db/schema";
import { and, eq, sql } from "drizzle-orm";
import { minerModels } from '../types/bitcoin';
import { processSingleDay, fetch2024Difficulties } from '../services/bitcoinService';
import { format, subDays, parseISO } from 'date-fns';

const START_DATE = '2025-02-10';
const BATCH_SIZE = 5; // Increased batch size since we removed delay
const PROGRESS_FILE = 'difficulty_sync_progress.json';

// Local cache for difficulties to avoid redundant DynamoDB calls
const difficultyCache = new Map<string, number>();

async function saveProgress(lastProcessedDate: string, successCount: number, failedDates: Array<{ date: string; reason: string }>) {
  try {
    await db.execute(sql`
      INSERT INTO process_tracking (process_name, last_processed_date, success_count, failed_dates)
      VALUES ('difficulty_sync', ${lastProcessedDate}, ${successCount}, ${JSON.stringify(failedDates)})
      ON CONFLICT (process_name) 
      DO UPDATE SET 
        last_processed_date = ${lastProcessedDate},
        successCount = ${successCount},
        failed_dates = ${JSON.stringify(failedDates)},
        updated_at = NOW();
    `);
  } catch (error) {
    console.error('Error saving progress:', error);
  }
}

async function getLastProgress() {
  try {
    const result = await db.execute(sql`
      SELECT last_processed_date, success_count, failed_dates
      FROM process_tracking
      WHERE process_name = 'difficulty_sync'
      LIMIT 1;
    `);

    if (result.rows.length > 0) {
      return {
        lastProcessedDate: result.rows[0].last_processed_date as string,
        successCount: result.rows[0].success_count as number,
        failedDates: result.rows[0].failed_dates as Array<{ date: string; reason: string }>
      };
    }
  } catch (error) {
    console.error('Error getting progress:', error);
  }
  return null;
}

async function getDifficulty(date: string): Promise<number> {
  // Check cache first
  if (difficultyCache.has(date)) {
    console.log(`Using cached difficulty for ${date}: ${difficultyCache.get(date)}`);
    return difficultyCache.get(date)!;
  }

  // Fetch from DynamoDB if not in cache
  console.log('Fetching difficulty from DynamoDB...');
  const difficultyData = await getDifficultyData(date);
  // Add type assertion to handle unknown type
  if (typeof difficultyData === 'object' && difficultyData !== null) {
    const typedResult = difficultyData as { difficulty?: number };
    if (typedResult.difficulty) {
      const difficulty = typedResult.difficulty;
      // Cache the result
      difficultyCache.set(date, difficulty);
      console.log(`Cached difficulty for ${date}: ${difficulty}`);
      return difficulty;
    }
  } else if (typeof difficultyData === 'number') {
    const difficulty = difficultyData;
    // Cache the result
    difficultyCache.set(date, difficulty);
    console.log(`Cached difficulty for ${date}: ${difficulty}`);
    return difficulty;
  }
  throw new Error(`Invalid difficulty data for date ${date}`);
}

async function findDatesWithMismatchedDifficulty(startDate?: string) {
  const query = sql`
    WITH difficulty_check AS (
      SELECT 
        settlement_date,
        MIN(difficulty::numeric) as saved_difficulty,
        MAX(difficulty::numeric) as max_difficulty,
        COUNT(DISTINCT difficulty) as difficulty_variations
      FROM historical_bitcoin_calculations
      GROUP BY settlement_date
    )
    SELECT 
      settlement_date::text as date,
      saved_difficulty::text as saved_difficulty,
      difficulty_variations
    FROM difficulty_check
    WHERE settlement_date <= ${START_DATE}
    ${startDate ? sql`AND settlement_date < ${startDate}` : sql``}
    ORDER BY settlement_date DESC;
  `;

  const result = await db.execute(query);
  if (!result || typeof result !== 'object' || !('rows' in result) || !Array.isArray(result.rows)) {
    return [];
  }
  return result.rows.map(row => ({
    date: row.date as string,
    savedDifficulty: row.saved_difficulty as string,
    difficultyVariations: row.difficulty_variations as number
  }));
}

async function processSingleDateDifficulty(date: string) {
  try {
    console.log(`\n=== Processing ${date} ===`);

    const dynamoDbDifficulty = await getDifficulty(date);
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
      return { success: true, message: 'Difficulties match' };
    }

    // Process each miner model
    const MINER_MODEL_LIST = Object.keys(minerModels);
    console.log(`\nProcessing ${MINER_MODEL_LIST.length} miner models...`);

    for (const minerModel of MINER_MODEL_LIST) {
      console.log(`\n- Processing ${minerModel}`);

      try {
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
      } catch (error) {
        console.error(`Error processing ${minerModel} for ${date}:`, error);
        return { success: false, message: `Failed processing ${minerModel}: ${error}` };
      }
    }

    return { success: true, message: 'Successfully processed all models' };
  } catch (error) {
    console.error(`Error processing ${date}:`, error);
    return { success: false, message: String(error) };
  }
}

async function processDifficultyMismatch() {
  try {
    console.log('\n=== Processing Historical Difficulty Mismatches ===');
    console.log(`Starting from: ${START_DATE}\n`);

    // Get last progress
    const progress = await getLastProgress();
    let startDate = progress?.lastProcessedDate;
    let successCount = progress?.successCount || 0;
    let failedDates = progress?.failedDates || [];

    // Pre-fetch difficulties
    console.log('Pre-fetching difficulties...');
    await fetch2024Difficulties();
    console.log('Difficulties pre-fetch complete\n');

    // Get dates with mismatched difficulties
    const dates = await findDatesWithMismatchedDifficulty(startDate);
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

      for (const { date, savedDifficulty } of batch) {
        console.log(`\n=== Date ${++processed}/${dates.length} ===`);
        console.log(`Processing: ${date}`);
        console.log(`Current saved difficulty: ${savedDifficulty}`);

        const result = await processSingleDateDifficulty(date);

        if (result.success) {
          results.success.push(date);
          successCount++;
        } else {
          results.failed.push({
            date,
            reason: result.message
          });
          failedDates.push({
            date,
            reason: result.message
          });
        }

        // Save progress after each date
        await saveProgress(date, successCount, failedDates);

        // Progress update
        const progress = ((processed / dates.length) * 100).toFixed(1);
        console.log(`\nOverall Progress: ${progress}% (${processed}/${dates.length})`);
        console.log(`Success: ${results.success.length}, Failed: ${results.failed.length}`);
      }
    }

    console.log('\n=== Difficulty Update Complete ===');
    console.log(`Total dates processed: ${dates.length}`);
    console.log(`Successfully processed: ${results.success.length}`);
    console.log(`Failed processing: ${results.failed.length}`);

    if (results.failed.length > 0) {
      console.log('\nFailed Dates:', results.failed);
    }

    await generateProgressReport();

  } catch (error) {
    console.error('Error processing difficulty mismatches:', error);
    process.exit(1);
  }
}

async function generateProgressReport() {
  try {
    const stats = await db.execute(sql`
      WITH summary AS (
        SELECT 
          COUNT(DISTINCT settlement_date) as total_dates,
          MIN(settlement_date) as earliest_date,
          MAX(settlement_date) as latest_date,
          COUNT(DISTINCT difficulty) as unique_difficulties
        FROM historical_bitcoin_calculations
        WHERE settlement_date <= ${START_DATE}
      ),
      difficulty_variations AS (
        SELECT 
          settlement_date,
          COUNT(DISTINCT difficulty) as diff_count
        FROM historical_bitcoin_calculations
        GROUP BY settlement_date
        HAVING COUNT(DISTINCT difficulty) > 1
      )
      SELECT 
        s.total_dates,
        s.earliest_date::text,
        s.latest_date::text,
        s.unique_difficulties,
        COUNT(dv.settlement_date) as dates_with_variations
      FROM summary s
      LEFT JOIN difficulty_variations dv ON true
      GROUP BY s.total_dates, s.earliest_date, s.latest_date, s.unique_difficulties;
    `);

    const progress = await db.execute(sql`
      SELECT 
        last_processed_date::text,
        success_count,
        failed_dates,
        updated_at::text
      FROM process_tracking 
      WHERE process_name = 'difficulty_sync'
      ORDER BY updated_at DESC
      LIMIT 1;
    `);

    console.log('\n=== Difficulty Sync Progress Report ===');
    console.log('\nDatabase Statistics:');
    if (stats.rows[0]) {
      console.log(`Total Dates Processed: ${stats.rows[0].total_dates}`);
      console.log(`Date Range: ${stats.rows[0].earliest_date} to ${stats.rows[0].latest_date}`);
      console.log(`Unique Difficulty Values: ${stats.rows[0].unique_difficulties}`);
      console.log(`Dates with Multiple Difficulties: ${stats.rows[0].dates_with_variations}`);
    }

    console.log('\nSync Progress:');
    if (progress.rows[0]) {
      console.log(`Last Processed Date: ${progress.rows[0].last_processed_date}`);
      console.log(`Successfully Processed: ${progress.rows[0].success_count} dates`);
      console.log(`Failed Dates: ${progress.rows[0]?.failed_dates?.length || 0}`);
      console.log(`Last Update: ${progress.rows[0].updated_at}`);
    }

    return { stats: stats.rows[0], progress: progress.rows[0] };
  } catch (error) {
    console.error('Error generating progress report:', error);
    return null;
  }
}

// Start processing
processDifficultyMismatch();
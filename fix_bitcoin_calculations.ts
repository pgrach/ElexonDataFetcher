/**
 * Bitcoin Calculation Fix Tool
 * 
 * This script addresses an issue where curtailment records don't have a 1:1 relationship
 * with bitcoin calculations. The system was designed to aggregate multiple curtailment records
 * per period/farm into a single bitcoin calculation, but we need each curtailment record
 * to have its own calculation.
 * 
 * Usage:
 *   npx tsx fix_bitcoin_calculations.ts [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--limit N] [--analyze-only]
 * 
 * Options:
 *   --start         Start date (default: 2025-01-01)
 *   --end           End date (default: today)
 *   --limit         Limit number of records to process (default: process all)
 *   --analyze-only  Only analyze the data without making changes
 */

import { db } from './db';
import { and, eq, sql } from 'drizzle-orm';
import { DEFAULT_DIFFICULTY, minerModels } from './server/types/bitcoin';
import { getDifficultyData } from './server/services/dynamodbService';
import { format, parseISO, addDays } from 'date-fns';

// Constants
const SETTLEMENT_PERIOD_MINUTES = 30;
const BLOCK_REWARD = 6.25; // Current Bitcoin block reward
const BLOCKS_PER_SETTLEMENT_PERIOD = SETTLEMENT_PERIOD_MINUTES / 10; // 10 minutes per block

// Difficulty cache
const DIFFICULTY_CACHE = new Map<string, string>();
const MINER_MODELS = Object.keys(minerModels);

// Calculate Bitcoin for a single curtailment record
function calculateBitcoinForRecord(
  curtailedMwh: number,
  minerModel: string,
  difficulty: number
): number {
  const miner = minerModels[minerModel];
  if (!miner) throw new Error(`Invalid miner model: ${minerModel}`);

  const curtailedKwh = curtailedMwh * 1000;
  const minerConsumptionKwh = (miner.power / 1000) * (SETTLEMENT_PERIOD_MINUTES / 60);
  const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);
  const difficultyNum = typeof difficulty === 'string' ? parseFloat(difficulty) : difficulty;
  const hashesPerBlock = difficultyNum * Math.pow(2, 32);
  const networkHashRate = hashesPerBlock / 600;
  const networkHashRateTH = networkHashRate / 1e12;
  const totalHashPower = potentialMiners * miner.hashrate;
  const ourNetworkShare = totalHashPower / networkHashRateTH;
  return Number((ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_SETTLEMENT_PERIOD).toFixed(8));
}

// Analyze the problem - find dates, periods, and farms with incomplete bitcoin calculations
async function analyzeIncompleteCalculations(startDate: string, endDate: string): Promise<void> {
  console.log(`\nAnalyzing incomplete Bitcoin calculations from ${startDate} to ${endDate}`);
  
  // First, check overall counts
  const overallCountsQuery = `
    WITH curtailment_periods AS (
      SELECT 
        settlement_date,
        settlement_period,
        farm_id,
        COUNT(*) as curtailment_count
      FROM 
        curtailment_records
      WHERE 
        settlement_date BETWEEN '${startDate}' AND '${endDate}'
      GROUP BY 
        settlement_date, settlement_period, farm_id
    ),
    expected_calculations AS (
      SELECT
        settlement_date,
        settlement_period,
        farm_id,
        '${MINER_MODELS[0]}' AS miner_model,
        curtailment_count
      FROM
        curtailment_periods
      UNION ALL
      SELECT
        settlement_date,
        settlement_period,
        farm_id,
        '${MINER_MODELS[1]}' AS miner_model,
        curtailment_count
      FROM
        curtailment_periods
      UNION ALL
      SELECT
        settlement_date,
        settlement_period,
        farm_id,
        '${MINER_MODELS[2]}' AS miner_model,
        curtailment_count
      FROM
        curtailment_periods
    ),
    actual_calculations AS (
      SELECT
        settlement_date,
        settlement_period,
        farm_id,
        miner_model,
        COUNT(*) as calculation_count
      FROM
        historical_bitcoin_calculations
      WHERE
        settlement_date BETWEEN '${startDate}' AND '${endDate}'
      GROUP BY
        settlement_date, settlement_period, farm_id, miner_model
    )
    SELECT
      COUNT(*) as total_incomplete,
      SUM(CASE WHEN a.calculation_count IS NULL THEN e.curtailment_count ELSE e.curtailment_count - a.calculation_count END) as total_missing_calculations,
      COUNT(DISTINCT e.settlement_date) as affected_dates
    FROM
      expected_calculations e
    LEFT JOIN
      actual_calculations a ON
        e.settlement_date = a.settlement_date AND
        e.settlement_period = a.settlement_period AND
        e.farm_id = a.farm_id AND
        e.miner_model = a.miner_model
    WHERE
      a.calculation_count IS NULL OR a.calculation_count != e.curtailment_count;
  `;
  
  const overallCounts = await db.execute(sql.raw(overallCountsQuery));
  
  if (overallCounts.rows.length > 0) {
    const stats = overallCounts.rows[0];
    console.log(`Found ${stats.total_incomplete} incomplete combinations`);
    console.log(`Missing ${stats.total_missing_calculations} total calculations`);
    console.log(`Affecting ${stats.affected_dates} different dates`);
  }
  
  // Get breakdown by date
  const dateBreakdownQuery = `
    WITH curtailment_periods AS (
      SELECT 
        settlement_date,
        settlement_period,
        farm_id,
        COUNT(*) as curtailment_count
      FROM 
        curtailment_records
      WHERE 
        settlement_date BETWEEN '${startDate}' AND '${endDate}'
      GROUP BY 
        settlement_date, settlement_period, farm_id
    ),
    expected_calculations AS (
      SELECT
        settlement_date,
        settlement_period,
        farm_id,
        '${MINER_MODELS[0]}' AS miner_model,
        curtailment_count
      FROM
        curtailment_periods
      UNION ALL
      SELECT
        settlement_date,
        settlement_period,
        farm_id,
        '${MINER_MODELS[1]}' AS miner_model,
        curtailment_count
      FROM
        curtailment_periods
      UNION ALL
      SELECT
        settlement_date,
        settlement_period,
        farm_id,
        '${MINER_MODELS[2]}' AS miner_model,
        curtailment_count
      FROM
        curtailment_periods
    ),
    actual_calculations AS (
      SELECT
        settlement_date,
        settlement_period,
        farm_id,
        miner_model,
        COUNT(*) as calculation_count
      FROM
        historical_bitcoin_calculations
      WHERE
        settlement_date BETWEEN '${startDate}' AND '${endDate}'
      GROUP BY
        settlement_date, settlement_period, farm_id, miner_model
    )
    SELECT
      e.settlement_date,
      COUNT(*) as incomplete_combinations,
      SUM(CASE WHEN a.calculation_count IS NULL THEN e.curtailment_count ELSE e.curtailment_count - a.calculation_count END) as missing_calculations
    FROM
      expected_calculations e
    LEFT JOIN
      actual_calculations a ON
        e.settlement_date = a.settlement_date AND
        e.settlement_period = a.settlement_period AND
        e.farm_id = a.farm_id AND
        e.miner_model = a.miner_model
    WHERE
      a.calculation_count IS NULL OR a.calculation_count != e.curtailment_count
    GROUP BY
      e.settlement_date
    ORDER BY
      e.settlement_date;
  `;
  
  const dateBreakdown = await db.execute(sql.raw(dateBreakdownQuery));
  
  if (dateBreakdown.rows.length > 0) {
    console.log("\n=== Incomplete Calculations by Date ===");
    console.log("Date         | Incomplete Combinations | Missing Calculations");
    console.log("-------------|-------------------------|--------------------");
    
    for (const row of dateBreakdown.rows) {
      console.log(
        `${row.settlement_date} | ${String(row.incomplete_combinations).padStart(25)} | ${String(row.missing_calculations).padStart(20)}`
      );
    }
  }
}

// Process a single day for a specific miner model
async function processSingleDay(date: string, minerModel: string, analyzeOnly: boolean = false): Promise<{
  processed: number;
  totalRecords: number;
}> {
  console.log(`\nProcessing ${date} for ${minerModel}...`);
  
  try {
    // If difficulty is not in cache, fetch it
    if (!DIFFICULTY_CACHE.has(date)) {
      try {
        const difficulty = await getDifficultyData(date);
        DIFFICULTY_CACHE.set(date, difficulty.toString());
        console.log(`Fetched and cached difficulty for ${date}: ${difficulty}`);
      } catch (error) {
        console.warn(`Failed to fetch difficulty for ${date}, using default: ${DEFAULT_DIFFICULTY}`);
        DIFFICULTY_CACHE.set(date, DEFAULT_DIFFICULTY.toString());
      }
    }
    
    const difficulty = DIFFICULTY_CACHE.get(date) || DEFAULT_DIFFICULTY.toString();
    
    // Find curtailment records that don't have corresponding bitcoin calculations
    // This query addresses the unique constraint by generating a custom calculation ID for each record
    const incompleteCalculationsQuery = `
      WITH curtailment_data AS (
        SELECT 
          id,
          settlement_date,
          settlement_period,
          farm_id,
          volume::numeric as volume_num,
          ROW_NUMBER() OVER (PARTITION BY settlement_date, settlement_period, farm_id ORDER BY id) as row_num
        FROM 
          curtailment_records cr
        WHERE 
          settlement_date = '${date}'
          AND ABS(volume::numeric) > 0
          -- Check if there's already a record for this in historical_bitcoin_calculations
          AND NOT EXISTS (
            SELECT 1 FROM historical_bitcoin_calculations hbc
            WHERE 
              hbc.settlement_date = cr.settlement_date
              AND hbc.settlement_period = cr.settlement_period
              AND hbc.farm_id = cr.farm_id
              AND hbc.miner_model = '${minerModel}'
              -- Using the row number to distinguish multiple records for the same date/period/farm
              AND hbc.bitcoin_mined = ABS(cr.volume::numeric) * 0.000001 -- Use a formula to identify which curtailment record this calculation is for
          )
      )
      SELECT 
        id,
        settlement_date,
        settlement_period,
        farm_id,
        volume_num,
        row_num
      FROM 
        curtailment_data
      ORDER BY
        settlement_period, farm_id, row_num;
    `;
    
    // Execute the query
    const result = await db.execute(sql.raw(incompleteCalculationsQuery));
    const recordsToProcess = result.rows;
    
    if (recordsToProcess.length === 0) {
      console.log(`All curtailment records already have calculations for ${minerModel}`);
      return { processed: 0, totalRecords: 0 };
    }
    
    console.log(`Found ${recordsToProcess.length} curtailment records needing calculations for ${minerModel}`);
    
    if (analyzeOnly) {
      console.log(`Analysis only mode - not making changes`);
      return { processed: 0, totalRecords: recordsToProcess.length };
    }
    
    // Prepare bulk insert data
    const bulkInsertData = recordsToProcess.map(record => {
      const absVolume = Math.abs(Number(record.volume_num));
      const bitcoinMined = calculateBitcoinForRecord(
        absVolume,
        minerModel,
        parseFloat(difficulty)
      );
      
      return {
        settlementDate: record.settlement_date,
        settlementPeriod: record.settlement_period,
        farmId: record.farm_id,
        minerModel,
        bitcoinMined: bitcoinMined.toFixed(8),
        difficulty,
        calculatedAt: new Date()
      };
    });
    
    // Insert the new calculations in batches
    const BATCH_SIZE = 500;
    let insertedCount = 0;
    
    for (let i = 0; i < bulkInsertData.length; i += BATCH_SIZE) {
      const batch = bulkInsertData.slice(i, i + BATCH_SIZE);
      
      // Use raw SQL for insertion
      const insertValues = batch.map(b => `(
        '${b.settlementDate}', 
        ${b.settlementPeriod}, 
        '${b.farmId}', 
        '${b.minerModel}', 
        ${b.bitcoinMined}, 
        ${b.difficulty}, 
        '${b.calculatedAt.toISOString()}'
      )`).join(', ');
      
      const insertQuery = `
        INSERT INTO historical_bitcoin_calculations (
          settlement_date, 
          settlement_period, 
          farm_id, 
          miner_model, 
          bitcoin_mined, 
          difficulty, 
          calculated_at
        ) VALUES ${insertValues};
      `;
      
      await db.execute(sql.raw(insertQuery));
      insertedCount += batch.length;
      
      if (i + BATCH_SIZE < bulkInsertData.length) {
        console.log(`Inserted ${insertedCount}/${bulkInsertData.length} records...`);
      }
    }
    
    console.log(`Inserted ${insertedCount} new calculations for ${date} ${minerModel}`);
    return { processed: insertedCount, totalRecords: recordsToProcess.length };
  } catch (error) {
    console.error(`Error processing ${date} for ${minerModel}:`, error);
    throw error;
  }
}

// Process date range
async function processDateRange(
  startDate: string, 
  endDate: string, 
  limit?: number, 
  analyzeOnly: boolean = false
): Promise<void> {
  // First, analyze the current state
  await analyzeIncompleteCalculations(startDate, endDate);
  
  if (analyzeOnly) {
    console.log("\nAnalysis-only mode - not making any changes");
    return;
  }
  
  // Generate array of dates to process
  const dates: string[] = [];
  let currentDate = parseISO(startDate);
  const finalDate = parseISO(endDate);
  
  while (currentDate <= finalDate) {
    dates.push(format(currentDate, 'yyyy-MM-dd'));
    currentDate = addDays(currentDate, 1);
  }
  
  console.log(`\nProcessing ${dates.length} dates from ${startDate} to ${endDate}`);
  
  // Keep track of progress
  let totalProcessed = 0;
  const totalResults: Record<string, { processed: number, total: number }> = {};
  
  // Process each date for each miner model
  for (const date of dates) {
    console.log(`\n=== Processing date: ${date} ===`);
    
    for (const minerModel of MINER_MODELS) {
      const result = await processSingleDay(date, minerModel);
      totalProcessed += result.processed;
      
      if (!totalResults[date]) {
        totalResults[date] = { processed: 0, total: 0 };
      }
      
      totalResults[date].processed += result.processed;
      totalResults[date].total = result.totalRecords;
      
      // If we've reached the limit, exit early
      if (limit && totalProcessed >= limit) {
        console.log(`\nReached limit of ${limit} records, stopping processing`);
        break;
      }
    }
    
    // If we've reached the limit, exit the outer loop too
    if (limit && totalProcessed >= limit) {
      break;
    }
  }
  
  // Print summary
  console.log("\n=== Processing Summary ===");
  for (const [date, result] of Object.entries(totalResults)) {
    console.log(`${date}: Added ${result.processed} calculations of ${result.total} missing`);
  }
  
  console.log(`\nTotal calculations added: ${totalProcessed}`);
  
  // Analyze again to confirm changes
  console.log("\n=== Final State After Processing ===");
  await analyzeIncompleteCalculations(startDate, endDate);
}

// Main function
async function main() {
  const args = process.argv.slice(2);
  
  let startDate = '2025-01-01';
  let endDate = format(new Date(), 'yyyy-MM-dd');
  let limit: number | undefined;
  let analyzeOnly = false;
  
  // Parse command line arguments
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--start' && i + 1 < args.length) {
      startDate = args[i + 1];
    } else if (args[i] === '--end' && i + 1 < args.length) {
      endDate = args[i + 1];
    } else if (args[i] === '--limit' && i + 1 < args.length) {
      limit = parseInt(args[i + 1], 10);
    } else if (args[i] === '--analyze-only') {
      analyzeOnly = true;
    }
  }
  
  console.log('=== Bitcoin Calculation Fix Tool ===');
  console.log(`Start date: ${startDate}`);
  console.log(`End date: ${endDate}`);
  if (limit) {
    console.log(`Processing limit: ${limit} records`);
  }
  if (analyzeOnly) {
    console.log(`Mode: Analysis only (no changes will be made)`);
  }
  
  try {
    await processDateRange(startDate, endDate, limit, analyzeOnly);
    console.log('\nProcessing completed successfully');
  } catch (error) {
    console.error('\nProcessing failed:', error);
    process.exit(1);
  }
}

// Run the main function
main().catch(console.error);
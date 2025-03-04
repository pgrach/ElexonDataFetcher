/**
 * Bitcoin Calculation Fix Tool
 * 
 * This script addresses an issue where curtailment records don't have a 1:1 relationship
 * with bitcoin calculations. The system was designed to aggregate multiple curtailment records
 * per period/farm into a single bitcoin calculation, but we need each curtailment record
 * to have its own calculation.
 * 
 * Usage:
 *   npx tsx fix_bitcoin_calculations.ts [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--limit N]
 * 
 * Options:
 *   --start      Start date (default: 2025-01-01)
 *   --end        End date (default: today)
 *   --limit      Limit number of records to process (default: process all)
 */

import { db } from './db';
import { historicalBitcoinCalculations, curtailmentRecords } from './db/schema';
import { and, eq, inArray, sql } from 'drizzle-orm';
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

// Process a single day for a specific miner model
async function processSingleDay(date: string, minerModel: string): Promise<{
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
    
    // First, get all curtailment records for the day with positive volume
    const allCurtailmentRecords = await db
      .select({
        id: curtailmentRecords.id,
        settlementDate: curtailmentRecords.settlementDate,
        settlementPeriod: curtailmentRecords.settlementPeriod,
        farmId: curtailmentRecords.farmId,
        volume: curtailmentRecords.volume,
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          sql`ABS(volume::numeric) > 0`
        )
      );
    
    if (allCurtailmentRecords.length === 0) {
      console.log(`No curtailment records found for ${date}`);
      return { processed: 0, totalRecords: 0 };
    }
    
    console.log(`Found ${allCurtailmentRecords.length} curtailment records for ${date}`);
    
    // Check if the curtailmentId column exists
    let existingCalculationIds = new Set<number>();
    
    try {
      // First, check if the column exists to avoid errors
      const columnCheckResult = await db.execute(sql.raw(`
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'historical_bitcoin_calculations' 
        AND column_name = 'curtailment_id'
      `));
      
      // Only query with curtailmentId if the column exists and has data
      if (columnCheckResult.rows.length > 0) {
        // Use raw SQL to avoid Drizzle ORM issues when the schema doesn't match the actual table
        const existingCalculations = await db.execute(sql.raw(`
          SELECT curtailment_id
          FROM historical_bitcoin_calculations
          WHERE settlement_date = '${date}'
          AND miner_model = '${minerModel}'
          AND curtailment_id IS NOT NULL
        `));
        
        // Extract the curtailment IDs
        existingCalculationIds = new Set(
          existingCalculations.rows
            .map((row: any) => row.curtailment_id)
            .filter(Boolean)
            .map(Number)
        );
      }
    } catch (error) {
      console.warn(`Could not fetch existing calculations with curtailment_id: ${error}`);
      // Continue without filtering - we'll handle duplicates via constraints
    }
    
    console.log(`Found ${existingCalculationIds.size} existing calculations for ${minerModel}`);
    
    // Filter out records that already have calculations
    const recordsToProcess = allCurtailmentRecords.filter(
      record => !existingCalculationIds.has(record.id)
    );
    
    if (recordsToProcess.length === 0) {
      console.log(`All curtailment records already have calculations for ${minerModel}`);
      return { processed: 0, totalRecords: allCurtailmentRecords.length };
    }
    
    console.log(`Processing ${recordsToProcess.length} curtailment records for ${minerModel}`);
    
    // Prepare bulk insert data
    const bulkInsertData = recordsToProcess.map(record => {
      const absVolume = Math.abs(Number(record.volume));
      const bitcoinMined = calculateBitcoinForRecord(
        absVolume,
        minerModel,
        parseFloat(difficulty)
      );
      
      return {
        curtailmentId: record.id,
        settlementDate: record.settlementDate,
        settlementPeriod: record.settlementPeriod,
        farmId: record.farmId,
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
      await db.insert(historicalBitcoinCalculations).values(batch);
      insertedCount += batch.length;
      
      if (i + BATCH_SIZE < bulkInsertData.length) {
        console.log(`Inserted ${insertedCount}/${bulkInsertData.length} records...`);
      }
    }
    
    console.log(`Inserted ${insertedCount} new calculations for ${date} ${minerModel}`);
    return { processed: insertedCount, totalRecords: allCurtailmentRecords.length };
  } catch (error) {
    console.error(`Error processing ${date} for ${minerModel}:`, error);
    throw error;
  }
}

// Add a curtailmentId column to historicalBitcoinCalculations if it doesn't exist
async function ensureCurtailmentIdColumn(): Promise<void> {
  try {
    // Check if the column exists
    const checkQuery = `
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = 'historical_bitcoin_calculations' 
      AND column_name = 'curtailment_id'
    `;
    
    const result = await db.execute(sql.raw(checkQuery));
    
    if (result.rows.length === 0) {
      console.log("Adding curtailment_id column to historical_bitcoin_calculations table...");
      
      // Add the column
      await db.execute(sql.raw(`
        ALTER TABLE historical_bitcoin_calculations 
        ADD COLUMN curtailment_id INTEGER,
        ADD CONSTRAINT fk_curtailment 
        FOREIGN KEY (curtailment_id) 
        REFERENCES curtailment_records(id)
        ON DELETE CASCADE
      `));
      
      console.log("Column added successfully");
    } else {
      console.log("curtailment_id column already exists");
    }
  } catch (error) {
    console.error("Error ensuring curtailment_id column:", error);
    throw error;
  }
}

// Process date range
async function processDateRange(startDate: string, endDate: string, limit?: number): Promise<void> {
  // First ensure we have the necessary column
  await ensureCurtailmentIdColumn();
  
  // Generate array of dates to process
  const dates: string[] = [];
  let currentDate = parseISO(startDate);
  const finalDate = parseISO(endDate);
  
  while (currentDate <= finalDate) {
    dates.push(format(currentDate, 'yyyy-MM-dd'));
    currentDate = addDays(currentDate, 1);
  }
  
  console.log(`Processing ${dates.length} dates from ${startDate} to ${endDate}`);
  
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
    const percentage = result.total > 0 
      ? ((result.processed / (result.total * MINER_MODELS.length)) * 100).toFixed(1) 
      : '0.0';
    console.log(`${date}: Added ${result.processed} calculations (${percentage}% of missing)`);
  }
  
  console.log(`\nTotal calculations added: ${totalProcessed}`);
}

// Main function
async function main() {
  const args = process.argv.slice(2);
  
  let startDate = '2025-01-01';
  let endDate = format(new Date(), 'yyyy-MM-dd');
  let limit: number | undefined;
  
  // Parse command line arguments
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--start' && i + 1 < args.length) {
      startDate = args[i + 1];
    } else if (args[i] === '--end' && i + 1 < args.length) {
      endDate = args[i + 1];
    } else if (args[i] === '--limit' && i + 1 < args.length) {
      limit = parseInt(args[i + 1], 10);
    }
  }
  
  console.log('=== Bitcoin Calculation Fix Tool ===');
  console.log(`Start date: ${startDate}`);
  console.log(`End date: ${endDate}`);
  if (limit) {
    console.log(`Processing limit: ${limit} records`);
  }
  
  try {
    await processDateRange(startDate, endDate, limit);
    console.log('\nProcessing completed successfully');
  } catch (error) {
    console.error('\nProcessing failed:', error);
    process.exit(1);
  }
}

// Run the main function
main().catch(console.error);
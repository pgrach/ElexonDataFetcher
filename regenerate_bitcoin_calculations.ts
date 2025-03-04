/**
 * Bitcoin Calculation Regeneration Tool
 * 
 * This script completely regenerates bitcoin calculations for specified dates.
 * It uses a more reliable approach by first deleting existing calculations
 * and then regenerating them properly with one calculation per curtailment record.
 * 
 * Usage:
 *   npx tsx regenerate_bitcoin_calculations.ts [--start YYYY-MM-DD] [--end YYYY-MM-DD] [--fix] [--analyze-only]
 * 
 * Options:
 *   --start        Start date (default: 2025-01-01)
 *   --end          End date (default: today)
 *   --fix          Actually apply the fixes (default: false)
 *   --analyze-only Only analyze the data without making changes
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
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

// Analyze the status of a specific date range
async function analyzeCalculations(startDate: string, endDate: string): Promise<void> {
  console.log(`\nAnalyzing Bitcoin calculations from ${startDate} to ${endDate}`);
  
  // Get curtailment record counts and bitcoin calculation counts by date
  const query = `
    WITH curtailment_counts AS (
      SELECT 
        settlement_date,
        COUNT(*) as curtailment_count,
        COUNT(DISTINCT (settlement_period, farm_id)) as unique_period_farms,
        COUNT(DISTINCT miner_model) as miner_model_count
      FROM 
        curtailment_records
      CROSS JOIN (
        SELECT unnest(ARRAY['${MINER_MODELS.join("','")}']) as miner_model
      ) mm
      WHERE 
        settlement_date BETWEEN '${startDate}' AND '${endDate}'
      GROUP BY 
        settlement_date
    ),
    calculation_counts AS (
      SELECT 
        settlement_date,
        COUNT(*) as calculation_count,
        COUNT(DISTINCT (settlement_period, farm_id, miner_model)) as unique_calcs
      FROM 
        historical_bitcoin_calculations
      WHERE 
        settlement_date BETWEEN '${startDate}' AND '${endDate}'
      GROUP BY 
        settlement_date
    )
    SELECT
      cc.settlement_date,
      cc.curtailment_count,
      COALESCE(calc.calculation_count, 0) as calculation_count,
      cc.unique_period_farms * ${MINER_MODELS.length} as expected_unique_calcs,
      COALESCE(calc.unique_calcs, 0) as actual_unique_calcs,
      cc.curtailment_count as expected_calculations,
      COALESCE(calc.calculation_count, 0) as actual_calculations,
      cc.curtailment_count - COALESCE(calc.calculation_count, 0) as missing_calculations,
      CASE 
        WHEN COALESCE(calc.calculation_count, 0) = 0 THEN 'MISSING_ALL'
        WHEN cc.curtailment_count > COALESCE(calc.calculation_count, 0) THEN 'INCOMPLETE'
        WHEN cc.curtailment_count < COALESCE(calc.calculation_count, 0) THEN 'EXCESS'
        ELSE 'COMPLETE'
      END as status
    FROM
      curtailment_counts cc
    LEFT JOIN
      calculation_counts calc ON cc.settlement_date = calc.settlement_date
    ORDER BY
      cc.settlement_date;
  `;
  
  const results = await db.execute(sql.raw(query));
  
  if (results.rows.length === 0) {
    console.log("No dates found in the specified range.");
    return;
  }
  
  // Display summary statistics
  const totalCurtailmentRecords = results.rows.reduce((sum, row) => sum + parseInt(String(row.curtailment_count)), 0);
  const totalCalculations = results.rows.reduce((sum, row) => sum + parseInt(String(row.calculation_count)), 0);
  const totalMissingCalculations = results.rows.reduce((sum, row) => sum + parseInt(String(row.missing_calculations)), 0);
  const datesNeedingFix = results.rows.filter(row => row.status !== 'COMPLETE').length;
  
  console.log("=== Overall Statistics ===");
  console.log(`Total curtailment records: ${totalCurtailmentRecords}`);
  console.log(`Total bitcoin calculations: ${totalCalculations}`);
  console.log(`Total missing calculations: ${totalMissingCalculations}`);
  console.log(`Dates needing fixes: ${datesNeedingFix} of ${results.rows.length}\n`);
  
  if (datesNeedingFix > 0) {
    console.log("=== Dates Needing Fixes ===");
    console.log("Date         | Curtailment Records | Bitcoin Calculations | Missing | Status");
    console.log("-------------|---------------------|----------------------|---------|--------");
    
    for (const row of results.rows) {
      if (row.status !== 'COMPLETE') {
        console.log(
          `${row.settlement_date} | ${String(row.curtailment_count).padStart(19)} | ${String(row.calculation_count).padStart(20)} | ${String(row.missing_calculations).padStart(7)} | ${row.status}`
        );
      }
    }
  }
}

// Regenerate bitcoin calculations for a specific date
async function regenerateCalculations(date: string, applyFix: boolean = false): Promise<{
  curtailmentCount: number;
  calculationsGenerated: number;
}> {
  console.log(`\n=== Processing date: ${date} ===`);
  
  if (!applyFix) {
    console.log("Analysis mode - not applying fixes");
  }
  
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
    
    // First, count the curtailment records
    const countQuery = `
      SELECT COUNT(*) as record_count
      FROM curtailment_records
      WHERE settlement_date = '${date}' AND ABS(volume::numeric) > 0
    `;
    
    const countResult = await db.execute(sql.raw(countQuery));
    const curtailmentCount = parseInt(String(countResult.rows[0]?.record_count) || '0');
    
    if (curtailmentCount === 0) {
      console.log(`No curtailment records found for ${date}`);
      return { curtailmentCount: 0, calculationsGenerated: 0 };
    }
    
    console.log(`Found ${curtailmentCount} curtailment records for ${date}`);
    
    if (!applyFix) {
      return { curtailmentCount, calculationsGenerated: 0 };
    }
    
    // Delete existing bitcoin calculations for this date
    console.log(`Deleting existing bitcoin calculations for ${date}...`);
    const deleteQuery = `
      DELETE FROM historical_bitcoin_calculations
      WHERE settlement_date = '${date}'
    `;
    
    await db.execute(sql.raw(deleteQuery));
    console.log(`Deleted existing bitcoin calculations for ${date}`);
    
    // Create a temporary table for bulk loading, including curtailment_id to avoid duplicates
    const createTempTableQuery = `
      CREATE TEMP TABLE temp_bitcoin_calculations (
        settlement_date DATE NOT NULL,
        settlement_period INTEGER NOT NULL,
        farm_id TEXT NOT NULL,
        miner_model TEXT NOT NULL,
        bitcoin_mined NUMERIC NOT NULL,
        difficulty NUMERIC NOT NULL,
        calculated_at TIMESTAMP NOT NULL,
        curtailment_id INTEGER
      ) ON COMMIT DROP;
    `;
    
    await db.execute(sql.raw(createTempTableQuery));
    console.log(`Created temporary table for calculations`);
    
    // Get all curtailment records for this date
    const curtailmentQuery = `
      SELECT 
        id,
        settlement_date,
        settlement_period,
        farm_id,
        ABS(volume::numeric) as volume
      FROM 
        curtailment_records
      WHERE 
        settlement_date = '${date}'
        AND ABS(volume::numeric) > 0
      ORDER BY
        settlement_period, farm_id, id
    `;
    
    const curtailmentResult = await db.execute(sql.raw(curtailmentQuery));
    const curtailmentRecords = curtailmentResult.rows;
    
    // For each miner model, generate bitcoin calculations
    let totalCalculationsGenerated = 0;
    
    for (const minerModel of MINER_MODELS) {
      console.log(`Generating calculations for ${minerModel}...`);
      
      // Prepare batch insert data
      const batchInsertData = curtailmentRecords.map(record => {
        const volume = parseFloat(String(record.volume));
        const bitcoinMined = calculateBitcoinForRecord(volume, minerModel, parseFloat(difficulty));
        
        return {
          settlement_date: record.settlement_date,
          settlement_period: record.settlement_period,
          farm_id: record.farm_id,
          miner_model: minerModel,
          bitcoin_mined: bitcoinMined.toFixed(8),
          difficulty,
          calculated_at: new Date().toISOString(),
          curtailment_id: record.id
        };
      });
      
      // Insert in batches using raw SQL
      const BATCH_SIZE = 500;
      
      for (let i = 0; i < batchInsertData.length; i += BATCH_SIZE) {
        const batch = batchInsertData.slice(i, i + BATCH_SIZE);
        
        // Create values string with all records
        const valuesString = batch.map(record => `(
          '${record.settlement_date}', 
          ${record.settlement_period}, 
          '${record.farm_id}', 
          '${record.miner_model}', 
          ${record.bitcoin_mined}, 
          ${record.difficulty}, 
          '${record.calculated_at}', 
          ${record.curtailment_id}
        )`).join(', ');
        
        const insertTempQuery = `
          INSERT INTO temp_bitcoin_calculations (
            settlement_date,
            settlement_period,
            farm_id,
            miner_model,
            bitcoin_mined,
            difficulty,
            calculated_at,
            curtailment_id
          )
          VALUES ${valuesString}
        `;
        
        await db.execute(sql.raw(insertTempQuery));
      }
      
      console.log(`Generated ${batchInsertData.length} calculations for ${minerModel}`);
      totalCalculationsGenerated += batchInsertData.length;
    }
    
    // Now, insert from the temporary table to the real table
    console.log(`Inserting calculations from temporary table to historical_bitcoin_calculations...`);
    const finalInsertQuery = `
      INSERT INTO historical_bitcoin_calculations 
        (settlement_date, settlement_period, farm_id, miner_model, bitcoin_mined, difficulty, calculated_at, curtailment_id)
      SELECT 
        settlement_date, settlement_period, farm_id, miner_model, bitcoin_mined, difficulty, calculated_at, curtailment_id
      FROM 
        temp_bitcoin_calculations;
    `;
    
    await db.execute(sql.raw(finalInsertQuery));
    
    console.log(`Total calculations generated for ${date}: ${totalCalculationsGenerated}`);
    return { curtailmentCount, calculationsGenerated: totalCalculationsGenerated };
    
  } catch (error) {
    console.error(`Error processing ${date}:`, error);
    throw error;
  }
}

// Process a date range
async function processDateRange(
  startDate: string, 
  endDate: string, 
  applyFix: boolean = false,
  analyzeOnly: boolean = false
): Promise<void> {
  // First, analyze the current state
  await analyzeCalculations(startDate, endDate);
  
  if (analyzeOnly) {
    console.log("\nAnalysis-only mode - not processing any dates");
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
  
  if (!applyFix) {
    console.log(`\nSimulation mode - would process ${dates.length} dates but not applying changes`);
    return;
  }
  
  console.log(`\nProcessing ${dates.length} dates from ${startDate} to ${endDate}`);
  
  // Keep track of progress
  let totalCurtailmentRecords = 0;
  let totalCalculationsGenerated = 0;
  
  // Process each date
  for (const date of dates) {
    const result = await regenerateCalculations(date, applyFix);
    totalCurtailmentRecords += result.curtailmentCount;
    totalCalculationsGenerated += result.calculationsGenerated;
  }
  
  // Print summary
  console.log("\n=== Processing Summary ===");
  console.log(`Total curtailment records processed: ${totalCurtailmentRecords}`);
  console.log(`Total bitcoin calculations generated: ${totalCalculationsGenerated}`);
  
  // Analyze again to confirm changes
  if (applyFix) {
    console.log("\n=== Final State After Processing ===");
    await analyzeCalculations(startDate, endDate);
  }
}

// Main function
async function main() {
  const args = process.argv.slice(2);
  
  let startDate = '2025-01-01';
  let endDate = format(new Date(), 'yyyy-MM-dd');
  let applyFix = false;
  let analyzeOnly = false;
  
  // Parse command line arguments
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--start' && i + 1 < args.length) {
      startDate = args[i + 1];
    } else if (args[i] === '--end' && i + 1 < args.length) {
      endDate = args[i + 1];
    } else if (args[i] === '--fix') {
      applyFix = true;
    } else if (args[i] === '--analyze-only') {
      analyzeOnly = true;
    }
  }
  
  console.log('=== Bitcoin Calculation Regeneration Tool ===');
  console.log(`Start date: ${startDate}`);
  console.log(`End date: ${endDate}`);
  if (applyFix) {
    console.log(`Mode: Applying fixes (THIS WILL DELETE AND REGENERATE DATA!)`);
  } else if (analyzeOnly) {
    console.log(`Mode: Analysis only (no changes will be made)`);
  } else {
    console.log(`Mode: Simulation (changes will be calculated but not applied)`);
  }
  
  try {
    await processDateRange(startDate, endDate, applyFix, analyzeOnly);
    console.log('\nProcessing completed successfully');
  } catch (error) {
    console.error('\nProcessing failed:', error);
    process.exit(1);
  }
}

// Run the main function
main().catch(console.error);
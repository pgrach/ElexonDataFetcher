/**
 * Fix Bitcoin Calculations for March 31, 2025 using Historical Difficulty
 * 
 * This script corrects the Bitcoin calculations for March 31, 2025 by:
 * 1. Fetching the proper historical difficulty from DynamoDB for the specific date
 * 2. Deleting existing Bitcoin calculation records for that date
 * 3. Recalculating with the correct historical difficulty
 * 4. Updating the summary tables
 */

import { db } from "../db";
import { historicalBitcoinCalculations, bitcoinMonthlySummaries, bitcoinYearlySummaries } from "../db/schema";
import { and, eq, sql } from "drizzle-orm";
import { format } from "date-fns";
import { getDifficultyData } from "../server/services/dynamodbService";
import { minerModels } from "../server/types/bitcoin";
import fs from 'fs';

// Target date to fix
const TARGET_DATE = "2025-03-31";
const TARGET_MONTH = "2025-03";
const TARGET_YEAR = "2025";

// Configure logging
const logFile = `logs/fix_march_31_historical_difficulty_${format(new Date(), 'yyyy-MM-dd\'T\'HH-mm-ss')}.log`;
const originalConsoleLog = console.log;
const originalConsoleError = console.error;
const originalConsoleInfo = console.info;
const originalConsoleWarn = console.warn;

// Create a function to log to both console and file
function setupLogging() {
  const logStream = fs.createWriteStream(logFile, { flags: 'a' });
  
  console.log = function(...args) {
    const output = args.map(arg => 
      typeof arg === 'object' ? JSON.stringify(arg, null, 2) : arg
    ).join(' ');
    logStream.write(output + '\n');
    originalConsoleLog.apply(console, args);
  };
  
  console.error = function(...args) {
    const output = args.map(arg => 
      typeof arg === 'object' ? JSON.stringify(arg, null, 2) : arg
    ).join(' ');
    logStream.write('[ERROR] ' + output + '\n');
    originalConsoleError.apply(console, args);
  };
  
  console.info = function(...args) {
    const output = args.map(arg => 
      typeof arg === 'object' ? JSON.stringify(arg, null, 2) : arg
    ).join(' ');
    logStream.write('[INFO] ' + output + '\n');
    originalConsoleInfo.apply(console, args);
  };
  
  console.warn = function(...args) {
    const output = args.map(arg => 
      typeof arg === 'object' ? JSON.stringify(arg, null, 2) : arg
    ).join(' ');
    logStream.write('[WARN] ' + output + '\n');
    originalConsoleWarn.apply(console, args);
  };
  
  process.on('exit', () => {
    logStream.end();
  });
}

/**
 * Get curtailment records for the target date
 */
async function getCurtailmentRecords(date: string) {
  const query = sql`
    SELECT 
      cr.settlement_date,
      cr.settlement_period,
      cr.farm_id,
      cr.volume,
      cr.lead_party_name
    FROM curtailment_records cr
    WHERE cr.settlement_date = ${date}
    ORDER BY cr.settlement_period, cr.farm_id
  `;

  const records = await db.execute(query);
  console.log(`Found ${records.rows.length} curtailment records for ${date}`);
  return records.rows;
}

/**
 * Calculate Bitcoin mined for a specific volume and miner model
 */
function calculateBitcoin(volumeMwh: number, minerModel: string, difficulty: number): number {
  // Volume should always be positive (absolute value is taken in the calling function)
  if (volumeMwh < 0) {
    console.warn(`Warning: Negative volume (${volumeMwh}) received in calculateBitcoin. Converting to absolute value.`);
    volumeMwh = Math.abs(volumeMwh);
  }
  
  // Get miner stats
  const { hashrate, power } = minerModels[minerModel];
  
  // Energy in kWh
  const energyKwh = volumeMwh * 1000;
  
  // Hours the miner can run
  const hoursRunning = energyKwh / power * 1000;
  
  // Bitcoin calculation formula
  // bitcoinMined = (hashrate * blockReward * timeInSeconds) / difficulty
  const blockReward = 6.25; // Current Bitcoin block reward
  const timeInSeconds = hoursRunning * 3600;
  
  const bitcoinMined = (hashrate * blockReward * timeInSeconds) / difficulty;
  return bitcoinMined;
}

/**
 * Count existing Bitcoin calculation records for the target date and miner model
 * We'll use UPSERT instead of DELETE + INSERT to handle duplicates
 */
async function countExistingRecords(date: string, minerModel: string): Promise<number> {
  try {
    const result = await db.execute(sql`
      SELECT COUNT(*) as count
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${date}
      AND miner_model = ${minerModel}
    `);
    
    const count = parseInt(result.rows[0].count as string);
    console.log(`Found ${count} existing records for ${date} and ${minerModel}`);
    return count;
  } catch (error) {
    console.error(`Error counting records for ${date} and ${minerModel}:`, error);
    return 0;
  }
}

/**
 * Process all curtailment records for a date and miner model
 */
async function processRecords(date: string, minerModel: string, difficulty: number) {
  console.log(`Processing records for ${date} and ${minerModel} with difficulty ${difficulty}`);
  
  const records = await getCurtailmentRecords(date);
  let insertCount = 0;
  let totalBitcoin = 0;
  
  for (const record of records) {
    // Use absolute value to ensure positive energy values for calculations
    const volumeMwh = Math.abs(parseFloat(record.volume as string));
    if (insertCount % 100 === 0) {
      console.log(`Processing record: period=${record.settlement_period}, farm=${record.farm_id}, volume=${volumeMwh} MWh (absolute value)`);
    }
    
    const bitcoinMined = calculateBitcoin(volumeMwh, minerModel, difficulty);
    totalBitcoin += bitcoinMined;
    
    // Direct SQL execution of UPSERT
    try {
      const upsertQuery = sql`
        INSERT INTO historical_bitcoin_calculations (
          settlement_date, settlement_period, farm_id, miner_model, bitcoin_mined, difficulty, calculated_at
        ) VALUES (
          ${date}, 
          ${record.settlement_period}, 
          ${record.farm_id}, 
          ${minerModel}, 
          ${bitcoinMined.toString()}, 
          ${difficulty.toString()}, 
          ${new Date()}
        )
        ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
        DO UPDATE SET 
          bitcoin_mined = ${bitcoinMined.toString()}, 
          difficulty = ${difficulty.toString()},
          calculated_at = ${new Date()}
      `;
      
      await db.execute(upsertQuery);
      insertCount++;
      
      if (insertCount % 50 === 0) {
        console.log(`Processed ${insertCount}/${records.length} records...`);
      }
    } catch (error) {
      console.error(`Error processing record for period ${record.settlement_period}, farm ${record.farm_id}:`, error);
    }
  }
  
  console.log(`Completed processing ${insertCount}/${records.length} records for ${minerModel}`);
  console.log(`Total Bitcoin calculated: ${totalBitcoin} BTC`);
  
  return { insertCount, totalBitcoin };
}

/**
 * Update Bitcoin monthly summaries
 */
async function updateMonthlySummaries(yearMonth: string) {
  try {
    // Update monthly summaries for each miner model
    for (const minerModel of Object.keys(minerModels)) {
      const result = await db.execute(sql`
        UPDATE bitcoin_monthly_summaries
        SET
          bitcoin_mined = (
            SELECT SUM(bitcoin_mined::numeric)
            FROM historical_bitcoin_calculations
            WHERE settlement_date >= ${yearMonth + '-01'}
            AND settlement_date <= ${yearMonth + '-31'}
            AND miner_model = ${minerModel}
          ),
          updated_at = NOW()
        WHERE year_month = ${yearMonth} AND miner_model = ${minerModel}
      `);
      
      console.log(`Updated monthly summary for ${yearMonth} and ${minerModel}: ${result.rowCount} rows`);
    }
    
    return true;
  } catch (error) {
    console.error(`Error updating monthly summaries for ${yearMonth}:`, error);
    return false;
  }
}

/**
 * Update Bitcoin yearly summaries
 */
async function updateYearlySummaries(year: string) {
  try {
    // Update yearly summaries for each miner model
    for (const minerModel of Object.keys(minerModels)) {
      const result = await db.execute(sql`
        UPDATE bitcoin_yearly_summaries
        SET 
          bitcoin_mined = (
            SELECT SUM(bitcoin_mined::numeric)
            FROM bitcoin_monthly_summaries
            WHERE year_month LIKE ${year + '-%'} AND miner_model = ${minerModel}
          ),
          updated_at = NOW()
        WHERE year = ${year} AND miner_model = ${minerModel}
      `);
      
      console.log(`Updated yearly summary for ${year} and ${minerModel}: ${result.rowCount} rows`);
    }
    
    return true;
  } catch (error) {
    console.error(`Error updating yearly summaries for ${year}:`, error);
    return false;
  }
}

/**
 * Main function to fix Bitcoin calculations for March 31
 */
async function main() {
  setupLogging();
  console.log(`=== Starting fix for Bitcoin calculations on ${TARGET_DATE} ===`);
  const startTime = Date.now();
  
  try {
    // Step 1: Fetch the historical difficulty from DynamoDB
    console.log(`\nFetching historical difficulty for ${TARGET_DATE} from DynamoDB...`);
    const difficulty = await getDifficultyData(TARGET_DATE);
    console.log(`Retrieved historical difficulty: ${difficulty}`);
    
    // Step 2: Process each miner model with the correct historical difficulty
    const minerModelList = Object.keys(minerModels);
    console.log(`\nProcessing ${minerModelList.length} miner models...`);
    
    for (const minerModel of minerModelList) {
      console.log(`\n- Processing ${minerModel}`);
      
      // Step 2.1: Check existing records
      const existingCount = await countExistingRecords(TARGET_DATE, minerModel);
      console.log(`Will update ${existingCount} existing records or insert new ones for ${minerModel}`);
      
      // Step 2.2: Process all records with the correct difficulty using UPSERT
      const { insertCount, totalBitcoin } = await processRecords(TARGET_DATE, minerModel, difficulty);
      console.log(`Processed ${insertCount} records with total ${totalBitcoin} BTC for ${minerModel}`);
    }
    
    // Step 3: Update monthly and yearly summaries
    console.log(`\nUpdating monthly summaries for ${TARGET_MONTH}...`);
    await updateMonthlySummaries(TARGET_MONTH);
    
    console.log(`\nUpdating yearly summaries for ${TARGET_YEAR}...`);
    await updateYearlySummaries(TARGET_YEAR);
    
    // Print summary
    const durationMs = Date.now() - startTime;
    const durationMinutes = Math.floor(durationMs / 60000);
    const durationSeconds = ((durationMs % 60000) / 1000).toFixed(2);
    
    console.log(`\n=== Fix completed successfully ===`);
    console.log(`Execution time: ${durationMinutes}m ${durationSeconds}s`);
    console.log(`Log file: ${logFile}`);
    
  } catch (error) {
    console.error(`Error in main process:`, error);
  }
}

// Run the script
main().catch(console.error);
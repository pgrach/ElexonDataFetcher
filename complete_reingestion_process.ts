/**
 * Complete Reingestion Process for Bitcoin Mining Analytics
 * 
 * This script provides a comprehensive solution for reingesting Elexon API data for a specific date,
 * processing curtailment records, and calculating Bitcoin mining potential across all settlement
 * periods for multiple miner models.
 * 
 * Features:
 * - Handles API timeouts and connection issues
 * - Prevents duplicate records using ON CONFLICT clauses
 * - Processes data in efficient batches to avoid memory issues
 * - Supports all 48 settlement periods and multiple miner models
 * - Includes comprehensive logging and verification
 * 
 * Usage:
 *   npx tsx complete_reingestion_process.ts [date]
 * 
 * Example:
 *   npx tsx complete_reingestion_process.ts 2025-03-04
 */

import pg from 'pg';
import { exec } from 'child_process';
import { promisify } from 'util';
import * as fs from 'fs';
import { parse } from 'path';

// Promisify exec for cleaner async usage
const execAsync = promisify(exec);

// Configuration options
const DEFAULT_DATE = "2025-03-04"; // Use this date if none provided
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const DEFAULT_DIFFICULTY = 108105433845147;
const BATCH_SIZE = 8; // Number of periods to process in each batch

// ANSI color codes for console output
const colors = {
  reset: "\x1b[0m",
  bright: "\x1b[1m",
  red: "\x1b[31m",
  green: "\x1b[32m", 
  yellow: "\x1b[33m",
  blue: "\x1b[36m",
  magenta: "\x1b[35m"
};

// Create PostgreSQL pool
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL
});

/**
 * Log messages with consistent formatting and coloring
 */
function log(message: string, type: "info" | "success" | "warning" | "error" | "title" = "info"): void {
  const timestamp = new Date().toISOString().split('T')[1].replace('Z', '');
  
  switch (type) {
    case "title":
      console.log(`${colors.bright}${colors.magenta}${message}${colors.reset}`);
      break;
    case "info":
      console.log(`[${timestamp}] ${colors.blue}${message}${colors.reset}`);
      break;
    case "success":
      console.log(`[${timestamp}] ${colors.green}${message}${colors.reset}`);
      break;
    case "warning":
      console.log(`[${timestamp}] ${colors.yellow}${message}${colors.reset}`);
      break;
    case "error":
      console.log(`[${timestamp}] ${colors.red}${message}${colors.reset}`);
      break;
  }
}

/**
 * Get information about a specific miner model (hashrate and power consumption)
 */
function getMinerModelInfo(minerModel: string): { hashrate: number, power: number } {
  switch (minerModel) {
    case 'S19J_PRO':
      return { hashrate: 104, power: 3068 };
    case 'S9':
      return { hashrate: 13.5, power: 1323 };
    case 'M20S':
      return { hashrate: 68, power: 3360 };
    default:
      throw new Error(`Unknown miner model: ${minerModel}`);
  }
}

/**
 * Check if a date has curtailment data already present
 */
async function checkCurtailmentData(date: string): Promise<{
  exists: boolean;
  count: number;
  periods: number;
  totalVolume: string;
}> {
  const client = await pool.connect();
  
  try {
    const result = await client.query(`
      SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT settlement_period) as period_count,
        COALESCE(SUM(ABS(volume::numeric)), 0) as total_volume
      FROM 
        curtailment_records
      WHERE 
        settlement_date = $1
    `, [date]);
    
    const row = result.rows[0];
    
    return {
      exists: parseInt(row.record_count) > 0,
      count: parseInt(row.record_count),
      periods: parseInt(row.period_count),
      totalVolume: parseFloat(row.total_volume).toFixed(2)
    };
  } finally {
    client.release();
  }
}

/**
 * Clear existing curtailment records for a specific date
 */
async function clearCurtailmentRecords(date: string): Promise<number> {
  const client = await pool.connect();
  
  try {
    // First get the count so we can report it
    const countResult = await client.query(
      'SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = $1',
      [date]
    );
    const count = parseInt(countResult.rows[0].count);
    
    // Now delete the records
    await client.query(
      'DELETE FROM curtailment_records WHERE settlement_date = $1',
      [date]
    );
    
    return count;
  } finally {
    client.release();
  }
}

/**
 * Clear existing Bitcoin calculations for a specific date
 */
async function clearBitcoinCalculations(date: string): Promise<{[key: string]: number}> {
  const client = await pool.connect();
  
  try {
    // Get counts by miner model first
    const countResult = await client.query(`
      SELECT 
        miner_model, 
        COUNT(*) as count
      FROM 
        historical_bitcoin_calculations 
      WHERE 
        settlement_date = $1
      GROUP BY
        miner_model
    `, [date]);
    
    const counts: {[key: string]: number} = {};
    countResult.rows.forEach(row => {
      counts[row.miner_model] = parseInt(row.count);
    });
    
    // Delete all records for the date
    await client.query(
      'DELETE FROM historical_bitcoin_calculations WHERE settlement_date = $1',
      [date]
    );
    
    return counts;
  } finally {
    client.release();
  }
}

/**
 * Process curtailment data reingestion for a specific date
 */
async function processCurtailmentData(date: string): Promise<{
  success: boolean;
  records: number;
  periods: number;
  totalMwh: number;
}> {
  try {
    // Run the external reingestion script to fetch from Elexon API
    log(`Starting curtailment data reingestion for ${date}...`, "info");
    
    // Clear existing records first
    await clearCurtailmentRecords(date);
    
    const { stdout, stderr } = await execAsync(`
      # Check for missing periods first
      log_file="reingestion_${date.replace(/-/g, '')}.log"
      echo "Starting reingestion for ${date}" > $log_file
      
      # Process data in batches to avoid timeouts
      echo "Processing data in batches..." >> $log_file
      
      # Process periods 1-16 (batch 1)
      npx tsx ./batch_process_periods.ts ${date} 1 16 >> $log_file
      
      # Process periods 17-32 (batch 2)
      npx tsx ./batch_process_periods.ts ${date} 17 32 >> $log_file
      
      # Process periods 33-48 (batch 3)
      npx tsx ./batch_process_periods.ts ${date} 33 48 >> $log_file
      
      echo "Reingestion complete!" >> $log_file
    `);
    
    // Check the result after reingestion
    const status = await checkCurtailmentData(date);
    
    if (status.periods === 48) {
      log(`Successfully processed all 48 settlement periods for ${date}`, "success");
      log(`Ingested ${status.count} curtailment records with total volume ${status.totalVolume} MWh`, "success");
      
      return {
        success: true,
        records: status.count,
        periods: status.periods,
        totalMwh: parseFloat(status.totalVolume)
      };
    } else {
      log(`Incomplete data processing: Only ${status.periods} of 48 periods processed`, "warning");
      
      return {
        success: false,
        records: status.count,
        periods: status.periods,
        totalMwh: parseFloat(status.totalVolume)
      };
    }
  } catch (error) {
    log(`Error during curtailment data processing: ${error}`, "error");
    
    return {
      success: false,
      records: 0,
      periods: 0,
      totalMwh: 0
    };
  }
}

/**
 * Process Bitcoin calculations for a specific date, period, and miner model
 */
async function processBitcoinCalculationsForPeriod(
  date: string,
  period: number,
  minerModel: string,
  difficulty: number
): Promise<number> {
  const client = await pool.connect();
  
  try {
    const minerInfo = getMinerModelInfo(minerModel);
    
    // Complex transaction that:
    // 1. Gets curtailment records for the specific period
    // 2. Calculates Bitcoin based on miner parameters and curtailment volume
    // 3. Inserts calculation records with ON CONFLICT DO NOTHING to handle duplicates
    const query = `
      WITH curtailment_data AS (
        SELECT 
          settlement_date,
          settlement_period,
          farm_id,
          ABS(volume::numeric) AS curtailed_mwh
        FROM 
          curtailment_records
        WHERE 
          settlement_date = $1
          AND settlement_period = $2
          AND ABS(volume::numeric) > 0
      ),
      calculation_params AS (
        SELECT
          $3::text AS miner_model,
          $4::numeric AS difficulty,
          ${minerInfo.hashrate}::numeric AS hashrate,
          ${minerInfo.power}::numeric AS power
      ),
      calculation_data AS (
        SELECT
          cd.settlement_date,
          cd.settlement_period,
          cd.farm_id,
          cp.miner_model,
          cp.difficulty,
          cd.curtailed_mwh,
          -- Calculate Bitcoin based on hashrate, power, and difficulty
          (
            FLOOR(
              (cd.curtailed_mwh * 1000) / 
              ((cp.power / 1000.0) * (30.0 / 60.0))
            ) * cp.hashrate / 
            ((cp.difficulty * POWER(2, 32)) / 600 / 1000000000000)
          ) * 3.125 * 3 AS bitcoin_mined
        FROM
          curtailment_data cd,
          calculation_params cp
      ),
      inserted AS (
        INSERT INTO historical_bitcoin_calculations (
          settlement_date,
          settlement_period,
          farm_id,
          miner_model,
          difficulty,
          bitcoin_mined,
          calculated_at
        )
        SELECT
          settlement_date,
          settlement_period,
          farm_id,
          miner_model,
          difficulty,
          bitcoin_mined,
          NOW() AS calculated_at
        FROM
          calculation_data
        ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) DO NOTHING
        RETURNING *
      )
      SELECT COUNT(*) AS inserted_count FROM inserted;
    `;
    
    const result = await client.query(query, [date, period, minerModel, difficulty.toString()]);
    const insertedCount = parseInt(result.rows[0].inserted_count);
    
    return insertedCount;
  } catch (error) {
    log(`Error processing period ${period} for ${minerModel}: ${error}`, "error");
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Process Bitcoin calculations for all periods in a batch
 */
async function processBitcoinBatch(
  date: string,
  startPeriod: number,
  endPeriod: number,
  minerModel: string,
  difficulty: number
): Promise<{
  success: boolean;
  recordsProcessed: number;
  periodsProcessed: number;
}> {
  try {
    log(`Processing Bitcoin calculations for periods ${startPeriod}-${endPeriod} with ${minerModel}...`, "info");
    
    let totalRecords = 0;
    let periodsProcessed = 0;
    
    // Process each period in the batch
    for (let period = startPeriod; period <= endPeriod; period++) {
      try {
        const recordsInserted = await processBitcoinCalculationsForPeriod(date, period, minerModel, difficulty);
        totalRecords += recordsInserted;
        periodsProcessed++;
        
        log(`Period ${period}: inserted ${recordsInserted} records for ${minerModel}`, 
            recordsInserted > 0 ? "success" : "warning");
        
        // Short pause between periods to avoid overloading the database
        await new Promise(resolve => setTimeout(resolve, 100));
      } catch (error) {
        log(`Failed to process period ${period}: ${error}`, "error");
      }
    }
    
    return {
      success: periodsProcessed === (endPeriod - startPeriod + 1),
      recordsProcessed: totalRecords,
      periodsProcessed: periodsProcessed
    };
  } catch (error) {
    log(`Error processing batch ${startPeriod}-${endPeriod}: ${error}`, "error");
    
    return {
      success: false,
      recordsProcessed: 0,
      periodsProcessed: 0
    };
  }
}

/**
 * Process Bitcoin calculations for all 48 periods and a specific miner model
 */
async function processBitcoinCalculations(
  date: string,
  minerModel: string,
  difficulty: number
): Promise<{
  success: boolean;
  recordsProcessed: number;
  periodsProcessed: number;
}> {
  try {
    log(`Starting Bitcoin calculations for ${date} with ${minerModel}...`, "info");
    
    let totalRecords = 0;
    let totalPeriods = 0;
    
    // Process in batches to avoid timeouts
    for (let startPeriod = 1; startPeriod <= 48; startPeriod += BATCH_SIZE) {
      const endPeriod = Math.min(startPeriod + BATCH_SIZE - 1, 48);
      
      const batchResult = await processBitcoinBatch(
        date,
        startPeriod,
        endPeriod,
        minerModel,
        difficulty
      );
      
      totalRecords += batchResult.recordsProcessed;
      totalPeriods += batchResult.periodsProcessed;
      
      // Small pause between batches to avoid database overload
      await new Promise(resolve => setTimeout(resolve, 500));
    }
    
    return {
      success: totalPeriods === 48,
      recordsProcessed: totalRecords,
      periodsProcessed: totalPeriods
    };
  } catch (error) {
    log(`Error processing Bitcoin calculations for ${minerModel}: ${error}`, "error");
    
    return {
      success: false,
      recordsProcessed: 0,
      periodsProcessed: 0
    };
  }
}

/**
 * Verify Bitcoin calculations for a specific date and miner model
 */
async function verifyBitcoinCalculations(
  date: string, 
  minerModel?: string
): Promise<{
  [model: string]: {
    records: number;
    periods: number;
    minPeriod: number;
    maxPeriod: number;
    totalBitcoin: number;
    isComplete: boolean;
  }
}> {
  const client = await pool.connect();
  
  try {
    // Build the query based on whether a specific model is requested
    let query = `
      SELECT 
        miner_model,
        COUNT(*) AS record_count,
        COUNT(DISTINCT settlement_period) AS period_count,
        MIN(settlement_period) AS min_period,
        MAX(settlement_period) AS max_period,
        COALESCE(SUM(bitcoin_mined::numeric), 0) AS total_bitcoin
      FROM 
        historical_bitcoin_calculations
      WHERE 
        settlement_date = $1
    `;
    
    const params = [date];
    
    // Add model filter if specified
    if (minerModel) {
      query += ` AND miner_model = $2`;
      params.push(minerModel);
    }
    
    // Group by miner model
    query += ` GROUP BY miner_model`;
    
    const result = await client.query(query, params);
    
    // Transform the results into a more structured format
    const verificationResults: {
      [model: string]: {
        records: number;
        periods: number;
        minPeriod: number;
        maxPeriod: number;
        totalBitcoin: number;
        isComplete: boolean;
      }
    } = {};
    
    result.rows.forEach(row => {
      const model = row.miner_model;
      const periods = parseInt(row.period_count);
      const minPeriod = parseInt(row.min_period);
      const maxPeriod = parseInt(row.max_period);
      
      verificationResults[model] = {
        records: parseInt(row.record_count),
        periods: periods,
        minPeriod: minPeriod,
        maxPeriod: maxPeriod,
        totalBitcoin: parseFloat(row.total_bitcoin),
        isComplete: periods === 48 && minPeriod === 1 && maxPeriod === 48
      };
    });
    
    return verificationResults;
  } finally {
    client.release();
  }
}

/**
 * Complete reingestion and processing for a specified date
 */
async function completeReingestion(date: string): Promise<void> {
  try {
    log(`Starting complete reingestion process for ${date}`, "title");
    
    // Step 1: Check if data already exists
    const curtailmentStatus = await checkCurtailmentData(date);
    
    if (curtailmentStatus.exists) {
      log(`Found existing curtailment data: ${curtailmentStatus.count} records across ${curtailmentStatus.periods} periods`, "info");
      
      if (curtailmentStatus.periods < 48) {
        log(`Data is incomplete (only ${curtailmentStatus.periods}/48 periods). Clearing and reprocessing.`, "warning");
        const deleted = await clearCurtailmentRecords(date);
        log(`Cleared ${deleted} existing curtailment records`, "info");
      } else {
        log(`Data appears complete with ${curtailmentStatus.periods}/48 periods.`, "success");
      }
    }
    
    // Step 2: Process curtailment data reingestion
    const reingestionResult = await processCurtailmentData(date);
    
    if (!reingestionResult.success) {
      log(`Curtailment data processing was not fully successful. Attempting to continue anyway.`, "warning");
    } else {
      log(`Curtailment data processing complete: ${reingestionResult.records} records, ${reingestionResult.totalMwh.toFixed(2)} MWh`, "success");
    }
    
    // Step 3: Clear existing Bitcoin calculations
    const bitcoinCounts = await clearBitcoinCalculations(date);
    let totalCleared = 0;
    
    for (const model in bitcoinCounts) {
      log(`Cleared ${bitcoinCounts[model]} Bitcoin calculations for ${model}`, "info");
      totalCleared += bitcoinCounts[model];
    }
    
    log(`Cleared ${totalCleared} total Bitcoin calculations`, totalCleared > 0 ? "success" : "info");
    
    // Step 4: Process Bitcoin calculations for each miner model
    const bitcoinResults: {
      [model: string]: {
        success: boolean;
        records: number;
        periods: number;
      }
    } = {};
    
    for (const minerModel of MINER_MODELS) {
      log(`Processing Bitcoin calculations for ${minerModel}...`, "info");
      
      const modelResult = await processBitcoinCalculations(date, minerModel, DEFAULT_DIFFICULTY);
      
      // Transform the result to match the expected format
      bitcoinResults[minerModel] = {
        success: modelResult.success,
        records: modelResult.recordsProcessed,
        periods: modelResult.periodsProcessed
      };
      
      log(`Completed ${minerModel} calculations: ${modelResult.recordsProcessed} records across ${modelResult.periodsProcessed} periods`, 
        modelResult.success ? "success" : "warning");
    }
    
    // Step 5: Verify the results
    log("Verification of Bitcoin Calculations:", "title");
    const verificationResults = await verifyBitcoinCalculations(date);
    
    for (const model in verificationResults) {
      const result = verificationResults[model];
      log(
        `${model}: ${result.records} records across ${result.periods} periods, total: ${result.totalBitcoin.toFixed(8)} BTC`,
        result.isComplete ? "success" : "warning"
      );
      
      if (!result.isComplete) {
        log(`Warning: ${model} calculations are incomplete (${result.periods}/48 periods)`, "warning");
      }
    }
    
    // Final summary
    log(`Reingestion and reconciliation process complete for ${date}`, "title");
    log(`Curtailment Records: ${reingestionResult.records} across ${reingestionResult.periods} periods (${reingestionResult.totalMwh.toFixed(2)} MWh)`, "info");
    
    for (const model in verificationResults) {
      const result = verificationResults[model];
      log(`${model}: ${result.records} records, ${result.totalBitcoin.toFixed(8)} BTC`, "info");
    }
    
  } catch (error) {
    log(`Error in reingestion process: ${error}`, "error");
  } finally {
    // Close the connection pool when done
    await pool.end();
  }
}

/**
 * Parse command line arguments and run the process
 */
async function main() {
  // Parse command line args
  const args = process.argv.slice(2);
  const date = args[0] || DEFAULT_DATE;
  
  // Run the complete reingestion process
  await completeReingestion(date);
}

// Execute the main function
main().catch(error => {
  log(`Fatal error: ${error}`, "error");
  process.exit(1);
});
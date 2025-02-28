/**
 * Minimal Reconciliation Tool
 *
 * A lightweight script designed to handle reconciliation in problematic cases where
 * regular tools may time out. This tool uses minimal database connections, small
 * batches, and sequential processing to prevent connection issues.
 *
 * Usage:
 * npx tsx minimal_reconciliation.ts [command] [options]
 *
 * Commands:
 *   sequence DATE BATCH_SIZE   - Process a specific date in small sequential batches
 *   critical-date DATE         - Fix a problematic date with extra safeguards
 *   most-critical              - Find and fix the most problematic date
 *   spot-fix DATE PERIOD FARM  - Fix a specific date-period-farm combination
 */

import { db } from "./db";
import { sql } from "drizzle-orm";
import pg from 'pg';
import fs from 'fs';

// Constants
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
const DEFAULT_BATCH_SIZE = 1;
const LOG_FILE = './minimal_reconciliation.log';
const CHECKPOINT_FILE = './minimal_checkpoint.json';

// Database pool with minimal connections
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 2, // Very limited number of connections
  idleTimeoutMillis: 10000,
  connectionTimeoutMillis: 5000,
  query_timeout: 10000,
  allowExitOnIdle: true
});

// Logging utility
function log(message: string, type: 'info' | 'error' | 'success' = 'info'): void {
  const timestamp = new Date().toISOString();
  const formatted = `[${timestamp}] [${type.toUpperCase()}] ${message}`;
  console.log(formatted);
  fs.appendFileSync(LOG_FILE, formatted + '\n');
}

// Sleep utility
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Find missing calculations for a specific date
 */
async function findMissingCombinations(date: string): Promise<Array<{
  period: number;
  farmId: string;
  minerModels: string[];
}>> {
  try {
    log(`Finding missing combinations for ${date}...`, 'info');
    
    const query = `
      WITH curtailment_data AS (
        SELECT DISTINCT
          settlement_period,
          farm_id
        FROM curtailment_records
        WHERE settlement_date = $1
          AND volume::numeric != 0
      ),
      bitcoin_data AS (
        SELECT DISTINCT
          settlement_period,
          farm_id,
          miner_model
        FROM historical_bitcoin_calculations
        WHERE settlement_date = $1
      ),
      missing AS (
        SELECT
          cd.settlement_period,
          cd.farm_id,
          ARRAY(
            SELECT unnest(ARRAY['S19J_PRO', 'S9', 'M20S'])
            EXCEPT
            SELECT bd.miner_model
            FROM bitcoin_data bd
            WHERE bd.settlement_period = cd.settlement_period
              AND bd.farm_id = cd.farm_id
          ) as missing_models
        FROM curtailment_data cd
      )
      SELECT
        settlement_period,
        farm_id,
        missing_models
      FROM missing
      WHERE array_length(missing_models, 1) > 0
      ORDER BY settlement_period, farm_id
    `;
    
    const client = await pool.connect();
    try {
      const result = await client.query(query, [date]);
      return result.rows.map(row => ({
        period: row.settlement_period,
        farmId: row.farm_id,
        minerModels: row.missing_models
      }));
    } finally {
      client.release();
    }
  } catch (error) {
    log(`Error finding missing combinations: ${error}`, 'error');
    return [];
  }
}

/**
 * Get curtailment record details
 */
async function getCurtailmentRecord(date: string, period: number, farmId: string): Promise<{
  volume: number;
  payment: number;
  leadPartyName?: string;
} | null> {
  const client = await pool.connect();
  try {
    const query = `
      SELECT 
        ABS(volume::numeric) as volume,
        ABS(payment::numeric) as payment,
        lead_party_name
      FROM curtailment_records
      WHERE settlement_date = $1
        AND settlement_period = $2
        AND farm_id = $3
    `;
    
    const result = await client.query(query, [date, period, farmId]);
    if (result.rows.length === 0) return null;
    
    return {
      volume: Number(result.rows[0].volume),
      payment: Number(result.rows[0].payment),
      leadPartyName: result.rows[0].lead_party_name
    };
  } finally {
    client.release();
  }
}

/**
 * Get Bitcoin difficulty for a date
 */
async function getDifficulty(date: string): Promise<number> {
  const client = await pool.connect();
  try {
    // First try to find from existing calculations
    const query = `
      SELECT difficulty::numeric as difficulty
      FROM historical_bitcoin_calculations
      WHERE settlement_date = $1
      LIMIT 1
    `;
    
    const result = await client.query(query, [date]);
    if (result.rows.length > 0) {
      return Number(result.rows[0].difficulty);
    }
    
    // Fall back to default difficulty
    return 108105433845147; // Default network difficulty as fallback
  } catch (error) {
    log(`Error getting difficulty: ${error}`, 'error');
    return 108105433845147;
  } finally {
    client.release();
  }
}

/**
 * Calculate Bitcoin for a single record
 */
function calculateBitcoin(
  volume: number,
  minerModel: string,
  difficulty: number
): number {
  // Basic miner stats
  const minerStats = {
    'S19J_PRO': { hashrate: 104, power: 3068 },
    'S9': { hashrate: 13.5, power: 1323 },
    'M20S': { hashrate: 68, power: 3360 }
  };
  
  const miner = minerStats[minerModel as keyof typeof minerStats];
  if (!miner) return 0;
  
  // Constants
  const BLOCK_REWARD = 3.125;
  const SETTLEMENT_PERIOD_MINUTES = 30;
  const BLOCKS_PER_SETTLEMENT_PERIOD = 3;
  
  // Calculation
  const curtailedKwh = volume * 1000;
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

/**
 * Insert or update a Bitcoin calculation record
 */
async function insertBitcoinCalculation(
  date: string,
  period: number,
  farmId: string,
  minerModel: string,
  bitcoinMined: number,
  difficulty: number
): Promise<boolean> {
  const client = await pool.connect();
  try {
    // First check if the record already exists
    const checkQuery = `
      SELECT id 
      FROM historical_bitcoin_calculations
      WHERE settlement_date = $1
        AND settlement_period = $2
        AND farm_id = $3
        AND miner_model = $4
    `;
    
    const checkResult = await client.query(checkQuery, [date, period, farmId, minerModel]);
    
    if (checkResult.rows.length > 0) {
      // Update existing record
      const updateQuery = `
        UPDATE historical_bitcoin_calculations
        SET bitcoin_mined = $5,
            difficulty = $6,
            calculated_at = NOW()
        WHERE id = $7
      `;
      
      await client.query(updateQuery, [
        bitcoinMined.toString(),
        difficulty.toString(),
        checkResult.rows[0].id
      ]);
      
      log(`Updated calculation for ${date} P${period} ${farmId} ${minerModel}`, 'success');
    } else {
      // Insert new record
      const insertQuery = `
        INSERT INTO historical_bitcoin_calculations (
          settlement_date,
          settlement_period,
          farm_id,
          miner_model,
          bitcoin_mined,
          difficulty,
          calculated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, NOW())
      `;
      
      await client.query(insertQuery, [
        date,
        period,
        farmId,
        minerModel,
        bitcoinMined.toString(),
        difficulty.toString()
      ]);
      
      log(`Inserted calculation for ${date} P${period} ${farmId} ${minerModel}`, 'success');
    }
    
    return true;
  } catch (error) {
    log(`Error inserting/updating calculation for ${date} P${period} ${farmId} ${minerModel}: ${error}`, 'error');
    return false;
  } finally {
    client.release();
  }
}

/**
 * Process a single combination with careful error handling
 */
async function processCombo(
  date: string,
  period: number,
  farmId: string,
  minerModel: string
): Promise<boolean> {
  try {
    log(`Processing ${date} P${period} ${farmId} ${minerModel}...`, 'info');
    
    // Get curtailment record
    const curtailment = await getCurtailmentRecord(date, period, farmId);
    if (!curtailment) {
      log(`No curtailment record found for ${date} P${period} ${farmId}`, 'error');
      return false;
    }
    
    // Get difficulty
    const difficulty = await getDifficulty(date);
    
    // Calculate Bitcoin
    const bitcoinMined = calculateBitcoin(curtailment.volume, minerModel, difficulty);
    
    // Insert or update
    return await insertBitcoinCalculation(
      date,
      period,
      farmId,
      minerModel,
      bitcoinMined,
      difficulty
    );
  } catch (error) {
    log(`Error processing ${date} P${period} ${farmId} ${minerModel}: ${error}`, 'error');
    return false;
  }
}

/**
 * Process a date in sequential batches with careful pauses between operations
 */
async function sequentialProcess(date: string, batchSize: number = DEFAULT_BATCH_SIZE): Promise<void> {
  log(`Starting sequential processing for ${date} with batch size ${batchSize}`, 'info');
  
  // Find all missing combinations
  const missingCombos = await findMissingCombinations(date);
  
  if (missingCombos.length === 0) {
    log(`No missing combinations found for ${date}`, 'success');
    return;
  }
  
  log(`Found ${missingCombos.length} combinations with missing calculations`, 'info');
  
  // Flatten to individual tasks
  const tasks: Array<{ period: number; farmId: string; minerModel: string }> = [];
  
  for (const combo of missingCombos) {
    for (const model of combo.minerModels) {
      tasks.push({
        period: combo.period,
        farmId: combo.farmId,
        minerModel: model
      });
    }
  }
  
  log(`Total tasks to process: ${tasks.length}`, 'info');
  
  // Process in small batches with pauses
  let processed = 0;
  let success = 0;
  let failed = 0;
  
  // Save checkpoint
  const checkpoint = {
    date,
    totalTasks: tasks.length,
    processed: 0,
    success: 0,
    failed: 0,
    startTime: Date.now()
  };
  
  fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2));
  
  for (let i = 0; i < tasks.length; i += batchSize) {
    const batch = tasks.slice(i, i + batchSize);
    
    log(`Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(tasks.length / batchSize)}`, 'info');
    
    // Process one-by-one
    for (const task of batch) {
      const result = await processCombo(date, task.period, task.farmId, task.minerModel);
      
      processed++;
      if (result) {
        success++;
      } else {
        failed++;
      }
      
      // Update checkpoint
      checkpoint.processed = processed;
      checkpoint.success = success;
      checkpoint.failed = failed;
      fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2));
      
      // Small pause between operations to avoid overwhelming the database
      await sleep(500);
    }
    
    // Pause between batches
    log(`Completed batch. Pausing before next batch...`, 'info');
    await sleep(2000);
    
    // Close and reestablish connection pool to avoid leaks
    if (i > 0 && i % (batchSize * 5) === 0) {
      log('Refreshing connection pool...', 'info');
      await pool.end();
      await sleep(2000);
      // Recreate pool
      Object.assign(pool, new pg.Pool({
        connectionString: process.env.DATABASE_URL,
        max: 2,
        idleTimeoutMillis: 10000,
        connectionTimeoutMillis: 5000,
        query_timeout: 10000,
        allowExitOnIdle: true
      }));
    }
  }
  
  // Final status
  log(`\n=== Processing Complete for ${date} ===`, 'info');
  log(`Total Processed: ${processed}`, 'info');
  log(`Success: ${success}`, 'success');
  log(`Failed: ${failed}`, failed > 0 ? 'error' : 'info');
  
  // Verify
  const remainingMissing = await findMissingCombinations(date);
  if (remainingMissing.length === 0) {
    log(`✅ Verification successful. All combinations for ${date} are now reconciled.`, 'success');
  } else {
    log(`⚠️ Verification found ${remainingMissing.length} combinations still missing calculations.`, 'error');
    log('Try running the process again or fixing individual combinations.', 'info');
  }
}

/**
 * Fix a critical date with extreme caution
 */
async function fixCriticalDate(date: string): Promise<void> {
  log(`Starting critical date fix for ${date}`, 'info');
  
  // Use a very small batch size for critical dates
  await sequentialProcess(date, 1);
}

/**
 * Find the most critical date (date with most missing calculations)
 */
async function findMostCriticalDate(): Promise<string | null> {
  try {
    log('Finding the date with the most missing calculations...', 'info');
    
    const query = `
      WITH curtailment_summary AS (
        SELECT 
          settlement_date,
          COUNT(DISTINCT (settlement_period || '-' || farm_id)) * 3 as expected_count
        FROM curtailment_records
        GROUP BY settlement_date
      ),
      bitcoin_summary AS (
        SELECT 
          settlement_date,
          COUNT(*) as actual_count
        FROM historical_bitcoin_calculations
        GROUP BY settlement_date
      )
      SELECT 
        cs.settlement_date::text as date,
        cs.expected_count,
        COALESCE(bs.actual_count, 0) as actual_count,
        cs.expected_count - COALESCE(bs.actual_count, 0) as missing_count,
        CASE 
          WHEN cs.expected_count = 0 THEN 100
          ELSE ROUND((COALESCE(bs.actual_count, 0) * 100.0) / cs.expected_count, 2)
        END as completion_percentage
      FROM curtailment_summary cs
      LEFT JOIN bitcoin_summary bs ON cs.settlement_date = bs.settlement_date
      WHERE cs.expected_count > 0
        AND (cs.expected_count - COALESCE(bs.actual_count, 0)) > 0
      ORDER BY missing_count DESC, completion_percentage ASC
      LIMIT 1
    `;
    
    const client = await pool.connect();
    try {
      const result = await client.query(query);
      
      if (result.rows.length === 0) {
        log('No dates found with missing calculations', 'success');
        return null;
      }
      
      const criticalDate = result.rows[0].date;
      const missingCount = result.rows[0].missing_count;
      const completionPercentage = result.rows[0].completion_percentage;
      
      log(`Most critical date: ${criticalDate}`, 'info');
      log(`Missing calculations: ${missingCount}`, 'info');
      log(`Completion percentage: ${completionPercentage}%`, 'info');
      
      return criticalDate;
    } finally {
      client.release();
    }
  } catch (error) {
    log(`Error finding critical date: ${error}`, 'error');
    return null;
  }
}

/**
 * Spot fix a specific date-period-farm combination
 */
async function spotFix(date: string, period: number, farmId: string): Promise<void> {
  log(`Spot fixing ${date} P${period} ${farmId}`, 'info');
  
  let success = 0;
  let failed = 0;
  
  for (const model of MINER_MODELS) {
    const result = await processCombo(date, period, farmId, model);
    if (result) {
      success++;
    } else {
      failed++;
    }
    // Pause between operations
    await sleep(1000);
  }
  
  log(`Spot fix complete: ${success} successful, ${failed} failed`, success === 3 ? 'success' : 'warning');
}

/**
 * Main function to handle command line arguments
 */
async function main() {
  try {
    const command = process.argv[2]?.toLowerCase();
    const param1 = process.argv[3];
    const param2 = process.argv[4];
    const param3 = process.argv[5];
    
    if (!command) {
      log("Missing command. Use: sequence, critical-date, most-critical, or spot-fix", 'error');
      process.exit(1);
    }
    
    switch (command) {
      case "sequence":
        if (!param1) {
          log("Missing date parameter. Format: YYYY-MM-DD", 'error');
          process.exit(1);
        }
        const batchSize = param2 ? parseInt(param2) : DEFAULT_BATCH_SIZE;
        await sequentialProcess(param1, batchSize);
        break;
        
      case "critical-date":
        if (!param1) {
          log("Missing date parameter. Format: YYYY-MM-DD", 'error');
          process.exit(1);
        }
        await fixCriticalDate(param1);
        break;
        
      case "most-critical":
        const criticalDate = await findMostCriticalDate();
        if (criticalDate) {
          log(`Fixing most critical date: ${criticalDate}`, 'info');
          await fixCriticalDate(criticalDate);
        }
        break;
        
      case "spot-fix":
        if (!param1 || !param2 || !param3) {
          log("Missing parameters. Format: spot-fix YYYY-MM-DD PERIOD FARM_ID", 'error');
          process.exit(1);
        }
        await spotFix(param1, parseInt(param2), param3);
        break;
        
      default:
        log("Unknown command. Use: sequence, critical-date, most-critical, or spot-fix", 'error');
        process.exit(1);
    }
  } catch (error) {
    log(`Fatal error: ${error}`, 'error');
    throw error;
  } finally {
    // Clean up
    await pool.end();
  }
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .then(() => {
      log("\n=== Minimal Reconciliation Tool Complete ===", 'success');
      process.exit(0);
    })
    .catch(error => {
      log(`Fatal error: ${error}`, 'error');
      process.exit(1);
    });
}

export {
  sequentialProcess,
  fixCriticalDate,
  findMostCriticalDate,
  spotFix
};
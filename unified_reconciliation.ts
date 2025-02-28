/**
 * Unified Reconciliation System
 * 
 * A comprehensive solution for ensuring data integrity between
 * curtailment_records and historical_bitcoin_calculations tables.
 * 
 * This tool combines the best features from multiple reconciliation tools:
 * - Efficient batch processing from efficient_reconciliation.ts
 * - Careful connection handling from minimal_reconciliation.ts
 * - Progress tracking and checkpoints for resumability
 * - Advanced retry logic with exponential backoff
 * - Comprehensive logging and reporting
 * 
 * Usage:
 * npx tsx unified_reconciliation.ts [command] [options]
 * 
 * Commands:
 *   status                 - Show current reconciliation status
 *   analyze                - Analyze missing calculations and detect issues
 *   reconcile [batchSize]  - Process all missing calculations with specified batch size
 *   date YYYY-MM-DD        - Process a specific date
 *   range YYYY-MM-DD YYYY-MM-DD [batchSize] - Process a date range
 *   critical DATE          - Process a problematic date with extra safeguards
 *   spot-fix DATE PERIOD FARM - Fix a specific date-period-farm combination
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { sql, eq, and } from "drizzle-orm";
import pg from 'pg';
import fs from 'fs';
import { format, parseISO, eachDayOfInterval } from 'date-fns';
import { reconcileDay, auditAndFixBitcoinCalculations } from "./server/services/historicalReconciliation";

// Configuration constants
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
const DEFAULT_BATCH_SIZE = 10;
const MAX_CONCURRENT_OPERATIONS = 5;
const MAX_RETRIES = 3;
const CHECKPOINT_FILE = './reconciliation_checkpoint.json';
const LOG_FILE = './reconciliation.log';

// Database connection pool with optimized settings
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 15000,
  query_timeout: 20000,
  allowExitOnIdle: true
});

// Add error handler to pool
pool.on('error', (err) => {
  try {
    const timestamp = new Date().toISOString();
    const message = `[${timestamp}] [DB ERROR] Database pool error: ${err.message}`;
    
    try {
      process.stderr.write(message + '\n');
    } catch (writeErr) {
      // Handle stderr pipe errors silently
    }
    
    try {
      fs.appendFileSync(LOG_FILE, message + '\n');
    } catch (fileErr) {
      // If logging fails, this is a last resort
    }
  } catch (e) {
    // Last resort error handling
  }
});

// Checkpoint interface
interface ReconciliationCheckpoint {
  lastProcessedDate: string;
  pendingDates: string[];
  completedDates: string[];
  startTime: number;
  lastUpdateTime: number;
  stats: {
    totalRecords: number;
    processedRecords: number;
    successfulRecords: number;
    failedRecords: number;
    timeouts: number;
  };
}

// Initialize checkpoint
let checkpoint: ReconciliationCheckpoint = {
  lastProcessedDate: '',
  pendingDates: [],
  completedDates: [],
  startTime: Date.now(),
  lastUpdateTime: Date.now(),
  stats: {
    totalRecords: 0,
    processedRecords: 0,
    successfulRecords: 0,
    failedRecords: 0,
    timeouts: 0
  }
};

/**
 * Log a message to console and log file
 */
function log(message: string, type: 'info' | 'error' | 'warning' | 'success' = 'info'): void {
  const timestamp = new Date().toISOString();
  const formatted = `[${timestamp}] [${type.toUpperCase()}] ${message}`;
  
  try {
    console.log(formatted);
  } catch (err) {
    // Handle stdout pipe errors silently
    if (err && (err as any).code !== 'EPIPE') {
      process.stderr.write(`Error writing to console: ${err}\n`);
    }
  }
  
  try {
    fs.appendFileSync(LOG_FILE, formatted + '\n');
  } catch (err) {
    process.stderr.write(`Error writing to log file: ${err}\n`);
  }
}

/**
 * Save current checkpoint to file
 */
function saveCheckpoint(): void {
  try {
    checkpoint.lastUpdateTime = Date.now();
    fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2));
  } catch (error) {
    log(`Error saving checkpoint: ${error}`, 'error');
  }
}

/**
 * Load checkpoint from file if exists
 */
function loadCheckpoint(): boolean {
  try {
    if (fs.existsSync(CHECKPOINT_FILE)) {
      const data = fs.readFileSync(CHECKPOINT_FILE, 'utf8');
      checkpoint = JSON.parse(data);
      log('Loaded checkpoint from file', 'info');
      return true;
    }
  } catch (error) {
    log(`Error loading checkpoint: ${error}`, 'error');
  }
  return false;
}

/**
 * Reset checkpoint data
 */
function resetCheckpoint(): void {
  checkpoint = {
    lastProcessedDate: '',
    pendingDates: [],
    completedDates: [],
    startTime: Date.now(),
    lastUpdateTime: Date.now(),
    stats: {
      totalRecords: 0,
      processedRecords: 0,
      successfulRecords: 0,
      failedRecords: 0,
      timeouts: 0
    }
  };
  saveCheckpoint();
}

/**
 * Sleep utility with Promise
 */
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Detect timeout from error message
 */
function isTimeoutError(error: any): boolean {
  if (error instanceof Error) {
    return error.message.includes('timeout') || 
           error.message.includes('deadlock') || 
           error.message.includes('terminating connection') ||
           error.message.includes('connection reset');
  }
  return false;
}

/**
 * Get summary statistics about reconciliation status
 */
async function getReconciliationStatus() {
  try {
    log('Fetching reconciliation status...', 'info');
    
    const query = `
      WITH curtailment_stats AS (
        SELECT 
          COUNT(DISTINCT settlement_date) as total_dates,
          SUM(ABS(volume::numeric)) as total_volume,
          COUNT(*) as total_records
        FROM curtailment_records
      ),
      bitcoin_stats AS (
        SELECT
          COUNT(DISTINCT settlement_date) as total_dates,
          SUM(bitcoin_mined::numeric) as total_bitcoin,
          COUNT(*) as total_records
        FROM historical_bitcoin_calculations
      ),
      date_completion AS (
        SELECT
          cr.settlement_date,
          COUNT(DISTINCT cr.settlement_period || '-' || cr.farm_id) as expected_calculations,
          COUNT(DISTINCT hbc.settlement_period || '-' || hbc.farm_id || '-' || hbc.miner_model) as actual_calculations
        FROM
          curtailment_records cr
        LEFT JOIN
          historical_bitcoin_calculations hbc ON cr.settlement_date = hbc.settlement_date
        GROUP BY
          cr.settlement_date
      )
      SELECT
        cs.total_dates as curtailment_dates,
        bs.total_dates as bitcoin_dates,
        cs.total_volume as total_curtailed_volume,
        bs.total_bitcoin as total_bitcoin_mined,
        cs.total_records as curtailment_records,
        bs.total_records as bitcoin_records,
        (SELECT COUNT(*) FROM date_completion WHERE actual_calculations >= expected_calculations * 3) as complete_dates,
        (SELECT COUNT(*) FROM date_completion WHERE actual_calculations > 0 AND actual_calculations < expected_calculations * 3) as partial_dates,
        (SELECT COUNT(*) FROM date_completion WHERE actual_calculations = 0 AND expected_calculations > 0) as missing_dates
      FROM
        curtailment_stats cs, bitcoin_stats bs
    `;
    
    const result = await db.execute(sql.raw(query));
    const status = result.rows[0];
    
    console.log('\n=== Reconciliation Status ===');
    console.log(`Curtailment Records: ${formatNumber(status.curtailment_records)}`);
    console.log(`Bitcoin Calculations: ${formatNumber(status.bitcoin_records)}`);
    console.log(`Total Curtailed Energy: ${formatNumber(status.total_curtailed_volume)} MWh`);
    console.log(`Total Bitcoin Mined: ${formatNumber(status.total_bitcoin_mined)} BTC`);
    console.log('\n=== Date Completion ===');
    console.log(`Complete Dates: ${formatNumber(status.complete_dates)}`);
    console.log(`Partial Dates: ${formatNumber(status.partial_dates)}`);
    console.log(`Missing Dates: ${formatNumber(status.missing_dates)}`);
    console.log(`Completion Rate: ${formatPercentage(Number(status.complete_dates) / Number(status.curtailment_dates) * 100)}%`);
    
    return status;
  } catch (error) {
    log(`Error getting reconciliation status: ${error}`, 'error');
    throw error;
  }
}

/**
 * Format a number with commas
 */
function formatNumber(value: any): string {
  const num = parseFloat(value);
  if (isNaN(num)) return '0';
  
  return num.toLocaleString('en-US', {
    minimumFractionDigits: num % 1 === 0 ? 0 : 2,
    maximumFractionDigits: 2
  });
}

/**
 * Format a percentage with 2 decimal places
 */
function formatPercentage(value: any): string {
  const num = parseFloat(value);
  if (isNaN(num)) return '0.00%';
  return num.toFixed(2) + '%';
}

/**
 * Find dates with missing Bitcoin calculations
 */
async function findDatesWithMissingCalculations(limit: number = 100) {
  try {
    log('Finding dates with missing Bitcoin calculations...', 'info');
    
    const query = `
      WITH curtailment_summary AS (
        SELECT 
          settlement_date,
          array_agg(DISTINCT settlement_period) as curtailment_periods,
          COUNT(DISTINCT settlement_period) as period_count,
          COUNT(DISTINCT farm_id) as farm_count,
          SUM(ABS(volume::numeric)) as total_volume
        FROM curtailment_records
        WHERE ABS(volume::numeric) > 0
        GROUP BY settlement_date
      ),
      bitcoin_summary AS (
        SELECT 
          settlement_date,
          array_agg(DISTINCT miner_model) as processed_models,
          miner_model,
          COUNT(DISTINCT settlement_period) as period_count,
          COUNT(DISTINCT farm_id) as farm_count
        FROM historical_bitcoin_calculations
        GROUP BY settlement_date, miner_model
      )
      SELECT 
        cs.settlement_date::text as date,
        cs.curtailment_periods,
        cs.period_count as required_period_count,
        cs.farm_count,
        cs.total_volume,
        ARRAY(
          SELECT unnest(array['S19J_PRO', 'S9', 'M20S'])
          EXCEPT
          SELECT unnest(COALESCE(array_agg(DISTINCT bs.miner_model), ARRAY[]::text[]))
        ) as missing_models,
        COALESCE(MIN(bs.period_count), 0) as min_calculated_periods,
        COALESCE(MAX(bs.period_count), 0) as max_calculated_periods
      FROM curtailment_summary cs
      LEFT JOIN bitcoin_summary bs ON cs.settlement_date = bs.settlement_date
      WHERE cs.total_volume > 0
      GROUP BY 
        cs.settlement_date,
        cs.curtailment_periods,
        cs.period_count,
        cs.farm_count,
        cs.total_volume
      HAVING 
        ARRAY_LENGTH(ARRAY(
          SELECT unnest(array['S19J_PRO', 'S9', 'M20S'])
          EXCEPT
          SELECT unnest(COALESCE(array_agg(DISTINCT bs.miner_model), ARRAY[]::text[]))
        ), 1) > 0
        OR MIN(COALESCE(bs.period_count, 0)) < cs.period_count
      ORDER BY cs.settlement_date
      LIMIT $1
    `;
    
    const result = await db.execute(sql.raw(query.replace('$1', limit.toString())));
    
    if (result.rows.length === 0) {
      log('No dates with missing calculations found', 'success');
      return [];
    }
    
    log(`Found ${result.rows.length} dates with missing calculations`, 'info');
    
    console.log('\n=== Dates with Missing Calculations ===');
    result.rows.slice(0, 10).forEach((row, index) => {
      const missingModels = Array.isArray(row.missing_models) ? row.missing_models : [];
      const minPeriods = Number(row.min_calculated_periods || 0);
      const requiredPeriods = Number(row.required_period_count || 0);
      console.log(`${index + 1}. ${row.date}: Missing ${missingModels.length > 0 ? missingModels.join(', ') : 'none'}, ` +
                  `Periods: ${minPeriods}/${requiredPeriods} (${formatPercentage(requiredPeriods > 0 ? minPeriods / requiredPeriods * 100 : 0)})`);
    });
    
    if (result.rows.length > 10) {
      console.log(`... and ${result.rows.length - 10} more dates`);
    }
    
    return result.rows.map(row => {
      const minPeriods = Number(row.min_calculated_periods || 0);
      const requiredPeriods = Number(row.required_period_count || 0);
      
      return {
        date: row.date,
        curtailmentPeriods: row.curtailment_periods,
        requiredPeriodCount: requiredPeriods,
        farmCount: row.farm_count,
        totalVolume: row.total_volume,
        missingModels: Array.isArray(row.missing_models) ? row.missing_models : [],
        minCalculatedPeriods: minPeriods,
        maxCalculatedPeriods: Number(row.max_calculated_periods || 0),
        completionRate: formatPercentage(requiredPeriods > 0 ? minPeriods / requiredPeriods * 100 : 0)
      };
    });
  } catch (error) {
    log(`Error finding dates with missing calculations: ${error}`, 'error');
    throw error;
  }
}

/**
 * Process a specific date with retry logic
 */
async function processDate(date: string, attemptNumber: number = 1): Promise<{success: boolean, message: string}> {
  const MAX_ATTEMPTS = 3;
  
  try {
    log(`Processing date ${date} (attempt ${attemptNumber}/${MAX_ATTEMPTS})...`, 'info');
    
    // Use the historical reconciliation service's function
    const result = await auditAndFixBitcoinCalculations(date);
    
    if (result.success) {
      if (result.fixed) {
        log(`Successfully fixed Bitcoin calculations for ${date}: ${result.message}`, 'success');
      } else {
        log(`Verified Bitcoin calculations for ${date}: ${result.message}`, 'success');
      }
      
      checkpoint.lastProcessedDate = date;
      checkpoint.completedDates.push(date);
      checkpoint.pendingDates = checkpoint.pendingDates.filter(d => d !== date);
      saveCheckpoint();
      
      return { success: true, message: result.message };
    } else {
      throw new Error(result.message);
    }
  } catch (error) {
    log(`Error processing date ${date}: ${error}`, 'error');
    
    // Implement retry with exponential backoff for recoverable errors
    if (attemptNumber < MAX_ATTEMPTS && (error instanceof Error && (
      isTimeoutError(error) || 
      error.message.includes('database') || 
      error.message.includes('connection')
    ))) {
      const backoffTime = Math.pow(2, attemptNumber) * 1000;
      log(`Retrying in ${backoffTime}ms...`, 'warning');
      
      await sleep(backoffTime);
      return processDate(date, attemptNumber + 1);
    }
    
    return { 
      success: false, 
      message: error instanceof Error ? error.message : String(error)
    };
  }
}

/**
 * Process a batch of dates concurrently with limited parallelism
 */
async function processDates(dates: string[], batchSize: number = DEFAULT_BATCH_SIZE): Promise<{
  processedDates: number;
  successfulDates: number;
  failedDates: number;
}> {
  try {
    log(`Processing ${dates.length} dates with batch size ${batchSize}...`, 'info');
    
    // Setup checkpoint for resumability
    checkpoint.pendingDates = [...dates];
    checkpoint.stats.totalRecords = dates.length;
    saveCheckpoint();
    
    let processedCount = 0;
    let successCount = 0;
    let failureCount = 0;
    
    // Process in batches with limited concurrency
    for (let i = 0; i < dates.length; i += batchSize) {
      const batch = dates.slice(i, i + batchSize);
      log(`Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(dates.length / batchSize)}`, 'info');
      
      const results = await Promise.allSettled(batch.map(date => processDate(date)));
      
      // Process results
      results.forEach((result, index) => {
        processedCount++;
        checkpoint.stats.processedRecords = processedCount;
        
        if (result.status === 'fulfilled' && result.value.success) {
          successCount++;
          checkpoint.stats.successfulRecords = successCount;
        } else {
          failureCount++;
          checkpoint.stats.failedRecords = failureCount;
          
          const errorMessage = result.status === 'fulfilled' 
            ? result.value.message 
            : result.reason instanceof Error ? result.reason.message : String(result.reason);
          
          log(`Failed to process ${batch[index]}: ${errorMessage}`, 'error');
          
          if (isTimeoutError(errorMessage)) {
            checkpoint.stats.timeouts++;
          }
        }
        
        saveCheckpoint();
      });
      
      // Add a small delay between batches to prevent connection issues
      if (i + batchSize < dates.length) {
        await sleep(1000);
      }
    }
    
    log(`Completed processing ${dates.length} dates: ${successCount} successful, ${failureCount} failed`, 
        failureCount > 0 ? 'warning' : 'success');
    
    return {
      processedDates: processedCount,
      successfulDates: successCount,
      failedDates: failureCount
    };
  } catch (error) {
    log(`Error during batch processing: ${error}`, 'error');
    throw error;
  }
}

/**
 * Process a date range
 */
async function processDateRange(startDate: string, endDate: string, batchSize: number = DEFAULT_BATCH_SIZE): Promise<void> {
  try {
    log(`Processing date range from ${startDate} to ${endDate}...`, 'info');
    
    // Generate array of dates in the range
    const dates = eachDayOfInterval({
      start: parseISO(startDate),
      end: parseISO(endDate)
    }).map(date => format(date, 'yyyy-MM-dd'));
    
    if (dates.length === 0) {
      log('No dates to process in the specified range', 'warning');
      return;
    }
    
    log(`Found ${dates.length} dates to process`, 'info');
    
    // Reset checkpoint for this operation
    resetCheckpoint();
    
    const result = await processDates(dates, batchSize);
    
    log(`Processed ${result.processedDates} dates: ${result.successfulDates} successful, ${result.failedDates} failed`, 
        result.failedDates > 0 ? 'warning' : 'success');
  } catch (error) {
    log(`Error processing date range: ${error}`, 'error');
    throw error;
  }
}

/**
 * Process a critical date with extra safeguards
 */
async function processCriticalDate(date: string): Promise<void> {
  try {
    log(`Processing critical date ${date} with extra safeguards...`, 'info');
    
    // Find missing combinations with a safer query
    const client = await pool.connect();
    try {
      // First check what's missing
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
      
      const result = await client.query(query, [date]);
      const missingCombos = result.rows.map(row => ({
        period: row.settlement_period,
        farmId: row.farm_id,
        minerModels: row.missing_models
      }));
      
      if (missingCombos.length === 0) {
        log(`No missing combinations found for ${date}`, 'success');
        return;
      }
      
      log(`Found ${missingCombos.length} combinations with missing calculations`, 'info');
      
      // Process carefully
      for (const combo of missingCombos) {
        for (const model of combo.minerModels) {
          // Get curtailment record
          const curtailmentQuery = `
            SELECT 
              ABS(volume::numeric) as volume,
              difficulty::numeric as difficulty
            FROM curtailment_records cr
            LEFT JOIN historical_bitcoin_calculations hbc 
              ON cr.settlement_date = hbc.settlement_date
              AND hbc.difficulty IS NOT NULL
            WHERE cr.settlement_date = $1
              AND cr.settlement_period = $2
              AND cr.farm_id = $3
            LIMIT 1
          `;
          
          const curtailmentResult = await client.query(curtailmentQuery, [
            date, combo.period, combo.farmId
          ]);
          
          if (curtailmentResult.rows.length === 0) {
            log(`No curtailment record found for ${date} P${combo.period} ${combo.farmId}`, 'warning');
            continue;
          }
          
          const volume = Number(curtailmentResult.rows[0].volume);
          const difficulty = Number(curtailmentResult.rows[0].difficulty || 108105433845147);
          
          // Calculate Bitcoin using the same logic from minimal_reconciliation.ts
          const minerStats = {
            'S19J_PRO': { hashrate: 104, power: 3068 },
            'S9': { hashrate: 13.5, power: 1323 },
            'M20S': { hashrate: 68, power: 3360 }
          };
          
          const miner = minerStats[model as keyof typeof minerStats];
          if (!miner) {
            log(`Invalid miner model: ${model}`, 'error');
            continue;
          }
          
          const BLOCK_REWARD = 3.125;
          const SETTLEMENT_PERIOD_MINUTES = 30;
          const BLOCKS_PER_SETTLEMENT_PERIOD = 3;
          
          const curtailedKwh = volume * 1000;
          const minerConsumptionKwh = (miner.power / 1000) * (SETTLEMENT_PERIOD_MINUTES / 60);
          const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);
          const difficultyNum = difficulty;
          const hashesPerBlock = difficultyNum * Math.pow(2, 32);
          const networkHashRate = hashesPerBlock / 600;
          const networkHashRateTH = networkHashRate / 1e12;
          const totalHashPower = potentialMiners * miner.hashrate;
          const ourNetworkShare = totalHashPower / networkHashRateTH;
          
          const bitcoinMined = Number((ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_SETTLEMENT_PERIOD).toFixed(8));
          
          // Insert with extra caution
          try {
            // Check if exists first
            const checkQuery = `
              SELECT id 
              FROM historical_bitcoin_calculations
              WHERE settlement_date = $1
                AND settlement_period = $2
                AND farm_id = $3
                AND miner_model = $4
            `;
            
            const checkResult = await client.query(checkQuery, [
              date, combo.period, combo.farmId, model
            ]);
            
            if (checkResult.rows.length > 0) {
              // Update
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
              
              log(`Updated calculation for ${date} P${combo.period} ${combo.farmId} ${model}`, 'success');
            } else {
              // Insert
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
                combo.period,
                combo.farmId,
                model,
                bitcoinMined.toString(),
                difficulty.toString()
              ]);
              
              log(`Inserted calculation for ${date} P${combo.period} ${combo.farmId} ${model}`, 'success');
            }
          } catch (error) {
            log(`Error processing ${date} P${combo.period} ${combo.farmId} ${model}: ${error}`, 'error');
          }
          
          // Small pause between operations
          await sleep(500);
        }
      }
      
      log(`Completed processing critical date ${date}`, 'success');
    } finally {
      client.release();
    }
  } catch (error) {
    log(`Error processing critical date ${date}: ${error}`, 'error');
    throw error;
  }
}

/**
 * Find and fix a specific spot (date-period-farm combination)
 */
async function spotFix(date: string, period: number, farmId: string): Promise<void> {
  try {
    log(`Spot fixing ${date} P${period} ${farmId}...`, 'info');
    
    const client = await pool.connect();
    try {
      // Get curtailment record
      const curtailmentQuery = `
        SELECT 
          ABS(volume::numeric) as volume,
          difficulty::numeric as difficulty
        FROM curtailment_records cr
        LEFT JOIN historical_bitcoin_calculations hbc 
          ON cr.settlement_date = hbc.settlement_date
          AND hbc.difficulty IS NOT NULL
        WHERE cr.settlement_date = $1
          AND cr.settlement_period = $2
          AND cr.farm_id = $3
        LIMIT 1
      `;
      
      const curtailmentResult = await client.query(curtailmentQuery, [date, period, farmId]);
      
      if (curtailmentResult.rows.length === 0) {
        log(`No curtailment record found for ${date} P${period} ${farmId}`, 'error');
        return;
      }
      
      const volume = Number(curtailmentResult.rows[0].volume);
      const difficulty = Number(curtailmentResult.rows[0].difficulty || 108105433845147);
      
      // Process each model
      for (const model of MINER_MODELS) {
        try {
          // Calculate Bitcoin using the same logic
          const minerStats = {
            'S19J_PRO': { hashrate: 104, power: 3068 },
            'S9': { hashrate: 13.5, power: 1323 },
            'M20S': { hashrate: 68, power: 3360 }
          };
          
          const miner = minerStats[model as keyof typeof minerStats];
          
          const BLOCK_REWARD = 3.125;
          const SETTLEMENT_PERIOD_MINUTES = 30;
          const BLOCKS_PER_SETTLEMENT_PERIOD = 3;
          
          const curtailedKwh = volume * 1000;
          const minerConsumptionKwh = (miner.power / 1000) * (SETTLEMENT_PERIOD_MINUTES / 60);
          const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);
          const difficultyNum = difficulty;
          const hashesPerBlock = difficultyNum * Math.pow(2, 32);
          const networkHashRate = hashesPerBlock / 600;
          const networkHashRateTH = networkHashRate / 1e12;
          const totalHashPower = potentialMiners * miner.hashrate;
          const ourNetworkShare = totalHashPower / networkHashRateTH;
          
          const bitcoinMined = Number((ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_SETTLEMENT_PERIOD).toFixed(8));
          
          // Check if exists
          const checkQuery = `
            SELECT id 
            FROM historical_bitcoin_calculations
            WHERE settlement_date = $1
              AND settlement_period = $2
              AND farm_id = $3
              AND miner_model = $4
          `;
          
          const checkResult = await client.query(checkQuery, [
            date, period, farmId, model
          ]);
          
          if (checkResult.rows.length > 0) {
            // Update
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
            
            log(`Updated calculation for ${date} P${period} ${farmId} ${model}`, 'success');
          } else {
            // Insert
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
              model,
              bitcoinMined.toString(),
              difficulty.toString()
            ]);
            
            log(`Inserted calculation for ${date} P${period} ${farmId} ${model}`, 'success');
          }
        } catch (error) {
          log(`Error processing ${model} for ${date} P${period} ${farmId}: ${error}`, 'error');
        }
      }
    } finally {
      client.release();
    }
  } catch (error) {
    log(`Error in spot fix: ${error}`, 'error');
    throw error;
  }
}

/**
 * Analyze current reconciliation status and provide recommendations
 */
async function analyzeReconciliationStatus(): Promise<void> {
  try {
    log('Analyzing reconciliation status...', 'info');
    
    // Get overall status
    const status = await getReconciliationStatus();
    
    // Find missing dates
    const missingDates = await findDatesWithMissingCalculations(25);
    
    // Check database connection health
    let connectionStatus = 'healthy';
    try {
      await pool.query('SELECT 1');
    } catch (error) {
      connectionStatus = 'degraded';
      log(`Database connection issue detected: ${error}`, 'error');
    }
    
    // Check for active long-running queries
    let longRunningQueries = 0;
    try {
      const longQueryResult = await pool.query(`
        SELECT count(*) as count FROM pg_stat_activity 
        WHERE state = 'active' 
        AND now() - query_start > interval '5 minutes'
      `);
      longRunningQueries = parseInt(longQueryResult.rows[0].count, 10);
    } catch (error) {
      log(`Error checking for long-running queries: ${error}`, 'warning');
    }
    
    // Generate recommendations
    console.log('\n=== Reconciliation Analysis ===');
    console.log(`Database connection: ${connectionStatus}`);
    console.log(`Long-running queries: ${longRunningQueries}`);
    console.log(`Completion rate: ${formatPercentage(Number(status.complete_dates) / Number(status.curtailment_dates) * 100)}`);
    
    console.log('\n=== Recommendations ===');
    
    if (connectionStatus !== 'healthy') {
      console.log('⚠️ Database connection issues detected. Consider:');
      console.log('  - Checking for connection leaks in application code');
      console.log('  - Reducing the maximum pool size to prevent overloading the database');
      console.log('  - Running with smaller batch sizes');
    }
    
    if (longRunningQueries > 0) {
      console.log(`⚠️ ${longRunningQueries} long-running queries detected. Consider:`);
      console.log('  - Terminating these queries if they are reconciliation operations that have stalled');
      console.log('  - Use critical mode for problematic dates to process one record at a time');
    }
    
    if (missingDates.length > 0) {
      console.log('📊 Missing calculations detected:');
      console.log(`  - ${missingDates.length} dates need reconciliation`);
      
      // Group by completion percentage
      const criticalDates = missingDates.filter(d => 
        parseFloat(d.completionRate.replace('%', '')) < 50
      );
      const partialDates = missingDates.filter(d => 
        parseFloat(d.completionRate.replace('%', '')) >= 50 && 
        parseFloat(d.completionRate.replace('%', '')) < 95
      );
      const almostCompleteDates = missingDates.filter(d => 
        parseFloat(d.completionRate.replace('%', '')) >= 95
      );
      
      if (criticalDates.length > 0) {
        console.log(`  - Critical dates (${criticalDates.length}): ${criticalDates.slice(0, 3).map(d => d.date).join(', ')}${criticalDates.length > 3 ? '...' : ''}`);
        console.log('    Recommendation: Process these dates with "critical" command for extra safeguards');
      }
      
      if (partialDates.length > 0) {
        console.log(`  - Partial dates (${partialDates.length}): ${partialDates.slice(0, 3).map(d => d.date).join(', ')}${partialDates.length > 3 ? '...' : ''}`);
      }
      
      if (almostCompleteDates.length > 0) {
        console.log(`  - Almost complete dates (${almostCompleteDates.length}): ${almostCompleteDates.slice(0, 3).map(d => d.date).join(', ')}${almostCompleteDates.length > 3 ? '...' : ''}`);
      }
    } else {
      console.log('✅ No missing calculations detected! Reconciliation is complete.');
    }
  } catch (error) {
    log(`Error during analysis: ${error}`, 'error');
    throw error;
  }
}

/**
 * Display help menu
 */
function showHelp(): void {
  console.log('Unified Reconciliation System');
  console.log('===========================');
  console.log('Usage: npx tsx unified_reconciliation.ts [command] [options]');
  console.log('');
  console.log('Commands:');
  console.log('  status                 - Show current reconciliation status');
  console.log('  analyze                - Analyze missing calculations and detect issues');
  console.log('  reconcile [batchSize]  - Process all missing calculations with specified batch size');
  console.log('  date YYYY-MM-DD        - Process a specific date');
  console.log('  range YYYY-MM-DD YYYY-MM-DD [batchSize] - Process a date range');
  console.log('  critical DATE          - Process a problematic date with extra safeguards');
  console.log('  spot-fix DATE PERIOD FARM - Fix a specific date-period-farm combination');
  console.log('  help                   - Show this help message');
}

/**
 * Main function to handle command line arguments
 */
async function main() {
  try {
    // Check if database connection string is set
    if (!process.env.DATABASE_URL) {
      log('DATABASE_URL environment variable is not set', 'error');
      process.exit(1);
    }
    
    // Load checkpoint if resuming
    loadCheckpoint();
    
    const args = process.argv.slice(2);
    const command = args[0]?.toLowerCase();
    
    if (!command || command === 'help') {
      showHelp();
      return;
    }
    
    switch (command) {
      case 'status':
        await getReconciliationStatus();
        break;
        
      case 'analyze':
        await analyzeReconciliationStatus();
        break;
        
      case 'reconcile': {
        const batchSize = args[1] ? parseInt(args[1], 10) : DEFAULT_BATCH_SIZE;
        log(`Starting reconciliation with batch size ${batchSize}...`, 'info');
        
        // Find dates that need reconciliation
        const missingDates = await findDatesWithMissingCalculations(100);
        
        if (missingDates.length === 0) {
          log('No dates need reconciliation', 'success');
          break;
        }
        
        const dates = missingDates.map(d => d.date as string);
        await processDates(dates, batchSize);
        break;
      }
        
      case 'date': {
        if (!args[1]) {
          log('Date is required (YYYY-MM-DD)', 'error');
          showHelp();
          break;
        }
        
        const date = args[1];
        const result = await processDate(date);
        
        if (result.success) {
          log(`Successfully processed ${date}: ${result.message}`, 'success');
        } else {
          log(`Failed to process ${date}: ${result.message}`, 'error');
        }
        break;
      }
        
      case 'range': {
        if (!args[1] || !args[2]) {
          log('Start and end dates are required (YYYY-MM-DD)', 'error');
          showHelp();
          break;
        }
        
        const startDate = args[1];
        const endDate = args[2];
        const batchSize = args[3] ? parseInt(args[3], 10) : DEFAULT_BATCH_SIZE;
        
        await processDateRange(startDate, endDate, batchSize);
        break;
      }
        
      case 'critical': {
        if (!args[1]) {
          log('Date is required (YYYY-MM-DD)', 'error');
          showHelp();
          break;
        }
        
        const date = args[1];
        await processCriticalDate(date);
        break;
      }
        
      case 'spot-fix': {
        if (!args[1] || !args[2] || !args[3]) {
          log('Date, period, and farm ID are required', 'error');
          showHelp();
          break;
        }
        
        const date = args[1];
        const period = parseInt(args[2], 10);
        const farmId = args[3];
        
        await spotFix(date, period, farmId);
        break;
      }
        
      default:
        log(`Unknown command: ${command}`, 'error');
        showHelp();
    }
  } catch (error) {
    log(`Error in main function: ${error}`, 'error');
  } finally {
    try {
      // Ensure pool is terminated properly
      await pool.end();
    } catch (err) {
      // Ignore errors during cleanup
    }
  }
}

// Run the main function if this script is executed directly
// Using import.meta.url instead of require.main for ES modules
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(err => {
    console.error('Unhandled error:', err);
    process.exit(1);
  });
}

// Export functions for use by other modules
export {
  getReconciliationStatus,
  findDatesWithMissingCalculations,
  processDate,
  processDates,
  processDateRange,
  processCriticalDate,
  spotFix,
  analyzeReconciliationStatus
};
/**
 * Process Missing Bitcoin Calculations for 2025-03-04
 * 
 * This script identifies and processes only the settlement periods that are missing
 * Bitcoin calculations, avoiding duplicate key conflicts.
 */

import pg from 'pg';

// Configuration
const DATE = "2025-03-04";
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const DEFAULT_DIFFICULTY = 108105433845147;

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

async function getMinerModelInfo(minerModel: string): Promise<{ hashrate: number, power: number }> {
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

async function findMissingPeriods(minerModel: string): Promise<number[]> {
  const client = await pool.connect();
  try {
    // Get all 48 periods
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    
    // Find periods that already have Bitcoin calculations
    const result = await client.query(`
      SELECT DISTINCT settlement_period 
      FROM historical_bitcoin_calculations 
      WHERE settlement_date = $1 AND miner_model = $2
    `, [DATE, minerModel]);
    
    const existingPeriods = result.rows.map(row => row.settlement_period);
    
    // Return periods that don't have calculations
    return allPeriods.filter(period => !existingPeriods.includes(period));
  } finally {
    client.release();
  }
}

async function processPeriod(period: number, minerModel: string, difficulty: number): Promise<number> {
  const client = await pool.connect();
  try {
    const minerInfo = await getMinerModelInfo(minerModel);
    
    // This complex transaction:
    // 1. Gets curtailment records for the specific period
    // 2. Calculates Bitcoin based on the miner parameters and curtailment volume
    // 3. Inserts new calculation records, avoiding duplicates
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
    
    const result = await client.query(query, [DATE, period, minerModel, difficulty.toString()]);
    const insertedCount = parseInt(result.rows[0].inserted_count);
    
    return insertedCount;
  } catch (error) {
    log(`Error processing period ${period} for ${minerModel}: ${error}`, "error");
    throw error;
  } finally {
    client.release();
  }
}

async function processAllMissingPeriods(): Promise<void> {
  // Process each miner model
  for (const model of MINER_MODELS) {
    // Find which periods are missing for this model
    const missingPeriods = await findMissingPeriods(model);
    
    if (missingPeriods.length === 0) {
      log(`No missing periods for ${model}`, "success");
      continue;
    }
    
    log(`Found ${missingPeriods.length} missing periods for ${model}: ${missingPeriods.join(', ')}`, "info");
    
    // Process each missing period
    let totalInserted = 0;
    for (const period of missingPeriods) {
      const insertedCount = await processPeriod(period, model, DEFAULT_DIFFICULTY);
      totalInserted += insertedCount;
      
      log(`Period ${period}: inserted ${insertedCount} records for ${model}`, insertedCount > 0 ? "success" : "warning");
      
      // Short pause between periods to avoid overloading the database
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    log(`Completed processing ${model}: inserted ${totalInserted} records across ${missingPeriods.length} periods`, "success");
  }
}

async function verifyCalculations(): Promise<void> {
  const client = await pool.connect();
  
  try {
    // Verify the results for each miner model
    for (const model of MINER_MODELS) {
      const result = await client.query(`
        SELECT 
          COUNT(*) AS record_count,
          COUNT(DISTINCT settlement_period) AS period_count,
          COALESCE(SUM(bitcoin_mined), 0) AS total_bitcoin,
          MIN(settlement_period) AS min_period,
          MAX(settlement_period) AS max_period
        FROM 
          historical_bitcoin_calculations
        WHERE 
          settlement_date = $1
          AND miner_model = $2
      `, [DATE, model]);
      
      const stats = result.rows[0];
      const isPeriodComplete = stats.period_count === 48 && stats.min_period === 1 && stats.max_period === 48;
      
      log(
        `${model}: ${stats.record_count} records across ${stats.period_count} periods, total: ${parseFloat(stats.total_bitcoin).toFixed(8)} BTC`,
        isPeriodComplete ? "success" : "warning"
      );
      
      if (!isPeriodComplete) {
        // Find which periods are still missing
        const periodsResult = await client.query(`
          SELECT array_agg(p) AS missing_periods
          FROM generate_series(1, 48) p
          WHERE p NOT IN (
            SELECT DISTINCT settlement_period
            FROM historical_bitcoin_calculations
            WHERE settlement_date = $1 AND miner_model = $2
          )
        `, [DATE, model]);
        
        const missingPeriods = periodsResult.rows[0].missing_periods || [];
        if (missingPeriods.length > 0) {
          log(`${model} is missing periods: ${missingPeriods.join(', ')}`, "warning");
        }
      }
    }
  } catch (error) {
    log(`Error verifying calculations: ${error}`, "error");
  } finally {
    client.release();
  }
}

async function main() {
  try {
    log(`Processing Missing Bitcoin Calculations for ${DATE}`, "title");
    
    // Step 1: Process all missing periods for each miner model
    await processAllMissingPeriods();
    
    // Step 2: Verify that all calculations are now complete
    log("Verification of Bitcoin Calculations:", "title");
    await verifyCalculations();
    
    log("Processing complete!", "success");
  } catch (error) {
    log(`Error: ${error}`, "error");
    process.exit(1);
  } finally {
    // Close the pool when done
    await pool.end();
  }
}

// Run the main function
main();
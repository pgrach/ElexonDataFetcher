/**
 * Direct SQL-based Bitcoin Calculator for 2025-03-04
 * 
 * This script performs the Bitcoin calculations directly with SQL operations,
 * bypassing the need for module imports that might cause resolution issues.
 */

// Using pg directly for database access
import * as fs from 'fs';
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

// Create a PostgreSQL client using the DATABASE_URL environment variable
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

async function countExistingRecords(): Promise<number> {
  const client = await pool.connect();
  try {
    const result = await client.query(
      'SELECT COUNT(*) FROM historical_bitcoin_calculations WHERE settlement_date = $1',
      [DATE]
    );
    return parseInt(result.rows[0].count);
  } finally {
    client.release();
  }
}

async function clearExistingCalculations(): Promise<void> {
  const count = await countExistingRecords();
  
  const client = await pool.connect();
  try {
    await client.query(
      'DELETE FROM historical_bitcoin_calculations WHERE settlement_date = $1',
      [DATE]
    );
    log(`Cleared ${count} existing Bitcoin calculation records`, "info");
  } catch (error) {
    log(`Error clearing existing records: ${error}`, "error");
    throw error;
  } finally {
    client.release();
  }
}

async function processMinerModel(minerModel: string, difficulty: number): Promise<number> {
  const client = await pool.connect();
  
  try {
    // First, get info about the miner model from the constants
    let minerHashrate, minerPower;
    
    switch (minerModel) {
      case 'S19J_PRO':
        minerHashrate = 104; // TH/s
        minerPower = 3068;   // Watts
        break;
      case 'S9':
        minerHashrate = 13.5;
        minerPower = 1323;
        break;
      case 'M20S':
        minerHashrate = 68;
        minerPower = 3360;
        break;
      default:
        throw new Error(`Unknown miner model: ${minerModel}`);
    }
    
    // Insert Bitcoin calculations directly with SQL
    const insertQuery = `
      WITH curtailment_data AS (
        SELECT 
          id,
          settlement_date, 
          settlement_period,
          farm_id,
          ABS(volume::numeric) AS curtailed_mwh
        FROM 
          curtailment_records
        WHERE 
          settlement_date = $1
      ),
      calculation_data AS (
        SELECT
          settlement_date,
          settlement_period,
          farm_id,
          curtailed_mwh,
          -- Calculate Bitcoin based on hashrate, power, and difficulty
          (
            FLOOR(
              (curtailed_mwh * 1000) / 
              ((${minerPower} / 1000.0) * (30.0 / 60.0))
            ) * ${minerHashrate} / 
            ((${difficulty} * POW(2, 32)) / 600 / 1000000000000)
          ) * 3.125 * 3 AS bitcoin_mined
        FROM
          curtailment_data
      )
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
        $2 AS miner_model,
        $3 AS difficulty,
        bitcoin_mined,
        NOW() AS calculated_at
      FROM
        calculation_data
      RETURNING *
    `;
    
    const result = await client.query(insertQuery, [DATE, minerModel, difficulty.toString()]);
    
    log(`Inserted ${result.rowCount} Bitcoin calculation records for ${minerModel}`, "success");
    return result.rowCount;
  } catch (error) {
    log(`Error processing ${minerModel}: ${error}`, "error");
    throw error;
  } finally {
    client.release();
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
          SUM(bitcoin_mined::numeric) AS total_bitcoin
        FROM 
          historical_bitcoin_calculations
        WHERE 
          settlement_date = $1
          AND miner_model = $2
      `, [DATE, model]);
      
      const stats = result.rows[0];
      
      log(
        `${model}: ${stats.record_count} records across ${stats.period_count} periods, total: ${parseFloat(stats.total_bitcoin).toFixed(8)} BTC`,
        stats.period_count === 48 ? "success" : "warning"
      );
    }
  } catch (error) {
    log(`Error verifying calculations: ${error}`, "error");
  } finally {
    client.release();
  }
}

async function main() {
  try {
    log(`Direct SQL Bitcoin Calculator for ${DATE}`, "title");
    
    // Step 1: Clear existing calculations
    await clearExistingCalculations();
    
    // Step 2: Process each miner model
    for (const model of MINER_MODELS) {
      log(`Processing calculations for ${model}...`, "info");
      await processMinerModel(model, DEFAULT_DIFFICULTY);
    }
    
    // Step 3: Verify the results
    log("Bitcoin calculation summary:", "title");
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
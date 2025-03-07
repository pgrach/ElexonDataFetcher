/**
 * Date Status Checker
 * 
 * A simple tool to check the status of data for a specific date,
 * including curtailment records and Bitcoin calculations.
 * 
 * Usage:
 *   npx tsx check_date_status.ts <date>
 * 
 * Example:
 *   npx tsx check_date_status.ts 2025-03-06
 */

import pkg from 'pg';
const { Pool } = pkg;
import { isValidDateString } from "./server/utils/dates";
import { minerModels } from "./server/types/bitcoin";

// ANSI color codes for better console output
const colors = {
  reset: "\x1b[0m",
  bright: "\x1b[1m",
  green: "\x1b[32m",
  cyan: "\x1b[36m",
  yellow: "\x1b[33m",
  red: "\x1b[31m",
  magenta: "\x1b[35m"
};

/**
 * Log a message with color formatting
 */
function log(message: string, type: "info" | "success" | "warning" | "error" | "title" = "info"): void {
  switch (type) {
    case "title":
      console.log(`${colors.bright}${colors.magenta}${message}${colors.reset}`);
      break;
    case "info":
      console.log(`${colors.cyan}${message}${colors.reset}`);
      break;
    case "success":
      console.log(`${colors.green}${message}${colors.reset}`);
      break;
    case "warning":
      console.log(`${colors.yellow}${message}${colors.reset}`);
      break;
    case "error":
      console.log(`${colors.red}${message}${colors.reset}`);
      break;
  }
}

/**
 * Check curtailment data for a date
 */
async function checkCurtailmentData(pool: any, date: string) {
  // Use the correct column names based on the actual schema
  const result = await pool.query(
    `SELECT 
       COUNT(*) as record_count, 
       COUNT(DISTINCT settlement_period) as period_count,
       ROUND(SUM(volume)::numeric, 2) as total_volume,
       ROUND(SUM(payment)::numeric, 2) as total_payment
     FROM curtailment_records 
     WHERE settlement_date = $1`,
    [date]
  );
  
  const { record_count, period_count, total_volume, total_payment } = result.rows[0];
  
  return {
    records: parseInt(record_count),
    periods: parseInt(period_count),
    volume: parseFloat(total_volume || '0'),
    payment: parseFloat(total_payment || '0')
  };
}

/**
 * Check Bitcoin calculations for a date
 */
async function checkBitcoinCalculations(pool: any, date: string) {
  const result = await pool.query(
    `SELECT 
       miner_model,
       COUNT(*) as record_count, 
       COUNT(DISTINCT settlement_period) as period_count,
       ROUND(SUM(bitcoin_mined::numeric), 8) as total_bitcoin
     FROM historical_bitcoin_calculations 
     WHERE settlement_date = $1
     GROUP BY miner_model
     ORDER BY miner_model`,
    [date]
  );
  
  return result.rows.map(row => ({
    minerModel: row.miner_model,
    records: parseInt(row.record_count),
    periods: parseInt(row.period_count),
    bitcoinMined: parseFloat(row.total_bitcoin)
  }));
}

/**
 * Main function to check a specific date's data
 */
async function main() {
  // Get date from command line
  const date = process.argv[2];
  
  // Validate date
  if (!date || !isValidDateString(date)) {
    log("Please provide a valid date in YYYY-MM-DD format", "error");
    log("Usage: npx tsx check_date_status.ts YYYY-MM-DD", "info");
    process.exit(1);
  }
  
  // Create database connection
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL
  });
  
  try {
    // Print header
    log(`Data Status for ${date}`, "title");
    console.log();
    
    // Check curtailment data
    log("Curtailment Records:", "info");
    const curtailmentData = await checkCurtailmentData(pool, date);
    
    if (curtailmentData.records === 0) {
      log("  No curtailment records found", "warning");
    } else {
      log(`  Records: ${curtailmentData.records}`, curtailmentData.records > 0 ? "success" : "warning");
      log(`  Periods: ${curtailmentData.periods}/48`, curtailmentData.periods === 48 ? "success" : "warning");
      log(`  Volume: ${curtailmentData.volume.toLocaleString()} MWh`, "info");
      log(`  Payment: £${curtailmentData.payment.toLocaleString()}`, "info");
    }
    
    console.log();
    
    // Check Bitcoin calculations
    log("Bitcoin Calculations:", "info");
    const bitcoinData = await checkBitcoinCalculations(pool, date);
    
    if (bitcoinData.length === 0) {
      log("  No Bitcoin calculations found", "warning");
    } else {
      const minerModelsList = Object.keys(minerModels);
      
      for (const minerModel of minerModelsList) {
        const data = bitcoinData.find(d => d.minerModel === minerModel);
        
        if (data) {
          log(`  ${minerModel}:`, "info");
          log(`    Records: ${data.records}`, data.records > 0 ? "success" : "warning");
          log(`    Periods: ${data.periods}/48`, data.periods === 48 ? "success" : "warning");
          log(`    Bitcoin Mined: ${data.bitcoinMined.toFixed(8)} BTC`, "success");
        } else {
          log(`  ${minerModel}: No calculations found`, "warning");
        }
      }
    }
    
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    log(`Error checking date: ${errorMessage}`, "error");
    process.exit(1);
  } finally {
    // Close database connection
    await pool.end();
  }
}

// Run the main function
main();
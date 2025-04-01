/**
 * Fix Bitcoin Calculations for 2025-03-28 Missing Periods
 * 
 * This script will:
 * 1. Calculate Bitcoin mining potential for periods 40-48
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import { format } from 'date-fns';
import fs from 'fs';

// Configuration
const DATE_TO_PROCESS = '2025-03-28';
const MISSING_PERIODS = [40, 41, 42, 43, 44, 45, 46, 47, 48];
const LOG_FILE = `./logs/fix_bitcoin_calculations_2025-03-28_${format(new Date(), 'yyyy-MM-dd')}.log`;

// Create log directory if it doesn't exist
if (!fs.existsSync('./logs')) {
  fs.mkdirSync('./logs');
}

// Logger
async function logToFile(message: string): Promise<void> {
  const timestamp = format(new Date(), 'yyyy-MM-dd HH:mm:ss');
  const logMessage = `[${timestamp}] ${message}\n`;
  fs.appendFileSync(LOG_FILE, logMessage);
}

function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
  const timestamp = format(new Date(), 'yyyy-MM-dd HH:mm:ss');
  
  // Terminal output with colors
  let colorCode = "";
  switch (type) {
    case "success": colorCode = "\x1b[32m"; break; // Green
    case "warning": colorCode = "\x1b[33m"; break; // Yellow
    case "error": colorCode = "\x1b[31m"; break;   // Red
    default: colorCode = "\x1b[36m";               // Cyan for info
  }
  
  console.log(`${colorCode}[${timestamp}] ${message}\x1b[0m`);
  logToFile(message).catch(console.error);
}

async function calculateBitcoinMining(): Promise<void> {
  try {
    log(`Starting Bitcoin calculation updates for ${DATE_TO_PROCESS} missing periods ${MISSING_PERIODS.join(', ')}`);
    
    // Perform calculations with each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      log(`Processing Bitcoin calculations for ${minerModel}`);
      
      // Get mining difficulty and BTC price
      let difficulty = 113757508810853; // Default value
      
      // Process each period
      for (const period of MISSING_PERIODS) {
        // Get all farms for this period
        const farmsResult = await db.execute(
          sql`SELECT DISTINCT farm_id, lead_party_name 
              FROM curtailment_records 
              WHERE settlement_date = ${DATE_TO_PROCESS}
              AND settlement_period = ${period}`
        );
        
        const farms = farmsResult.rows || [];
        log(`Found ${farms.length} farms with curtailment records for period ${period}`);
        
        // Process each farm
        for (const farm of farms) {
          // Get volume for this farm and period
          const farmRecordsResult = await db.execute(
            sql`SELECT volume 
                FROM curtailment_records 
                WHERE settlement_date = ${DATE_TO_PROCESS} 
                AND farm_id = ${farm.farm_id}
                AND settlement_period = ${period}`
          );
          
          if (!farmRecordsResult.rows || farmRecordsResult.rows.length === 0) {
            continue;
          }
          
          // Check if calculation already exists
          const existingCalcResult = await db.execute(
            sql`SELECT id FROM historical_bitcoin_calculations 
                WHERE settlement_date = ${DATE_TO_PROCESS}
                AND settlement_period = ${period}
                AND farm_id = ${farm.farm_id}
                AND miner_model = ${minerModel}`
          );
          
          if (existingCalcResult.rows && existingCalcResult.rows.length > 0) {
            continue; // Skip if calculation already exists
          }
          
          // Calculate Bitcoin amount
          let energyMWh = Math.abs(Number(farmRecordsResult.rows[0].volume));
          let energyWh = energyMWh * 1000000;
          
          // Different efficiency values for different miner models (J/TH)
          let efficiency;
          switch (minerModel) {
            case 'S19J_PRO': efficiency = 29.5; break;
            case 'S9': efficiency = 94.0; break;
            case 'M20S': efficiency = 48.0; break;
            default: efficiency = 30.0;
          }
          
          // Calculate terahashes
          let hashrateTh = energyWh / efficiency; // Wh / (J/TH) = TH
          
          // Calculate Bitcoin mined
          let btcMined = (hashrateTh * 3600) / (difficulty * Math.pow(2, 32) / Math.pow(10, 12)) * 6.25;
          
          // Insert the calculation
          await db.execute(
            sql`INSERT INTO historical_bitcoin_calculations 
                (settlement_date, settlement_period, farm_id, miner_model, 
                 bitcoin_mined, difficulty, calculated_at)
                VALUES 
                (${DATE_TO_PROCESS}, ${period}, ${farm.farm_id}, ${minerModel},
                 ${btcMined}, ${difficulty}, NOW())`
          );
          
          log(`Added Bitcoin calculation for farm ${farm.farm_id} period ${period} with model ${minerModel}: ${btcMined.toFixed(8)} BTC`);
        }
      }
      
      // Count calculations for this model
      const calcCountResult = await db.execute(
        sql`SELECT COUNT(*) as calc_count 
            FROM historical_bitcoin_calculations 
            WHERE settlement_date = ${DATE_TO_PROCESS}
            AND miner_model = ${minerModel}`
      );
      
      const calcCount = calcCountResult.rows?.[0]?.calc_count || 0;
      log(`Total Bitcoin calculations for ${minerModel}: ${calcCount}`, 'success');
    }
    
  } catch (error) {
    log(`Error calculating Bitcoin mining: ${error}`, 'error');
  }
}

async function main() {  
  try {
    log(`Starting Bitcoin calculations for ${DATE_TO_PROCESS} missing periods ${MISSING_PERIODS.join(', ')}`, 'info');
    
    // Calculate Bitcoin mining for the new records
    await calculateBitcoinMining();
    
    log(`Bitcoin calculations completed for ${DATE_TO_PROCESS} missing periods`, 'success');
  } catch (error) {
    log(`Error in main process: ${error}`, 'error');
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
}).finally(() => {
  // Don't exit immediately to allow any pending logs to be written
  setTimeout(() => {
    process.exit(0);
  }, 1000);
});
/**
 * Process Period 40 for 2025-03-28
 * 
 * This script processes just period 40 for 2025-03-28
 */
import dotenv from 'dotenv';
import fs from 'fs';
import { db } from './db';
import { sql } from 'drizzle-orm';
import { format, parse } from 'date-fns';

// Load environment variables
dotenv.config();

// Configuration
const DATE_TO_PROCESS = '2025-03-28';
const PERIOD_TO_PROCESS = 40;
const LOG_FILE = `./logs/process_period_40_2025-03-28_${format(new Date(), 'yyyy-MM-dd')}.log`;

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

async function fetchElexonDataForPeriod(): Promise<any[]> {
  try {
    // Format the date for the API (DD-MM-YYYY)
    const parsedDate = parse(DATE_TO_PROCESS, 'yyyy-MM-dd', new Date());
    const formattedDate = format(parsedDate, 'dd-MM-yyyy');
    
    // Construct the URL for the Elexon API
    const url = `https://api.bmreports.com/BMRS/B1610/v1?APIKey=${process.env.ELEXON_API_KEY}&SettlementDate=${formattedDate}&Period=${PERIOD_TO_PROCESS}&ServiceType=xml`;
    
    log(`Fetching data from Elexon API for ${DATE_TO_PROCESS} Period ${PERIOD_TO_PROCESS}`);
    
    const response = await fetch(url, {
      method: 'GET',
      headers: { 'Content-Type': 'application/xml' },
      signal: AbortSignal.timeout(30000) // 30 second timeout
    });
    
    if (!response.ok) {
      throw new Error(`API returned status code ${response.status}`);
    }
    
    const xmlData = await response.text();
    
    // Simple XML parsing to extract the relevant data
    const records: any[] = [];
    const regex = /<item>.*?<settlementDate>(.*?)<\/settlementDate>.*?<settlementPeriod>(.*?)<\/settlementPeriod>.*?<bMUnitID>(.*?)<\/bMUnitID>.*?<bMUnitType>(.*?)<\/bMUnitType>.*?<leadPartyName>(.*?)<\/leadPartyName>.*?<ngcBMUnitName>(.*?)<\/ngcBMUnitName>.*?<cashFlow>(.*?)<\/cashFlow>.*?<volume>(.*?)<\/volume>.*?<\/item>/gs;
    
    let match;
    while ((match = regex.exec(xmlData)) !== null) {
      if (match[4] === 'T') { // Only process records where BMUnitType is 'T' (Wind)
        records.push({
          settlementDate: match[1],
          settlementPeriod: parseInt(match[2]),
          bmuId: match[3],
          bmuType: match[4],
          leadPartyName: match[5],
          ngcBmuName: match[6],
          cashFlow: parseFloat(match[7]),
          volume: parseFloat(match[8])
        });
      }
    }
    
    log(`Retrieved ${records.length} wind farm records for period ${PERIOD_TO_PROCESS}`);
    return records;
  } catch (error) {
    log(`Error fetching data for period ${PERIOD_TO_PROCESS}: ${error}`, 'error');
    return [];
  }
}

async function processPeriod(): Promise<boolean> {
  try {
    // Fetch data from Elexon API
    const apiRecords = await fetchElexonDataForPeriod();
    
    log(`[${DATE_TO_PROCESS} P${PERIOD_TO_PROCESS}] API records: ${apiRecords.length}`);
    
    if (apiRecords.length === 0) {
      log(`No data available from API for ${DATE_TO_PROCESS} period ${PERIOD_TO_PROCESS}`, 'warning');
      return false;
    }
    
    // Clear any existing records for this period to avoid duplicates
    await db.execute(
      sql`DELETE FROM curtailment_records 
          WHERE settlement_date = ${DATE_TO_PROCESS} 
          AND settlement_period = ${PERIOD_TO_PROCESS}`
    );
    
    log(`Cleared any existing records for ${DATE_TO_PROCESS} period ${PERIOD_TO_PROCESS}`);
    
    // Process each record
    for (const record of apiRecords) {
      // Insert directly with SQL
      await db.execute(
        sql`INSERT INTO curtailment_records 
            (settlement_date, settlement_period, farm_id, lead_party_name, 
             volume, payment, so_flag, cadl_flag, original_price, final_price, created_at)
            VALUES 
            (${DATE_TO_PROCESS}, ${PERIOD_TO_PROCESS}, ${record.bmuId}, ${record.leadPartyName},
             ${Math.abs(record.volume)}, ${Math.abs(record.cashFlow)}, 
             false, false, 0, 0, NOW())`
      );
    }
    
    log(`Successfully inserted ${apiRecords.length} records for ${DATE_TO_PROCESS} period ${PERIOD_TO_PROCESS}`, 'success');
    return true;
  } catch (error) {
    log(`Error processing period ${PERIOD_TO_PROCESS}: ${error}`, 'error');
    return false;
  }
}

async function updateBitcoinCalculations(): Promise<void> {
  try {
    log(`Starting Bitcoin calculation updates for ${DATE_TO_PROCESS} period ${PERIOD_TO_PROCESS}`);
    
    // Perform calculations with each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      log(`Processing Bitcoin calculations for ${minerModel}`);
      
      // Get all farms for this period
      const farmsResult = await db.execute(
        sql`SELECT DISTINCT farm_id, lead_party_name 
            FROM curtailment_records 
            WHERE settlement_date = ${DATE_TO_PROCESS}
            AND settlement_period = ${PERIOD_TO_PROCESS}`
      );
      
      const farms = farmsResult.rows || [];
      log(`Found ${farms.length} farms with curtailment records for period ${PERIOD_TO_PROCESS}`);
      
      // Get mining difficulty and BTC price
      let difficulty = 113757508810853; // Default value
      let btcPrice = 66061.96; // Default price in GBP
      
      // Process each farm
      for (const farm of farms) {
        // Get volume for this farm and period
        const farmRecordsResult = await db.execute(
          sql`SELECT volume 
              FROM curtailment_records 
              WHERE settlement_date = ${DATE_TO_PROCESS} 
              AND farm_id = ${farm.farm_id}
              AND settlement_period = ${PERIOD_TO_PROCESS}`
        );
        
        if (!farmRecordsResult.rows || farmRecordsResult.rows.length === 0) {
          continue;
        }
        
        const volume = Math.abs(Number(farmRecordsResult.rows[0].volume));
        
        // Check if calculation already exists
        const existingCalcResult = await db.execute(
          sql`SELECT id FROM historical_bitcoin_calculations 
              WHERE settlement_date = ${DATE_TO_PROCESS}
              AND settlement_period = ${PERIOD_TO_PROCESS}
              AND farm_id = ${farm.farm_id}
              AND miner_model = ${minerModel}`
        );
        
        if (existingCalcResult.rows && existingCalcResult.rows.length > 0) {
          log(`Bitcoin calculation already exists for farm ${farm.farm_id} period ${PERIOD_TO_PROCESS} with model ${minerModel}`);
          continue; // Skip if calculation already exists
        }
        
        // Calculate Bitcoin amount
        let energyMWh = volume;
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
        
        // Calculate value in GBP
        let valueGbp = btcMined * btcPrice;
        
        // Insert the calculation
        await db.execute(
          sql`INSERT INTO historical_bitcoin_calculations 
              (settlement_date, settlement_period, farm_id, miner_model, 
               curtailed_energy, bitcoin_mined, value_gbp, 
               network_difficulty, btc_price_gbp, created_at)
              VALUES 
              (${DATE_TO_PROCESS}, ${PERIOD_TO_PROCESS}, ${farm.farm_id}, ${minerModel},
               ${energyMWh}, ${btcMined}, ${valueGbp}, 
               ${difficulty}, ${btcPrice}, NOW())`
        );
        
        log(`Added Bitcoin calculation for farm ${farm.farm_id} period ${PERIOD_TO_PROCESS} with model ${minerModel}: ${btcMined.toFixed(8)} BTC`);
      }
    }
    
    // Verify counts
    const calculationsCountResult = await db.execute(
      sql`SELECT miner_model, COUNT(*) as calc_count 
          FROM historical_bitcoin_calculations 
          WHERE settlement_date = ${DATE_TO_PROCESS}
          AND settlement_period = ${PERIOD_TO_PROCESS}
          GROUP BY miner_model`
    );
    
    log(`Bitcoin calculations for ${DATE_TO_PROCESS} period ${PERIOD_TO_PROCESS}:`);
    if (calculationsCountResult.rows) {
      calculationsCountResult.rows.forEach(row => {
        log(`  ${row.miner_model}: ${row.calc_count} records`);
      });
    }
    
  } catch (error) {
    log(`Error updating Bitcoin calculations: ${error}`, 'error');
  }
}

async function main() {  
  try {
    log(`Starting processing for ${DATE_TO_PROCESS} period ${PERIOD_TO_PROCESS}`, 'info');
    
    // Step 1: Process the period
    const success = await processPeriod();
    
    if (success) {
      // Step 2: Update Bitcoin calculations
      await updateBitcoinCalculations();
      
      log(`Processing completed for ${DATE_TO_PROCESS} period ${PERIOD_TO_PROCESS}`, 'success');
    } else {
      log(`Failed to process ${DATE_TO_PROCESS} period ${PERIOD_TO_PROCESS}`, 'error');
    }
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
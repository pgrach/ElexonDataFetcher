/**
 * Process Missing Periods for 2025-03-28
 * 
 * This script will:
 * 1. Process ONLY missing settlement periods (40-48) for 2025-03-28
 * 2. Update Bitcoin calculations for the new data
 * 3. Update the daily summary
 */

import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { db } from './db';
import { sql } from 'drizzle-orm';
import { format, parse } from 'date-fns';
import pLimit from 'p-limit';

// Load environment variables
dotenv.config();

// Date to process
const DATE_TO_PROCESS = '2025-03-28';
const LOG_FILE = `./logs/process_missing_periods_2025-03-28_${format(new Date(), 'yyyy-MM-dd')}.log`;
const BMU_MAPPINGS_FILE = './data/bmu_mapping.json';

// Missing periods identified (40-48)
const MISSING_PERIODS = [40, 41, 42, 43, 44, 45, 46, 47, 48];

// Create log directory if it doesn't exist
if (!fs.existsSync('./logs')) {
  fs.mkdirSync('./logs');
}

// Logger setup
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
  
  // Also log to file
  logToFile(message).catch(console.error);
}

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mappings
async function loadBmuMappings(): Promise<{
  [bmuId: string]: { name: string; leadParty: string; farmId: string }
}> {
  try {
    const data = fs.readFileSync(BMU_MAPPINGS_FILE, 'utf8');
    return JSON.parse(data);
  } catch (error) {
    log(`Error loading BMU mappings: ${error}`, 'error');
    return {};
  }
}

async function fetchElexonDataForPeriod(settlementDate: string, settlementPeriod: number): Promise<any[]> {
  try {
    // Format the date for the API (DD-MM-YYYY)
    const parsedDate = parse(settlementDate, 'yyyy-MM-dd', new Date());
    const formattedDate = format(parsedDate, 'dd-MM-yyyy');
    
    // Construct the URL for the Elexon API
    const url = `https://api.bmreports.com/BMRS/B1610/v1?APIKey=${process.env.ELEXON_API_KEY}&SettlementDate=${formattedDate}&Period=${settlementPeriod}&ServiceType=xml`;
    
    log(`Fetching data from Elexon API for ${settlementDate} Period ${settlementPeriod}`);
    
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
    // This is a basic implementation - in production, use a proper XML parser
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
    
    log(`Retrieved ${records.length} wind farm records for period ${settlementPeriod}`);
    return records;
  } catch (error) {
    log(`Error fetching data for period ${settlementPeriod}: ${error}`, 'error');
    return [];
  }
}

async function processPeriod(
  date: string, 
  period: number, 
  bmuMappings: {[bmuId: string]: { name: string; leadParty: string; farmId: string }}
): Promise<boolean> {
  try {
    // Fetch data from Elexon API
    const apiRecords = await fetchElexonDataForPeriod(date, period);
    
    log(`[${date} P${period}] API records: ${apiRecords.length}`);
    
    if (apiRecords.length === 0) {
      log(`No data available from API for ${date} period ${period}`, 'warning');
      return false;
    }
    
    // Clear any existing records for this period to avoid duplicates
    await db.execute(
      sql`DELETE FROM curtailment_records 
          WHERE settlement_date = ${date} 
          AND settlement_period = ${period}`
    );
    
    log(`Cleared any existing records for ${date} period ${period}`);
    
    // Process each record
    for (const record of apiRecords) {
      const mappingInfo = bmuMappings[record.bmuId] || {
        name: record.ngcBmuName,
        leadParty: record.leadPartyName,
        farmId: record.bmuId
      };
      
      // Insert directly with SQL
      await db.execute(
        sql`INSERT INTO curtailment_records 
            (settlement_date, settlement_period, farm_id, lead_party_name, 
             volume, payment, so_flag, cadl_flag, original_price, final_price, created_at)
            VALUES 
            (${date}, ${period}, ${record.bmuId}, ${mappingInfo.leadParty},
             ${Math.abs(record.volume)}, ${Math.abs(record.cashFlow)}, 
             false, false, 0, 0, NOW())`
      );
    }
    
    log(`Successfully inserted ${apiRecords.length} records for ${date} period ${period}`, 'success');
    return true;
  } catch (error) {
    log(`Error processing period ${period}: ${error}`, 'error');
    return false;
  }
}

async function processMissingPeriods(): Promise<void> {
  try {
    log(`Starting processing for missing periods on ${DATE_TO_PROCESS}`, 'info');
    
    // Load BMU mappings
    const bmuMappings = await loadBmuMappings();
    log(`Loaded ${Object.keys(bmuMappings).length} BMU mappings`);
    
    // Process missing periods sequentially to avoid API rate limits
    for (const period of MISSING_PERIODS) {
      log(`Processing period ${period}`);
      await processPeriod(DATE_TO_PROCESS, period, bmuMappings);
      
      // Add a delay between periods to avoid API rate limits
      if (period !== MISSING_PERIODS[MISSING_PERIODS.length - 1]) {
        log(`Pausing for 5 seconds before next period...`);
        await delay(5000);
      }
    }
    
    // Log updated totals
    const updatedTotals = await db.execute(
      sql`SELECT COUNT(*) as record_count, 
                 COUNT(DISTINCT settlement_period) as period_count,
                 ABS(SUM(volume)) as total_volume, 
                 ABS(SUM(payment)) as total_payment
          FROM curtailment_records 
          WHERE settlement_date = ${DATE_TO_PROCESS}`
    );
    
    if (updatedTotals.rows && updatedTotals.rows[0]) {
      log(`Updated data for ${DATE_TO_PROCESS}: ${JSON.stringify({
        records: updatedTotals.rows[0].record_count,
        periods: updatedTotals.rows[0].period_count,
        volume: Number(updatedTotals.rows[0].total_volume).toFixed(2),
        payment: Number(updatedTotals.rows[0].total_payment).toFixed(2)
      })}`, 'success');
    }
    
  } catch (error) {
    log(`Error processing missing periods: ${error}`, 'error');
  }
}

async function updateBitcoinCalculations(): Promise<void> {
  try {
    log(`Starting Bitcoin calculation updates for ${DATE_TO_PROCESS} missing periods`);
    
    // Perform calculations with each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      log(`Processing Bitcoin calculations for ${minerModel}`);
      
      // Get all farms for the missing periods
      const farmsResult = await db.execute(
        sql`SELECT DISTINCT farm_id, lead_party_name 
            FROM curtailment_records 
            WHERE settlement_date = ${DATE_TO_PROCESS}
            AND settlement_period IN (${MISSING_PERIODS.join(', ')})`
      );
      
      const farms = farmsResult.rows || [];
      log(`Found ${farms.length} farms with curtailment records for the missing periods`);
      
      // Get mining difficulty and BTC price
      let difficulty = 113757508810853; // Default value
      let btcPrice = 66061.96; // Default price in GBP
      
      // Process each farm
      for (const farm of farms) {
        // Get periods for this farm
        const farmRecordsResult = await db.execute(
          sql`SELECT settlement_period, volume 
              FROM curtailment_records 
              WHERE settlement_date = ${DATE_TO_PROCESS} 
              AND farm_id = ${farm.farm_id}
              AND settlement_period IN (${MISSING_PERIODS.join(', ')})`
        );
        
        const farmRecords = farmRecordsResult.rows || [];
        
        // Process all periods
        for (const record of farmRecords) {
          // Check if calculation already exists
          const existingCalcResult = await db.execute(
            sql`SELECT id FROM historical_bitcoin_calculations 
                WHERE settlement_date = ${DATE_TO_PROCESS}
                AND settlement_period = ${record.settlement_period}
                AND farm_id = ${farm.farm_id}
                AND miner_model = ${minerModel}`
          );
          
          if (existingCalcResult.rows && existingCalcResult.rows.length > 0) {
            continue; // Skip if calculation already exists
          }
          
          // Calculate Bitcoin amount
          let energyMWh = Math.abs(Number(record.volume));
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
                (${DATE_TO_PROCESS}, ${record.settlement_period}, ${farm.farm_id}, ${minerModel},
                 ${energyMWh}, ${btcMined}, ${valueGbp}, 
                 ${difficulty}, ${btcPrice}, NOW())`
          );
        }
      }
      
      // Verify counts for this model
      const calculationsCountResult = await db.execute(
        sql`SELECT COUNT(*) as calc_count 
            FROM historical_bitcoin_calculations 
            WHERE settlement_date = ${DATE_TO_PROCESS}
            AND miner_model = ${minerModel}
            AND settlement_period IN (${MISSING_PERIODS.join(', ')})`
      );
      
      if (calculationsCountResult.rows && calculationsCountResult.rows[0]) {
        log(`Added ${calculationsCountResult.rows[0].calc_count} Bitcoin calculations for ${minerModel}`, 'success');
      }
    }
    
    // Final calculation summary
    const calcSummaryResult = await db.execute(
      sql`SELECT miner_model, COUNT(*) as calc_count
          FROM historical_bitcoin_calculations
          WHERE settlement_date = ${DATE_TO_PROCESS}
          GROUP BY miner_model`
    );
    
    log(`Bitcoin calculation summary for ${DATE_TO_PROCESS}:`);
    if (calcSummaryResult.rows) {
      calcSummaryResult.rows.forEach(row => {
        log(`  ${row.miner_model}: ${row.calc_count} records`);
      });
    }
    
  } catch (error) {
    log(`Error updating Bitcoin calculations: ${error}`, 'error');
  }
}

async function updateDailySummary(): Promise<void> {
  try {
    log(`Updating daily summary for ${DATE_TO_PROCESS}`);
    
    // Calculate totals from curtailment records
    const totalsResult = await db.execute(
      sql`SELECT 
            ABS(SUM(volume)) as total_volume, 
            ABS(SUM(payment)) as total_payment 
          FROM curtailment_records 
          WHERE settlement_date = ${DATE_TO_PROCESS}`
    );
    
    // Extract results
    const totalVolume = totalsResult.rows && totalsResult.rows[0] ? 
      Math.abs(Number(totalsResult.rows[0].total_volume) || 0) : 0;
    
    const totalPayment = totalsResult.rows && totalsResult.rows[0] ? 
      Math.abs(Number(totalsResult.rows[0].total_payment) || 0) : 0;
    
    log(`Calculated totals for ${DATE_TO_PROCESS}: Volume=${totalVolume.toFixed(2)} MWh, Payment=${totalPayment.toFixed(2)} GBP`);
    
    // Check if summary exists
    const existingSummaryResult = await db.execute(
      sql`SELECT * FROM daily_summaries WHERE summary_date = ${DATE_TO_PROCESS}`
    );
    
    const hasExistingSummary = existingSummaryResult.rows && existingSummaryResult.rows.length > 0;
    
    if (hasExistingSummary) {
      // Update existing summary
      await db.execute(
        sql`UPDATE daily_summaries 
            SET total_curtailed_energy = ${totalVolume},
                total_payment = ${totalPayment},
                last_updated = NOW()
            WHERE summary_date = ${DATE_TO_PROCESS}`
      );
      log(`Updated daily summary for ${DATE_TO_PROCESS}`);
    } else {
      // Create new summary
      await db.execute(
        sql`INSERT INTO daily_summaries 
            (summary_date, total_curtailed_energy, total_payment, created_at)
            VALUES (${DATE_TO_PROCESS}, ${totalVolume}, ${totalPayment}, NOW())`
      );
      log(`Created new daily summary for ${DATE_TO_PROCESS}`);
    }
    
    // Log the updated summary
    const updatedSummaryResult = await db.execute(
      sql`SELECT * FROM daily_summaries WHERE summary_date = ${DATE_TO_PROCESS}`
    );
    
    if (updatedSummaryResult.rows && updatedSummaryResult.rows.length > 0) {
      log(`Daily summary for ${DATE_TO_PROCESS}: ${JSON.stringify(updatedSummaryResult.rows[0])}`);
    }
    
  } catch (error) {
    log(`Error updating daily summary: ${error}`, 'error');
  }
}

async function main() {  
  try {
    log(`Starting processing for ${DATE_TO_PROCESS} missing periods ${MISSING_PERIODS.join(', ')}`, 'info');
    
    // Step 1: Process missing periods
    await processMissingPeriods();
    
    // Step 2: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 3: Update daily summary
    await updateDailySummary();
    
    log(`Processing completed for ${DATE_TO_PROCESS} missing periods`, 'success');
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
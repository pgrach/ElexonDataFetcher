/**
 * Staged Reingest for March 22, 2025
 * 
 * This script allows for reingesting settlement periods in smaller batches.
 * Set START_PERIOD and END_PERIOD to control which range to process.
 * 
 * The goal is to ensure complete and accurate data for March 22, 2025.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import axios from 'axios';
import fs from 'fs';
import path from 'path';

// Configuration
const TARGET_DATE = '2025-03-22';
const START_PERIOD = 1;   // Change this to control which range to process
const END_PERIOD = 4;     // Change this to control which range to process
const ELEXON_API_BASE_URL = 'https://api.bmreports.com/BMRS';
const API_THROTTLE_MS = 500;  // Delay between API calls to prevent rate limiting

// Set your Elexon API key here or use from environment
const ELEXON_API_KEY = process.env.ELEXON_API_KEY || 'elexon_api_key';

// Create log directory if it doesn't exist
const LOG_DIR = path.join(__dirname, 'logs');
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR);
}

// Set up logging
const logFile = path.join(LOG_DIR, `staged_reingest_march_22_${START_PERIOD}_${END_PERIOD}.log`);
const logStream = fs.createWriteStream(logFile, { flags: 'a' });

function log(message: string): void {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] ${message}`;
  console.log(formattedMessage);
  logStream.write(formattedMessage + '\n');
}

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function loadBmuMappings(): Promise<{
  bmuIdToFarmId: Map<string, string>;
  bmuIdToLeadParty: Map<string, string>;
}> {
  log('Loading BMU to Farm ID mappings...');
  
  // Load mappings from database or file
  const mappingsResult = await db.execute(sql`
    SELECT 
      bmu_id, 
      farm_id, 
      lead_party 
    FROM wind_farm_bmu_mappings
  `);
  
  const bmuIdToFarmId = new Map<string, string>();
  const bmuIdToLeadParty = new Map<string, string>();
  
  for (const row of mappingsResult.rows) {
    bmuIdToFarmId.set(row.bmu_id as string, row.farm_id as string);
    bmuIdToLeadParty.set(row.bmu_id as string, row.lead_party as string);
  }
  
  log(`Loaded ${bmuIdToFarmId.size} BMU mappings`);
  return { bmuIdToFarmId, bmuIdToLeadParty };
}

async function clearExistingPeriodsData(): Promise<void> {
  try {
    log(`Clearing existing data for periods ${START_PERIOD}-${END_PERIOD} on ${TARGET_DATE}...`);
    
    // Delete curtailment records for the specific periods
    const curtailmentResult = await db.execute(sql`
      DELETE FROM curtailment_records 
      WHERE settlement_date = ${TARGET_DATE} 
      AND settlement_period BETWEEN ${START_PERIOD} AND ${END_PERIOD}
    `);
    log(`Deleted ${curtailmentResult.rowCount} existing curtailment records`);
    
    // Delete Bitcoin calculations for the specific periods
    const bitcoinResult = await db.execute(sql`
      DELETE FROM historical_bitcoin_calculations 
      WHERE settlement_date = ${TARGET_DATE} 
      AND settlement_period BETWEEN ${START_PERIOD} AND ${END_PERIOD}
    `);
    log(`Deleted ${bitcoinResult.rowCount} existing Bitcoin calculation records`);
    
  } catch (error) {
    log(`Error clearing existing data: ${error}`);
    throw error;
  }
}

async function processPeriod(
  period: number, 
  bmuIdToFarmId: Map<string, string>,
  bmuIdToLeadParty: Map<string, string>
): Promise<{ recordCount: number; totalVolume: number; totalPayment: number }> {
  try {
    log(`Processing period ${period} for ${TARGET_DATE}...`);
    
    // Fetch curtailment data from Elexon API
    const url = `${ELEXON_API_BASE_URL}/DISBSAD/v1?APIKey=${ELEXON_API_KEY}&SettlementDate=${TARGET_DATE}&Period=${period}&ServiceType=xml`;
    
    log(`Fetching data from Elexon API for period ${period}...`);
    const response = await axios.get(url);
    
    if (!response.data || !response.data.response || !response.data.response.responseBody || !response.data.response.responseBody.responseList || !response.data.response.responseBody.responseList.item) {
      log(`No data returned from API for period ${period}`);
      return { recordCount: 0, totalVolume: 0, totalPayment: 0 };
    }
    
    const items = Array.isArray(response.data.response.responseBody.responseList.item) 
      ? response.data.response.responseBody.responseList.item 
      : [response.data.response.responseBody.responseList.item];
    
    log(`Received ${items.length} records from API for period ${period}`);
    
    let insertCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process and insert data
    for (const item of items) {
      const bmuId = item.bMUnitID;
      const farmId = bmuIdToFarmId.get(bmuId);
      const leadParty = bmuIdToLeadParty.get(bmuId);
      
      if (!farmId) {
        log(`Warning: No farm ID mapping found for BMU ID ${bmuId}, skipping`);
        continue;
      }
      
      const volume = parseFloat(item.activeFlag === 'Y' ? item.acceptanceVolume : '0');
      const price = parseFloat(item.activeFlag === 'Y' ? item.acceptancePrice : '0');
      const payment = volume * price * -1; // Payment is negative in the database
      
      // Only insert if there's actual curtailment (volume > 0)
      if (volume > 0) {
        await db.execute(sql`
          INSERT INTO curtailment_records (
            settlement_date, 
            settlement_period, 
            farm_id, 
            bmu_id, 
            curtailed_volume, 
            price, 
            payment,
            lead_party
          ) VALUES (
            ${TARGET_DATE}, 
            ${period}, 
            ${farmId}, 
            ${bmuId}, 
            ${volume}, 
            ${price}, 
            ${payment},
            ${leadParty || null}
          )
        `);
        
        insertCount++;
        totalVolume += volume;
        totalPayment += payment;
      }
    }
    
    log(`Inserted ${insertCount} records for period ${period}`);
    log(`Period ${period} total volume: ${totalVolume.toFixed(2)} MWh, total payment: £${Math.abs(totalPayment).toFixed(2)}`);
    
    return { recordCount: insertCount, totalVolume, totalPayment };
  } catch (error) {
    log(`Error processing period ${period}: ${error}`);
    throw error;
  }
}

async function getCompletedPeriods(): Promise<Set<number>> {
  try {
    const result = await db.execute(sql`
      SELECT DISTINCT settlement_period
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const completedPeriods = new Set<number>();
    for (const row of result.rows) {
      completedPeriods.add(parseInt(row.settlement_period as string));
    }
    
    return completedPeriods;
  } catch (error) {
    log(`Error getting completed periods: ${error}`);
    throw error;
  }
}

async function main(): Promise<void> {
  log('========================================');
  log(`Starting staged reingestion for ${TARGET_DATE}, periods ${START_PERIOD}-${END_PERIOD}`);
  log('========================================');
  
  try {
    // Load BMU mappings
    const { bmuIdToFarmId, bmuIdToLeadParty } = await loadBmuMappings();
    
    // Clear existing data for the periods we're about to process
    await clearExistingPeriodsData();
    
    // Get set of completed periods
    const completedPeriods = await getCompletedPeriods();
    log(`Found ${completedPeriods.size} already completed periods for ${TARGET_DATE}`);
    
    // Process each period in the batch
    let batchTotalRecords = 0;
    let batchTotalVolume = 0;
    let batchTotalPayment = 0;
    
    for (let period = START_PERIOD; period <= END_PERIOD; period++) {
      // Only process if not already completed elsewhere
      if (!completedPeriods.has(period) || (period >= START_PERIOD && period <= END_PERIOD)) {
        const { recordCount, totalVolume, totalPayment } = await processPeriod(period, bmuIdToFarmId, bmuIdToLeadParty);
        
        batchTotalRecords += recordCount;
        batchTotalVolume += totalVolume;
        batchTotalPayment += totalPayment;
        
        // Add a delay between API calls to prevent rate limiting
        await delay(API_THROTTLE_MS);
      } else {
        log(`Skipping period ${period} as it's already processed outside this batch`);
      }
    }
    
    // Log batch totals
    log('----------------------------------------');
    log(`Batch processing completed for periods ${START_PERIOD}-${END_PERIOD}`);
    log(`Batch total records: ${batchTotalRecords}`);
    log(`Batch total volume: ${batchTotalVolume.toFixed(2)} MWh`);
    log(`Batch total payment: £${Math.abs(batchTotalPayment).toFixed(2)}`);
    
    // Check current state
    const allCompletedPeriods = await getCompletedPeriods();
    log(`Total completed periods: ${allCompletedPeriods.size}/48`);
    
    if (allCompletedPeriods.size === 48) {
      log('SUCCESS: All 48 settlement periods have been processed!');
    } else {
      log(`NOTICE: ${48 - allCompletedPeriods.size} periods still need to be processed.`);
      log('Missing periods: ' + Array.from({length: 48}, (_, i) => i + 1).filter(p => !allCompletedPeriods.has(p)).join(', '));
    }
    
  } catch (error) {
    log(`Error in main function: ${error}`);
    throw error;
  } finally {
    logStream.end();
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
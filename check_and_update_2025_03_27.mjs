/**
 * Check and Update 2025-03-27 Data
 * 
 * This script checks volumes and payments from Elexon API for 2025-03-27 period by period
 * and compares it with data in the curtailment_records table. If there's anything missing
 * or incorrect, it ingests the missing data from the Elexon API.
 */

import { db } from './db';
import { and, between, eq, sql } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

// Configuration for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const LOG_FILE = `check_update_2025_03_27_${new Date().toISOString().split('T')[0]}.log`;
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds

// Target date
const date = '2025-03-27';

// Log helper
async function logToFile(message) {
  try {
    await fs.appendFile(LOG_FILE, `${message}\n`);
  } catch (error) {
    console.error('Error writing to log file:', error);
  }
}

function log(message, type = "info") {
  const timestamp = new Date().toISOString();
  const coloredMessage = type === "info" 
    ? `\x1b[36m${message}\x1b[0m` // Cyan
    : type === "success" 
    ? `\x1b[32m${message}\x1b[0m` // Green
    : type === "warning" 
    ? `\x1b[33m${message}\x1b[0m` // Yellow
    : `\x1b[31m${message}\x1b[0m`; // Red for error
  
  console.log(`[${timestamp}] ${coloredMessage}`);
  logToFile(`[${timestamp}] [${type.toUpperCase()}] ${message}`);
}

async function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mappings (wind farm IDs and lead party names)
async function loadBmuMappings() {
  try {
    const data = await fs.readFile(BMU_MAPPING_PATH, 'utf-8');
    const mapping = JSON.parse(data);
    
    // Extract wind farm IDs
    const windFarmIds = mapping.filter(item => 
      item.fuelType === 'WIND' && item.bmuType === 'T'
    ).map(item => item.bmUnitID);
    
    // Create mapping of BMU ID to lead party name
    const bmuLeadPartyMap = new Map();
    mapping.forEach(item => {
      bmuLeadPartyMap.set(item.bmUnitID, item.leadPartyName);
    });
    
    log(`Loaded ${windFarmIds.length} wind farm IDs from BMU mapping`, "success");
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    log(`Error loading BMU mappings: ${error}`, "error");
    throw new Error('Failed to load BMU mappings');
  }
}

// Get data from Elexon API for a specific period
async function getElexonDataForPeriod(period, windFarmIds) {
  const url = `${API_BASE_URL}/datasets/PHYBMDATA/entities`;
  
  const params = {
    settlementDate: date,
    settlementPeriod: period,
    bmUnitIds: windFarmIds.join(','),
  };
  
  let retries = 0;
  while (retries < MAX_RETRIES) {
    try {
      const response = await axios.get(url, { params });
      
      if (response.status === 200 && response.data && Array.isArray(response.data.data)) {
        // Extract and transform the records
        return response.data.data.map(item => ({
          id: item.bmUnitId,
          volume: -item.quantity, // Negate the quantity to get curtailment volume
          originalPrice: item.cashflow / Math.abs(item.quantity), // Calculate price from cashflow/quantity
          finalPrice: item.cashflow / Math.abs(item.quantity),
          soFlag: item.soFlag,
          cadlFlag: item.cadlFlag
        }));
      } else {
        throw new Error(`Invalid response format: ${JSON.stringify(response.data)}`);
      }
    } catch (error) {
      retries++;
      log(`Error fetching data for period ${period} (attempt ${retries}): ${error.message}`, "warning");
      if (retries < MAX_RETRIES) {
        await delay(RETRY_DELAY);
      } else {
        log(`Max retries reached for period ${period}`, "error");
        return [];
      }
    }
  }
  
  return [];
}

// Get current data from database for a specific period
async function getDatabaseDataForPeriod(period) {
  try {
    const records = await db.select()
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    return records.map(record => ({
      id: record.farmId,
      volume: Number(record.volume),
      payment: Number(record.payment),
      originalPrice: Number(record.originalPrice),
      finalPrice: Number(record.finalPrice),
      soFlag: record.soFlag,
      cadlFlag: record.cadlFlag
    }));
  } catch (error) {
    log(`Error fetching database data for period ${period}: ${error}`, "error");
    return [];
  }
}

// Compare API data with database data
function compareData(apiData, dbData) {
  // Create maps for easier lookup
  const dbDataMap = new Map();
  dbData.forEach(record => {
    dbDataMap.set(record.id, record);
  });
  
  const missing = [];
  const different = [];
  const identical = [];
  
  apiData.forEach(apiRecord => {
    const dbRecord = dbDataMap.get(apiRecord.id);
    
    if (!dbRecord) {
      // Record missing from database
      missing.push(apiRecord);
    } else {
      // Check if values match (allow for small floating-point differences)
      const volumeDiff = Math.abs(apiRecord.volume - dbRecord.volume);
      const priceDiff = Math.abs(apiRecord.originalPrice - dbRecord.originalPrice);
      
      if (volumeDiff > 0.01 || priceDiff > 0.01 || apiRecord.soFlag !== dbRecord.soFlag || apiRecord.cadlFlag !== dbRecord.cadlFlag) {
        // Record exists but has different values
        different.push({
          api: apiRecord,
          db: dbRecord,
          diff: {
            volume: volumeDiff,
            price: priceDiff,
            soFlag: apiRecord.soFlag !== dbRecord.soFlag,
            cadlFlag: apiRecord.cadlFlag !== dbRecord.cadlFlag
          }
        });
      } else {
        // Record matches
        identical.push(apiRecord);
      }
    }
  });
  
  return { missing, different, identical };
}

// Insert or update records in the database
async function updateDatabaseRecords(records, period, bmuLeadPartyMap) {
  if (records.length === 0) {
    return;
  }
  
  try {
    // First delete any existing records for these farm IDs in this period to avoid duplicates
    const farmIds = records.map(record => record.id);
    
    await db.delete(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period),
          sql`${curtailmentRecords.farmId} IN (${farmIds.join(',')})`
        )
      );
    
    // Now insert the new records
    const valuesToInsert = records.map(record => {
      const volume = record.volume;
      const payment = Math.abs(volume) * record.originalPrice;
      
      return {
        settlementDate: date,
        settlementPeriod: period,
        farmId: record.id,
        leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
        volume: volume.toString(), // Keep original value (negative for curtailment)
        payment: payment.toString(),
        originalPrice: record.originalPrice.toString(),
        finalPrice: record.finalPrice.toString(),
        soFlag: record.soFlag,
        cadlFlag: record.cadlFlag
      };
    });
    
    await db.insert(curtailmentRecords).values(valuesToInsert);
    
    log(`Period ${period}: Updated ${records.length} records`, "success");
  } catch (error) {
    log(`Period ${period}: Error updating records: ${error}`, "error");
  }
}

// Process a specific settlement period
async function processPeriod(period, windFarmIds, bmuLeadPartyMap) {
  log(`Processing period ${period}...`, "info");
  
  // Get data from Elexon API
  const apiData = await getElexonDataForPeriod(period, windFarmIds);
  if (apiData.length === 0) {
    log(`Period ${period}: No data from Elexon API`, "warning");
    return { missing: 0, different: 0, identical: 0, updated: 0 };
  }
  
  // Get data from database
  const dbData = await getDatabaseDataForPeriod(period);
  
  // Compare data
  const { missing, different, identical } = compareData(apiData, dbData);
  
  log(`Period ${period}: ${missing.length} missing, ${different.length} different, ${identical.length} identical`, "info");
  
  // Update database with missing or different records
  const recordsToUpdate = [
    ...missing,
    ...different.map(diff => diff.api)
  ];
  
  if (recordsToUpdate.length > 0) {
    await updateDatabaseRecords(recordsToUpdate, period, bmuLeadPartyMap);
  }
  
  return {
    missing: missing.length,
    different: different.length,
    identical: identical.length,
    updated: recordsToUpdate.length
  };
}

// Main function to process all periods for the target date
async function processDate() {
  try {
    log(`Starting data check and update for ${date}`, "info");
    
    // Initialize log file
    await fs.writeFile(LOG_FILE, `=== Data Check and Update for ${date} ===\n`);
    
    // Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Get current database status
    const dbStats = await db
      .select({
        recordCount: sql`COUNT(*)`,
        periodCount: sql`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        farmCount: sql`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
        totalVolume: sql`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    log(`Current DB state: ${dbStats[0]?.recordCount || 0} records, ${dbStats[0]?.periodCount || 0}/48 periods, ${dbStats[0]?.farmCount || 0} farms, ${Number(dbStats[0]?.totalVolume || 0).toFixed(2)} MWh, £${Number(dbStats[0]?.totalPayment || 0).toFixed(2)}`, "info");
    
    // Process all 48 settlement periods
    let totalStats = {
      missing: 0,
      different: 0,
      identical: 0,
      updated: 0
    };
    
    for (let period = 1; period <= 48; period++) {
      const periodStats = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
      
      totalStats.missing += periodStats.missing;
      totalStats.different += periodStats.different;
      totalStats.identical += periodStats.identical;
      totalStats.updated += periodStats.updated;
      
      // Add a small delay to avoid rate limiting
      await delay(500);
    }
    
    // Get updated database status
    const updatedDbStats = await db
      .select({
        recordCount: sql`COUNT(*)`,
        periodCount: sql`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        farmCount: sql`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
        totalVolume: sql`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    log(`Updated DB state: ${updatedDbStats[0]?.recordCount || 0} records, ${updatedDbStats[0]?.periodCount || 0}/48 periods, ${updatedDbStats[0]?.farmCount || 0} farms, ${Number(updatedDbStats[0]?.totalVolume || 0).toFixed(2)} MWh, £${Number(updatedDbStats[0]?.totalPayment || 0).toFixed(2)}`, "info");
    
    log(`Summary: Found ${totalStats.missing} missing and ${totalStats.different} different records, with ${totalStats.identical} identical records. Updated ${totalStats.updated} records.`, "success");
    
    if (totalStats.updated === 0) {
      log(`No updates were needed, database is in sync with Elexon API data.`, "success");
    } else {
      log(`Successfully synced database with Elexon API data.`, "success");
    }
    
  } catch (error) {
    log(`Error processing date: ${error}`, "error");
  }
}

// Run the script
processDate().catch(error => {
  log(`Unhandled error: ${error}`, "error");
  process.exit(1);
}).finally(() => {
  log('Script execution complete.', "info");
});
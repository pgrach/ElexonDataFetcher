/**
 * Check and Update 2025-03-27 Data
 * 
 * This script checks volumes and payments from Elexon API for 2025-03-27 period by period
 * and compares it with data in the curtailment_records table. If there's anything missing
 * or incorrect, it ingests the missing data from the Elexon API.
 */

import { db } from './db';
import { and, between, eq } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios, { AxiosResponse } from 'axios';
import fs from 'fs/promises';
import { existsSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';
import { sql } from 'drizzle-orm';

// Set up __dirname equivalent for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
// Try both possible locations
const BMU_MAPPING_PATH = existsSync(path.join(__dirname, 'data', 'bmu_mapping.json')) 
  ? path.join(__dirname, 'data', 'bmu_mapping.json')
  : path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const LOG_FILE = `check_update_2025_03_27_${new Date().toISOString().split('T')[0]}.log`;
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds

// Target date
const date = '2025-03-27';

// Log helper
async function logToFile(message: string): Promise<void> {
  try {
    await fs.appendFile(LOG_FILE, `${message}\n`);
  } catch (error) {
    console.error('Error writing to log file:', error);
  }
}

function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
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

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mappings (wind farm IDs and lead party names)
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
}> {
  try {
    log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`, "info");
    
    const data = await fs.readFile(BMU_MAPPING_PATH, 'utf-8');
    const mapping = JSON.parse(data);
    
    // Extract wind farm IDs - filter wind farms
    const filteredBmus = mapping.filter((item: any) => 
      item.fuelType === 'WIND'
    );
    
    // Create a Set of IDs for faster lookups
    const windFarmIds = new Set<string>();
    filteredBmus.forEach((item: any) => {
      windFarmIds.add(item.elexonBmUnit);
    });
    
    // Create mapping of BMU ID to lead party name
    const bmuLeadPartyMap = new Map<string, string>();
    mapping.forEach((item: any) => {
      bmuLeadPartyMap.set(item.elexonBmUnit, item.leadPartyName);
    });
    
    log(`Found ${windFarmIds.size} wind farm BMUs`, "success");
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    log(`Error loading BMU mappings: ${error}`, "error");
    return { windFarmIds: new Set(), bmuLeadPartyMap: new Map() };
  }
}

// Get data from Elexon API for a specific period
async function getElexonDataForPeriod(period: number, windFarmIds: Set<string>): Promise<any[]> {
  // Make parallel requests for bids and offers
  log(`Processing period ${period} with ${windFarmIds.size} BMUs`, "info");

  let allResults: any[] = [];
  
  try {
    // Create URLs for bids and offers endpoints
    const bidUrl = `${API_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`;
    const offerUrl = `${API_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`;
    
    log(`Fetching data from bid and offer endpoints for period ${period}`, "info");
    
    // Execute both requests
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get<any>(bidUrl, { 
        headers: { 'Accept': 'application/json' },
        timeout: 30000 // 30 second timeout
      }),
      axios.get<any>(offerUrl, { 
        headers: { 'Accept': 'application/json' },
        timeout: 30000 // 30 second timeout
      })
    ]).catch(error => {
      if (axios.isAxiosError(error)) {
        log(`Error fetching data for period ${period}: ${error.message}, Status: ${error.response?.status}, Data: ${JSON.stringify(error.response?.data)}`, "error");
      } else {
        log(`Error fetching data for period ${period}: ${error.message}`, "error");
      }
      return [{ data: { data: [] } }, { data: { data: [] } }];
    });
    
    // Process bids response
    if (bidsResponse.data && Array.isArray(bidsResponse.data.data)) {
      // Filter for valid wind farm bids
      const validBids = bidsResponse.data.data.filter((record: any) => 
        record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
      );
      
      allResults = [...allResults, ...validBids];
    }
    
    // Process offers response
    if (offersResponse.data && Array.isArray(offersResponse.data.data)) {
      // Filter for valid wind farm offers
      const validOffers = offersResponse.data.data.filter((record: any) => 
        record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
      );
      
      allResults = [...allResults, ...validOffers];
    }
    
    if (allResults.length > 0) {
      const periodTotal = allResults.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      const periodPayment = allResults.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice), 0);
      log(`Period ${period}: Got ${allResults.length} records (${periodTotal.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`, "success");
    } else {
      log(`Period ${period}: No valid records found`, "warning");
    }
    
    return allResults;
  } catch (error: any) {
    if (axios.isAxiosError(error)) {
      log(`Error fetching data for period ${period}: ${error.message}, Status: ${error.response?.status}, Data: ${JSON.stringify(error.response?.data)}`, "error");
    } else {
      log(`Error fetching data for period ${period}: ${error.message}`, "error");
    }
    return [];
  }
}

// Get current data from database for a specific period
async function getDatabaseDataForPeriod(period: number): Promise<any[]> {
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
function compareData(apiData: any[], dbData: any[]): {
  missing: any[],
  different: any[],
  identical: any[]
} {
  // Create maps for easier lookup
  const dbDataMap = new Map();
  dbData.forEach(record => {
    dbDataMap.set(record.id, record);
  });
  
  const missing: any[] = [];
  const different: any[] = [];
  const identical: any[] = [];
  
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
async function updateDatabaseRecords(records: any[], period: number, bmuLeadPartyMap: Map<string, string>): Promise<void> {
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
async function processPeriod(period: number, windFarmIds: Set<string>, bmuLeadPartyMap: Map<string, string>): Promise<{
  missing: number,
  different: number,
  identical: number,
  updated: number
}> {
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
async function processDate(startPeriod: number = 1): Promise<void> {
  try {
    console.log(`Starting data check and update for ${date} from period ${startPeriod}`);
    log(`Starting data check and update for ${date} from period ${startPeriod}`, "info");
    
    // Don't initialize log file if we're continuing from a specific period
    if (startPeriod === 1) {
      console.log(`Initializing log file: ${LOG_FILE}`);
      await fs.writeFile(LOG_FILE, `=== Data Check and Update for ${date} ===\n`);
    } else {
      // Just append a continuation message
      await fs.appendFile(LOG_FILE, `\n=== Continuing Data Check and Update from period ${startPeriod} ===\n`);
    }
    
    // Load BMU mappings
    console.log(`Loading BMU mappings from: ${BMU_MAPPING_PATH}`);
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Get current database status
    const dbStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        farmCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
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
    
    // Log first 5 BMU IDs for debugging
    log(`First 5 BMU IDs: ${Array.from(windFarmIds).slice(0, 5).join(', ')}`, "info");
    
    // Process all periods from the start period to 48
    for (let period = startPeriod; period <= 48; period++) {
      try {
        const periodStats = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
        
        totalStats.missing += periodStats.missing;
        totalStats.different += periodStats.different;
        totalStats.identical += periodStats.identical;
        totalStats.updated += periodStats.updated;
        
        // Add a small delay to avoid rate limiting
        await delay(500);
      } catch (error) {
        log(`Error processing period ${period}: ${error}`, "error");
        // Continue with the next period even if this one fails
        await delay(2000); // Longer delay after an error
      }
    }
    
    // Get updated database status
    const updatedDbStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        farmCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
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

// Parse command-line arguments
function parseCommandLineArgs(): { startPeriod: number } {
  let startPeriod = 1;

  // Get all arguments
  const args = process.argv.slice(2);
  
  // Look for --start-period or -p
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--start-period' || args[i] === '-p') {
      // Get the next argument as the period number
      if (i + 1 < args.length) {
        const periodArg = parseInt(args[i + 1], 10);
        if (!isNaN(periodArg) && periodArg >= 1 && periodArg <= 48) {
          startPeriod = periodArg;
        } else {
          console.error(`Invalid period number: ${args[i + 1]}. Using default of 1.`);
        }
      }
    }
  }

  return { startPeriod };
}

// Log directly to console for immediate feedback
console.log('Starting script execution...');

// Parse command-line arguments
const { startPeriod } = parseCommandLineArgs();

// Run the script
processDate(startPeriod).catch(error => {
  console.log(`Unhandled error: ${error}`);
  log(`Unhandled error: ${error}`, "error");
  process.exit(1);
}).finally(() => {
  console.log('Script execution complete.');
  log('Script execution complete.', "info");
});
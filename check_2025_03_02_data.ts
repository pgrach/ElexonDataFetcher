/**
 * Check 2025-03-02 Data Completeness
 * 
 * This script compares database records with Elexon API data for 2025-03-02
 * to identify any missing records and updates them if found.
 */
import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { eq, and } from 'drizzle-orm';
import fs from 'fs';
import { delay, fetchBidsOffers } from './server/services/elexon';
import { ElexonBidOffer } from './server/types/elexon';

// Load environment variables
// Using direct import for dotenv in ESM
import * as dotenv from 'dotenv';
dotenv.config();

// Load Wind Farm IDs from the mapping file
async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    const bmuMapping = JSON.parse(fs.readFileSync('./server/data/bmuMapping.json', 'utf8'));
    const windFarmIds = new Set<string>();
    
    for (const item of bmuMapping) {
      if (item.id) {
        windFarmIds.add(item.id);
      }
    }
    
    console.log(`Loaded ${windFarmIds.size} wind farm IDs from mapping file`);
    return windFarmIds;
  } catch (error) {
    console.error(`Error loading wind farm IDs: ${error.message}`);
    return new Set<string>();
  }
}

/**
 * Fetch data from Elexon API for a specific date and period
 * using our existing service
 */
async function fetchElexonData(date: string, period: number, windFarmIds: Set<string>): Promise<ElexonBidOffer[]> {
  try {
    console.log(`Fetching Elexon data for ${date} period ${period}...`);
    
    // Use our existing Elexon service
    const allRecords = await fetchBidsOffers(date, period);
    
    // Filter for wind farm records (with negative volume, which indicates curtailment)
    const filteredRecords = allRecords.filter(record => 
      record.bmUnit && 
      windFarmIds.has(record.bmUnit) && 
      record.volume < 0
    );
    
    console.log(`Found ${filteredRecords.length} wind farm records for period ${period}`);
    return filteredRecords;
  } catch (error) {
    console.error(`Error fetching Elexon data: ${error.message}`);
    return [];
  }
}

/**
 * Compare database records with Elexon data for a specific period
 */
async function compareDataForPeriod(date: string, period: number, windFarmIds: Set<string>): Promise<ElexonBidOffer[]> {
  // Get records from database
  const dbRecords = await db.select()
    .from(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, date),
        eq(curtailmentRecords.settlementPeriod, period)
      )
    );
  
  // Get records from Elexon API
  const elexonRecords = await fetchElexonData(date, period, windFarmIds);
  
  // Convert DB records to a map for easy lookup
  const dbRecordsMap = new Map();
  dbRecords.forEach(record => {
    dbRecordsMap.set(record.farmId, record);
  });
  
  // Find missing records
  const missingRecords: ElexonBidOffer[] = [];
  for (const elexonRecord of elexonRecords) {
    if (elexonRecord.bmUnit && !dbRecordsMap.has(elexonRecord.bmUnit)) {
      console.log(`Found missing record for ${elexonRecord.bmUnit} in period ${period}`);
      missingRecords.push(elexonRecord);
    }
  }
  
  return missingRecords;
}

/**
 * Insert missing records into the database
 */
async function insertMissingRecords(missingRecords: ElexonBidOffer[]): Promise<void> {
  if (missingRecords.length === 0) {
    return;
  }
  
  console.log(`Inserting ${missingRecords.length} missing records...`);
  
  const records = missingRecords.map(record => ({
    settlementDate: record.settlementDate,
    settlementPeriod: record.settlementPeriod,
    farmId: record.bmUnit || '',
    volume: record.volume,
    price: record.finalPrice,
    payment: record.volume * record.finalPrice,
    soFlag: record.soFlag,
    cadlFlag: record.cadlFlag,
    leadPartyName: record.leadPartyName || '',
    createdAt: new Date()
  }));
  
  try {
    await db.insert(curtailmentRecords).values(records);
    console.log(`Successfully inserted ${records.length} records`);
  } catch (error) {
    console.error(`Error inserting records: ${error.message}`);
  }
}

/**
 * Main function to check data completeness for 2025-03-02
 */
async function checkDataCompleteness() {
  const date = '2025-03-02';
  console.log(`Checking data completeness for ${date}...`);
  
  // Load wind farm IDs
  const windFarmIds = await loadWindFarmIds();
  
  let totalMissingRecords = 0;
  
  // Check each period (1-48)
  for (let period = 1; period <= 48; period++) {
    // Add a delay to avoid rate limits
    if (period > 1) {
      await delay(200); // 200ms delay between requests
    }
    
    const missingRecords = await compareDataForPeriod(date, period, windFarmIds);
    totalMissingRecords += missingRecords.length;
    
    if (missingRecords.length > 0) {
      await insertMissingRecords(missingRecords);
    }
  }
  
  console.log(`Completed check for ${date}. Found ${totalMissingRecords} missing records.`);
  
  // If we found and inserted missing records, update the Bitcoin calculations
  if (totalMissingRecords > 0) {
    console.log(`Updating Bitcoin calculations for ${date}...`);
    // Use dynamic import for ESM
    const historicalReconciliation = await import('./server/services/historicalReconciliation');
    await historicalReconciliation.reprocessDay(date);
    console.log(`Bitcoin calculations have been updated for ${date}`);
  } else {
    console.log(`No missing records found for ${date}. All data is complete.`);
  }
}

// Run the check
checkDataCompleteness().catch(console.error);
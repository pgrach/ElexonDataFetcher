/**
 * Process a Single Period for 2025-03-28
 * 
 * This script processes a single specified period for 2025-03-28.
 * 
 * Usage: npx tsx process_period.ts <period_number>
 * Example: npx tsx process_period.ts 45
 */

import { db } from './db';
import { and, eq, sql } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { processSingleDay } from './server/services/bitcoinService';

// ES Modules setup for dirname equivalent
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds
const MINER_MODEL_LIST = ['S19J_PRO', 'S9', 'M20S'];

// Date to process
const date = '2025-03-28';
const period = parseInt(process.argv[2] || '45');

// Helper function to delay execution
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mappings once
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  console.log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`);
  
  try {
    const data = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(data);
    
    const windFarmIds = new Set<string>();
    const bmuLeadPartyMap = new Map<string, string>();
    
    for (const bmu of bmuMapping) {
      windFarmIds.add(bmu.elexonBmUnit);
      bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName);
    }
    
    console.log(`Found ${windFarmIds.size} wind farm BMUs`);
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    console.error(`Error loading BMU mapping: ${error}`);
    return { windFarmIds: new Set(), bmuLeadPartyMap: new Map() };
  }
}

// Process a single settlement period
async function processPeriod(
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>,
  attempt: number = 1
): Promise<{
  success: boolean;
  records: number;
  volume: number;
  payment: number;
}> {
  console.log(`Processing period ${period} (attempt ${attempt})`);
  
  try {
    // Fetch data from the Elexon API using stack endpoints
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(`${API_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`),
      axios.get(`${API_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`)
    ]).catch(error => {
      console.error(`Error fetching data: ${error.message}`);
      return [{ data: { data: [] } }, { data: { data: [] } }];
    });
    
    const bidsData = bidsResponse.data?.data || [];
    const offersData = offersResponse.data?.data || [];
    const data = [...bidsData, ...offersData];
    
    // Filter to keep only valid wind farm records
    const validRecords = data.filter((record: any) => {
      return windFarmIds.has(record.id) && record.volume < 0; // Negative volume indicates curtailment
    });
    
    const totalVolume = validRecords.reduce((sum: number, record: any) => sum + Math.abs(record.volume), 0);
    const totalPayment = validRecords.reduce((sum: number, record: any) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
    
    console.log(`Period ${period}: Found ${validRecords.length} valid records (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
    
    let recordsAdded = 0;
    let totalVolumeAdded = 0;
    let totalPaymentAdded = 0;
    
    // Clear all existing records for this period - simpler approach that guarantees no duplicates
    try {
      const deleteResult = await db.delete(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, date),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );
      
      console.log(`Period ${period}: Cleared existing records before insertion`);
    } catch (error) {
      console.error(`Period ${period}: Error clearing existing records: ${error}`);
    }
    
    // Prepare all records for bulk insertion
    const recordsToInsert = validRecords.map((record: any) => {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      // Track totals for return value
      totalVolumeAdded += volume;
      totalPaymentAdded += payment;
      
      return {
        settlementDate: date,
        settlementPeriod: period,
        farmId: record.id,
        leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
        volume: record.volume.toString(), // Keep negative value
        payment: payment.toString(),
        originalPrice: record.originalPrice.toString(),
        finalPrice: record.finalPrice.toString(),
        soFlag: record.soFlag,
        cadlFlag: record.cadlFlag
      };
    });
    
    // Insert all records in a single transaction if there are any
    if (recordsToInsert.length > 0) {
      try {
        await db.insert(curtailmentRecords).values(recordsToInsert);
        recordsAdded = recordsToInsert.length;
        
        // Log summary 
        console.log(`Period ${period}: Added ${recordsAdded} records (${totalVolumeAdded.toFixed(2)} MWh, £${totalPaymentAdded.toFixed(2)})`);
        
      } catch (error) {
        console.error(`Period ${period}: Error bulk inserting records: ${error}`);
      }
    }
    
    return { 
      success: true, 
      records: recordsAdded,
      volume: totalVolumeAdded,
      payment: totalPaymentAdded
    };
    
  } catch (error) {
    console.error(`Error processing period ${period}: ${error}`);
    
    // Retry logic
    if (attempt < MAX_RETRIES) {
      console.log(`Retrying period ${period} in ${RETRY_DELAY/1000} seconds... (attempt ${attempt + 1}/${MAX_RETRIES})`);
      await delay(RETRY_DELAY);
      return processPeriod(windFarmIds, bmuLeadPartyMap, attempt + 1);
    }
    
    return { 
      success: false, 
      records: 0,
      volume: 0,
      payment: 0
    };
  }
}

// Update Bitcoin calculations
async function updateBitcoinCalculations(): Promise<void> {
  console.log(`Updating Bitcoin calculations for ${date}...`);
  
  for (const minerModel of MINER_MODEL_LIST) {
    try {
      console.log(`Processing ${minerModel} for ${date}...`);
      await processSingleDay(date, minerModel);
      console.log(`Successfully processed ${minerModel} for ${date}`);
    } catch (error) {
      console.error(`Error processing ${minerModel} for ${date}:`, error);
    }
  }
  
  // Verify the results
  const bitcoinStats = await db
    .select({
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
    
  console.log(`\nFinal curtailment stats:`, bitcoinStats[0]);
}

// Main function
async function main() {
  console.log(`Processing period ${period} for ${date}`);
  
  // Load BMU mappings
  const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
  
  // Process the period
  const result = await processPeriod(windFarmIds, bmuLeadPartyMap);
  
  if (result.success) {
    console.log(`Successfully processed period ${period}`);
    console.log(`Records: ${result.records}`);
    console.log(`Volume: ${result.volume.toFixed(2)} MWh`);
    console.log(`Payment: £${result.payment.toFixed(2)}`);
    
    // Update Bitcoin calculations
    if (result.records > 0) {
      await updateBitcoinCalculations();
    }
  } else {
    console.error(`Failed to process period ${period}`);
  }
  
  process.exit(0);
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
/**
 * Fetch and process specific periods directly from Elexon API
 * 
 * This script fetches data from Elexon API for specific periods and
 * processes it for insertion into the database.
 */

import { fetchBidsOffers } from './server/services/elexon';
import { db } from './db';
import { curtailmentRecords } from './db/schema';
import fs from 'fs/promises';
import path from 'path';
import { addMinutes } from 'date-fns';

// Configuration
const DATE_TO_PROCESS = '2025-03-31'; 
const PERIODS_TO_PROCESS = [24, 25, 26]; // Just a few periods to test

// BMU mapping
let windFarmIds: Set<string> | null = null;

async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    if (windFarmIds === null) {
      console.log('Loading BMU mapping from server/data/bmuMapping.json...');
      const mappingContent = await fs.readFile(path.join('server', 'data', 'bmuMapping.json'), 'utf8');
      const bmuMapping = JSON.parse(mappingContent);
      windFarmIds = new Set(bmuMapping.map((bmu: any) => bmu.elexonBmUnit));
      console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    }
    return windFarmIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

async function processPeriod(date: string, period: number): Promise<{
  records: number;
  volume: number;
  payment: number;
}> {
  try {
    const validWindFarmIds = await loadWindFarmIds();
    console.log(`Processing ${date} Period ${period}...`);
    
    // Fetch data from Elexon API
    const records = await fetchBidsOffers(date, period);
    
    if (!records || records.length === 0) {
      console.log(`[${date} P${period}] No records found`);
      return { records: 0, volume: 0, payment: 0 };
    }
    
    // Filter for valid curtailment records (negative volume, flagged, valid wind farm)
    const validRecords = records.filter(record => 
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      validWindFarmIds.has(record.id)
    );
    
    if (validRecords.length === 0) {
      console.log(`[${date} P${period}] No valid curtailment records found`);
      return { records: 0, volume: 0, payment: 0 };
    }
    
    // Log the records we're about to process
    const totalVolume = validRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
    const totalPayment = validRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice), 0);
    console.log(`[${date} P${period}] Found ${validRecords.length} valid records (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
    
    // Insert all records in a batch
    const batchInserts = validRecords.map(record => {
      const paymentValue = Math.abs(record.volume) * record.originalPrice * -1;
      
      return {
        settlementDate: date,
        settlementPeriod: period,
        farmId: record.id,
        leadPartyName: record.leadPartyName || 'Unknown',
        volume: record.volume.toString(),
        payment: paymentValue.toString(),
        originalPrice: record.originalPrice.toString(),
        finalPrice: record.finalPrice.toString(),
        soFlag: record.soFlag,
        cadlFlag: record.cadlFlag || false,
        createdAt: new Date()
      };
    });
    
    // Insert all records
    for (const record of batchInserts) {
      await db.insert(curtailmentRecords).values(record);
      console.log(`Inserted record for ${record.farmId}: Volume ${record.volume}, Payment ${record.payment}`);
    }
    
    return {
      records: validRecords.length,
      volume: totalVolume,
      payment: totalPayment
    };
  } catch (error) {
    console.error(`Error processing period ${period} for ${date}:`, error);
    return { records: 0, volume: 0, payment: 0 };
  }
}

async function main() {
  try {
    console.log(`\n=== Fetching Data for ${DATE_TO_PROCESS} (Selected Periods) ===\n`);
    
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const period of PERIODS_TO_PROCESS) {
      const result = await processPeriod(DATE_TO_PROCESS, period);
      totalRecords += result.records;
      totalVolume += result.volume;
      totalPayment += result.payment;
    }
    
    console.log(`\n=== Processing Complete ===\n`);
    console.log(`Total Records: ${totalRecords}`);
    console.log(`Total Volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total Payment: £${totalPayment.toFixed(2)}`);
    
  } catch (error) {
    console.error('Error in processing:', error);
    process.exit(1);
  }
}

main();
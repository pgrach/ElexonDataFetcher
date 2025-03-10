#!/usr/bin/env tsx
/**
 * Batch Process Periods
 * 
 * This script processes a specified range of settlement periods for a given date
 * to help with reingestion of Elexon API data into curtailment_records.
 * 
 * It's designed to be called by the complete_reingestion_process.ts script
 * to process periods in smaller batches to avoid timeouts.
 * 
 * Usage:
 *   npx tsx batch_process_periods.ts <date> <start_period> <end_period>
 * 
 * Example:
 *   npx tsx batch_process_periods.ts 2025-03-09 1 16
 */

import { fetchBidsOffers } from "./server/services/elexon";
import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { delay } from "./server/services/elexon";
import path from "path";
import fs from "fs/promises";
import { fileURLToPath } from 'url';

// Get the directory path
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Constants
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");

// Parse command line arguments
const args = process.argv.slice(2);
const date = args[0];
const startPeriod = parseInt(args[1] || '1');
const endPeriod = parseInt(args[2] || '48');

// Validate args
if (!date || isNaN(startPeriod) || isNaN(endPeriod)) {
  console.error("Usage: npx tsx batch_process_periods.ts <date> <start_period> <end_period>");
  process.exit(1);
}

// Load BMU mappings
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
}> {
  try {
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    const windFarmIds = new Set<string>(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit as string)
    );
    
    const bmuLeadPartyMap = new Map<string, string>(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => [bmu.elexonBmUnit as string, bmu.leadPartyName as string || 'Unknown'])
    );
    
    console.log(`Found ${windFarmIds.size} wind farm BMUs`);
    
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

// Process a single settlement period
async function processPeriod(
  period: number,
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  records: number;
  volume: number;
  payment: number;
}> {
  try {
    // Fetch the records from Elexon API
    const records = await fetchBidsOffers(date, period);
    
    // Filter for valid wind farm curtailment records
    const validRecords = records.filter(record =>
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      windFarmIds.has(record.id)
    );
    
    if (validRecords.length > 0) {
      console.log(`[${date} P${period}] Processing ${validRecords.length} records`);
    }
    
    // Track total volume and payment
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each record
    for (const record of validRecords) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      // Insert into database
      try {
        await db.insert(curtailmentRecords).values({
          settlementDate: date,
          settlementPeriod: period,
          farmId: record.id,
          leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
          volume: record.volume.toString(), // Keep the original negative value
          payment: payment.toString(),
          originalPrice: record.originalPrice.toString(),
          finalPrice: record.finalPrice.toString(),
          soFlag: record.soFlag,
          cadlFlag: record.cadlFlag
        });
        
        console.log(`[${date} P${period}] Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
        
        totalVolume += volume;
        totalPayment += payment;
      } catch (error) {
        console.error(`[${date} P${period}] Error inserting record for ${record.id}:`, error);
      }
    }
    
    if (totalVolume > 0) {
      console.log(`[${date} P${period}] Total: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    }
    
    return {
      records: validRecords.length,
      volume: totalVolume,
      payment: totalPayment
    };
  } catch (error) {
    console.error(`Error processing period ${period} for date ${date}:`, error);
    return { records: 0, volume: 0, payment: 0 };
  }
}

// Main function
async function main() {
  console.log(`Processing settlement periods ${startPeriod}-${endPeriod} for ${date}`);
  
  try {
    // Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each period in the range
    for (let period = startPeriod; period <= endPeriod; period++) {
      const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
      
      totalRecords += result.records;
      totalVolume += result.volume;
      totalPayment += result.payment;
      
      // Add small delay between periods to avoid rate limiting
      await delay(1000);
    }
    
    console.log(`Processed periods ${startPeriod}-${endPeriod} for ${date}:`);
    console.log(`- Total records: ${totalRecords}`);
    console.log(`- Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`- Total payment: £${totalPayment.toFixed(2)}`);
    
    process.exit(0);
  } catch (error) {
    console.error(`Error in batch processing:`, error);
    process.exit(1);
  }
}

// Run the script
main();
#!/usr/bin/env tsx
/**
 * Batch Reingestion Tool
 * 
 * This script provides a focused way to reingest Elexon data for specific periods of a date,
 * allowing for more manageable processing of large datasets.
 * 
 * Usage:
 *   npx tsx reingest_single_batch.ts <date> <start_period> <end_period>
 * 
 * Example:
 *   npx tsx reingest_single_batch.ts 2025-03-09 1 16
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, sql, count, and, between } from "drizzle-orm";
import { isValidDateString } from "./server/utils/dates";
import { fetchBidsOffers, delay } from "./server/services/elexon";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

// Get directory info
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Constants
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");

// Parse command line arguments
const args = process.argv.slice(2);
const date = args[0];
const startPeriod = parseInt(args[1] || '1');
const endPeriod = parseInt(args[2] || '16');

// Validate arguments
if (!date || !isValidDateString(date) || isNaN(startPeriod) || isNaN(endPeriod)) {
  console.error(`
Usage: npx tsx reingest_single_batch.ts <date> <start_period> <end_period>

Example: npx tsx reingest_single_batch.ts 2025-03-09 1 16
  `);
  process.exit(1);
}

// Helper function for logging
function log(message: string, type: "info" | "success" | "warning" | "error" = "info") {
  const timestamp = new Date().toISOString().split('T')[1].split('.')[0];
  
  let prefix = "";
  switch (type) {
    case "success":
      prefix = "\x1b[32m✓\x1b[0m "; // Green checkmark
      break;
    case "warning":
      prefix = "\x1b[33m⚠\x1b[0m "; // Yellow warning
      break;
    case "error":
      prefix = "\x1b[31m✗\x1b[0m "; // Red X
      break;
    default:
      prefix = "\x1b[36m•\x1b[0m "; // Blue dot for info
  }
  
  console.log(`${prefix}[${timestamp}] ${message}`);
}

// Load wind farm BMU mappings
async function loadWindFarmIds(): Promise<{ 
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
}> {
  try {
    log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`, "info");
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
    
    log(`Found ${windFarmIds.size} wind farm BMUs`, "info");
    
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    log(`Error loading BMU mapping: ${error}`, "error");
    throw error;
  }
}

// Process a single settlement period
async function processPeriod(
  period: number,
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  recordsAdded: number;
  totalVolume: number;
  totalPayment: number;
}> {
  try {
    // Fetch data from Elexon API
    const records = await fetchBidsOffers(date, period);
    
    // Filter for valid curtailment records
    const validRecords = records.filter(record =>
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      windFarmIds.has(record.id)
    );
    
    if (validRecords.length > 0) {
      log(`Period ${period}: ${validRecords.length} records found`, "info");
    }
    
    let recordsAdded = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each record
    for (const record of validRecords) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      try {
        await db.insert(curtailmentRecords).values({
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
        });
        
        recordsAdded++;
        totalVolume += volume;
        totalPayment += payment;
        
        log(`Added ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`, "success");
      } catch (error) {
        log(`Error inserting record for ${record.id}: ${error}`, "error");
      }
    }
    
    if (recordsAdded > 0) {
      log(`Period ${period} complete: ${recordsAdded} records, ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`, "success");
    } else {
      log(`No valid records for period ${period}`, "warning");
    }
    
    return { recordsAdded, totalVolume, totalPayment };
  } catch (error) {
    log(`Error processing period ${period}: ${error}`, "error");
    return { recordsAdded: 0, totalVolume: 0, totalPayment: 0 };
  }
}

// Main function
async function main() {
  try {
    log(`Starting reingestion for ${date} periods ${startPeriod}-${endPeriod}`, "info");
    
    // Clear existing records for these periods
    const deleteResult = await db.delete(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          between(curtailmentRecords.settlementPeriod, startPeriod, endPeriod)
        )
      );
    
    log(`Cleared existing records for periods ${startPeriod}-${endPeriod}`, "info");
    
    // Load wind farm IDs
    const { windFarmIds, bmuLeadPartyMap } = await loadWindFarmIds();
    
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    let periodsWithData = 0;
    
    // Process each period
    for (let period = startPeriod; period <= endPeriod; period++) {
      const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
      
      totalRecords += result.recordsAdded;
      totalVolume += result.totalVolume;
      totalPayment += result.totalPayment;
      
      if (result.recordsAdded > 0) {
        periodsWithData++;
      }
      
      // Add a delay between periods to avoid rate limiting
      await delay(1500);
    }
    
    // Final summary
    log(`Batch processing complete for ${date} periods ${startPeriod}-${endPeriod}`, "success");
    log(`Total records added: ${totalRecords}`, "info");
    log(`Total volume: ${totalVolume.toFixed(2)} MWh`, "info");
    log(`Total payment: £${totalPayment.toFixed(2)}`, "info");
    log(`Periods with data: ${periodsWithData}/${endPeriod - startPeriod + 1}`, "info");
    
    process.exit(0);
  } catch (error) {
    log(`Error during batch processing: ${error}`, "error");
    process.exit(1);
  }
}

// Run the script
main();
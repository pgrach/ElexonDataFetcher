/**
 * Elexon Data Validation for a Single Period
 * 
 * This script validates and fixes a specific settlement period for March 28, 2025
 * against the Elexon API, which is considered the authoritative source of truth.
 * 
 * Usage: npx tsx validate_single_period.ts [period_number]
 * Example: npx tsx validate_single_period.ts 25
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, and } from "drizzle-orm";
import { fetchBidsOffers } from "./server/services/elexon";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TARGET_DATE = '2025-03-28';
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");

// Get the period from command line arguments or use default
const DEFAULT_PERIOD = 25;
const periodArg = process.argv[2];
const PERIOD = periodArg ? parseInt(periodArg, 10) : DEFAULT_PERIOD;

if (isNaN(PERIOD) || PERIOD < 1 || PERIOD > 48) {
  console.error('Invalid period number. Please provide a value between 1 and 48.');
  process.exit(1);
}

// Load BMU mapping to get valid wind farm IDs
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  try {
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    const windFarmIds = new Set<string>(
      bmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );
    
    const bmuLeadPartyMap = new Map<string, string>();
    for (const bmu of bmuMapping.filter((bmu: any) => bmu.fuelType === "WIND")) {
      bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown');
    }
    
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

// Validate one period
async function validateAndFixPeriod(): Promise<{
  totalRecords: number;
  matchingRecords: number;
  missingRecords: number;
  updatedRecords: number;
  addedRecords: number;
  volumeDifferenceTotal: number;
  paymentDifferenceTotal: number;
}> {
  console.log(`\nValidating period ${PERIOD} for ${TARGET_DATE}...`);
  
  // Results
  const result = {
    totalRecords: 0,
    matchingRecords: 0,
    missingRecords: 0,
    updatedRecords: 0,
    addedRecords: 0,
    volumeDifferenceTotal: 0,
    paymentDifferenceTotal: 0
  };
  
  try {
    // Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Get existing records from database
    const existingRecords = await db
      .select()
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, PERIOD)
        )
      );
    
    console.log(`Found ${existingRecords.length} existing records in database for period ${PERIOD}`);
    
    // Create mapping of farmId -> record for quick lookup
    const existingRecordsMap = new Map();
    for (const record of existingRecords) {
      existingRecordsMap.set(record.farmId, record);
    }
    
    // Fetch data from Elexon API (source of truth)
    const apiRecords = await fetchBidsOffers(TARGET_DATE, PERIOD);
    
    // Filter for valid curtailment records
    const validApiRecords = apiRecords.filter(record =>
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      windFarmIds.has(record.id)
    );
    
    result.totalRecords = validApiRecords.length;
    
    if (validApiRecords.length === 0) {
      console.log(`No valid curtailment records found for ${TARGET_DATE} period ${PERIOD}`);
      return result;
    }
    
    console.log(`Processing ${validApiRecords.length} records from Elexon API for period ${PERIOD}`);
    
    const records = {
      toAdd: [],
      toUpdate: []
    };
    
    // Track volume and payment totals
    let apiTotalVolume = 0;
    let apiTotalPayment = 0;
    let dbTotalVolume = 0;
    let dbTotalPayment = 0;
    
    // Process each valid record from the API
    for (const apiRecord of validApiRecords) {
      const volume = Math.abs(apiRecord.volume);
      const payment = volume * apiRecord.originalPrice;
      
      // Add to API totals
      apiTotalVolume += volume;
      apiTotalPayment += payment;
      
      // Check if record exists in database
      const existingRecord = existingRecordsMap.get(apiRecord.id);
      
      if (!existingRecord) {
        // Record is missing - track for addition
        result.missingRecords++;
        result.addedRecords++;
        
        records.toAdd.push({
          settlementDate: TARGET_DATE,
          settlementPeriod: PERIOD,
          farmId: apiRecord.id,
          leadPartyName: bmuLeadPartyMap.get(apiRecord.id) || 'Unknown',
          volume: apiRecord.volume.toString(), // Keep the original negative value
          payment: payment.toString(),
          originalPrice: apiRecord.originalPrice.toString(),
          finalPrice: apiRecord.finalPrice.toString(),
          soFlag: apiRecord.soFlag,
          cadlFlag: apiRecord.cadlFlag
        });
        
        result.volumeDifferenceTotal += volume;
        result.paymentDifferenceTotal += payment;
        
        console.log(`MISSING: Record for ${apiRecord.id} in period ${PERIOD}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
      } else {
        // Record exists - validate values
        const existingVolume = Math.abs(parseFloat(existingRecord.volume));
        const existingPayment = parseFloat(existingRecord.payment);
        
        // Add to DB totals
        dbTotalVolume += existingVolume;
        dbTotalPayment += existingPayment;
        
        // Check if there's a significant discrepancy
        const volumeDiff = Math.abs(existingVolume - volume);
        const paymentDiff = Math.abs(existingPayment - payment);
        
        // Use threshold to account for floating point precision
        if (volumeDiff > 0.001 || paymentDiff > 0.001) {
          // Significant difference found - track for update
          result.updatedRecords++;
          
          records.toUpdate.push({
            id: existingRecord.id,
            volume: apiRecord.volume.toString(),
            payment: payment.toString(),
            originalPrice: apiRecord.originalPrice.toString(),
            finalPrice: apiRecord.finalPrice.toString(),
            soFlag: apiRecord.soFlag,
            cadlFlag: apiRecord.cadlFlag
          });
          
          result.volumeDifferenceTotal += (volume - existingVolume);
          result.paymentDifferenceTotal += (payment - existingPayment);
          
          console.log(`MISMATCH: Record for ${apiRecord.id} in period ${PERIOD}:`);
          console.log(`  Volume: ${existingVolume.toFixed(2)} MWh → ${volume.toFixed(2)} MWh (diff: ${(volume - existingVolume).toFixed(2)} MWh)`);
          console.log(`  Payment: £${existingPayment.toFixed(2)} → £${payment.toFixed(2)} (diff: £${(payment - existingPayment).toFixed(2)})`);
        } else {
          result.matchingRecords++;
        }
      }
      
      // Remove the record from the map since it's been processed
      existingRecordsMap.delete(apiRecord.id);
    }
    
    // At this point, any records left in existingRecordsMap shouldn't be there
    // They are in our database but not in the API response
    if (existingRecordsMap.size > 0) {
      console.log(`WARNING: Found ${existingRecordsMap.size} records in database that are not in API response for period ${PERIOD}`);
      
      // Calculate totals for records not found in API
      for (const [farmId, record] of existingRecordsMap.entries()) {
        const volume = Math.abs(parseFloat(record.volume));
        const payment = parseFloat(record.payment);
        
        dbTotalVolume += volume;
        dbTotalPayment += payment;
        
        console.log(`  Unmatched record: ${farmId}, Period ${PERIOD}, ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
      }
    }
    
    // Print summary of discrepancies
    console.log('\nDiscrepancy Summary:');
    console.log(`  API Records: ${validApiRecords.length}`);
    console.log(`  Database Records: ${existingRecords.length}`);
    console.log(`  Records to Add: ${records.toAdd.length}`);
    console.log(`  Records to Update: ${records.toUpdate.length}`);
    console.log(`  Records in DB not in API: ${existingRecordsMap.size}`);
    console.log('\nVolume & Payment Comparison:');
    console.log(`  API Total Volume: ${apiTotalVolume.toFixed(2)} MWh`);
    console.log(`  DB Total Volume: ${dbTotalVolume.toFixed(2)} MWh`);
    console.log(`  Volume Difference: ${(apiTotalVolume - dbTotalVolume).toFixed(2)} MWh`);
    console.log(`  API Total Payment: £${apiTotalPayment.toFixed(2)}`);
    console.log(`  DB Total Payment: £${dbTotalPayment.toFixed(2)}`);
    console.log(`  Payment Difference: £${(apiTotalPayment - dbTotalPayment).toFixed(2)}`);
    
    // Apply changes if needed
    if (records.toAdd.length > 0 || records.toUpdate.length > 0) {
      const prompt = 'Apply these changes to the database? (y/n): ';
      process.stdout.write(prompt);
      
      // Auto-apply for now since this is automated
      //const response = 'y'; 
      const response = await new Promise<string>(resolve => {
        process.stdin.once('data', data => {
          resolve(data.toString().trim().toLowerCase());
        });
      });
      
      if (response === 'y' || response === 'yes') {
        console.log('\nApplying changes...');
        
        // Add missing records
        if (records.toAdd.length > 0) {
          await db.insert(curtailmentRecords).values(records.toAdd);
          console.log(`Added ${records.toAdd.length} new records.`);
        }
        
        // Update mismatched records
        for (const record of records.toUpdate) {
          const { id, ...updateData } = record;
          await db.update(curtailmentRecords)
            .set(updateData)
            .where(eq(curtailmentRecords.id, id));
        }
        
        if (records.toUpdate.length > 0) {
          console.log(`Updated ${records.toUpdate.length} existing records.`);
        }
        
        console.log('All changes applied successfully.');
      } else {
        console.log('\nChanges not applied. Database remains unchanged.');
      }
    } else {
      console.log('\nNo changes needed. All records are accurate.');
    }
    
    return result;
  } catch (error) {
    console.error(`Error validating period ${PERIOD}:`, error);
    return result;
  }
}

// Main function
async function main(): Promise<void> {
  console.log(`=== Validating March 28, 2025 Settlement Period ${PERIOD} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    // Validate and fix one period
    const result = await validateAndFixPeriod();
    
    // Print validation summary
    console.log('\n=== Validation Summary ===');
    console.log(`Total records from API: ${result.totalRecords}`);
    console.log(`Matching records: ${result.matchingRecords}`);
    console.log(`Missing records: ${result.missingRecords}`);
    console.log(`Updated records: ${result.updatedRecords}`);
    console.log(`Total volume difference: ${result.volumeDifferenceTotal.toFixed(2)} MWh`);
    console.log(`Total payment difference: £${result.paymentDifferenceTotal.toFixed(2)}`);
    
    console.log(`\nValidation completed successfully at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Error during validation process:', error);
    process.exit(1);
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
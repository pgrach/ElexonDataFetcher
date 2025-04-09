/**
 * Simple JavaScript script for reingestion of 2025-03-24 data
 * 
 * This is a non-TypeScript version that should work more reliably
 * with the existing environment configuration.
 */

import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { eq } from "drizzle-orm";
import { readFile } from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

// Constants
const TARGET_DATE = '2025-03-24';
const BATCH_SIZE = 12; // Process 12 periods at a time

// Import the elexon service
import("../services/elexon.js").then(async ({ fetchBidsOffers }) => {
  try {
    console.log('\n============================================');
    console.log('STARTING CURTAILMENT REINGESTION (2025-03-24)');
    console.log('============================================\n');
    
    const startTime = Date.now();
    
    // Path setup for BMU mapping
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = path.dirname(__filename);
    const BMU_MAPPING_PATH = path.join(__dirname, "../../data/bmu_mapping.json");
    
    // Load wind farm BMU IDs
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    console.log(`Loaded ${bmuMapping.length} BMU mappings`);
    
    const windFarmBmuIds = new Set(
      bmuMapping
        .filter(bmu => bmu.fuelType === "WIND")
        .map(bmu => bmu.elexonBmUnit)
    );
    
    const bmuLeadPartyMap = new Map(
      bmuMapping
        .filter(bmu => bmu.fuelType === "WIND")
        .map(bmu => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
    );
    
    console.log(`Found ${windFarmBmuIds.size} wind farm BMUs`);
    
    console.log(`\n=== Starting reingestion for ${TARGET_DATE} ===`);
    
    let totalVolume = 0;
    let totalPayment = 0;
    let recordsProcessed = 0;
    
    // Step 1: Clear existing records for the target date
    console.log(`Clearing existing records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    console.log(`Cleared existing records`);
    
    // Process all 48 periods in batches
    for (let startPeriod = 1; startPeriod <= 48; startPeriod += BATCH_SIZE) {
      const endPeriod = Math.min(startPeriod + BATCH_SIZE - 1, 48);
      const periodPromises = [];
      
      console.log(`Processing periods ${startPeriod} to ${endPeriod}...`);
      
      for (let period = startPeriod; period <= endPeriod; period++) {
        periodPromises.push((async () => {
          try {
            // Fetch data from Elexon API
            const records = await fetchBidsOffers(TARGET_DATE, period);
            
            // Filter for valid wind farm curtailment records
            const validRecords = records.filter(record =>
              record.volume < 0 && 
              (record.soFlag || record.cadlFlag) &&
              windFarmBmuIds.has(record.id)
            );
            
            if (validRecords.length > 0) {
              console.log(`[${TARGET_DATE} P${period}] Processing ${validRecords.length} records`);
            }
            
            // Insert each valid record into the database
            const periodResults = await Promise.all(
              validRecords.map(async record => {
                const volume = Math.abs(record.volume);
                const payment = volume * record.originalPrice;
                
                try {
                  await db.insert(curtailmentRecords).values({
                    settlementDate: TARGET_DATE,
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
                  
                  recordsProcessed++;
                  console.log(`[${TARGET_DATE} P${period}] Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
                  return { volume, payment };
                } catch (error) {
                  console.error(`[${TARGET_DATE} P${period}] Error inserting record for ${record.id}:`, error);
                  return { volume: 0, payment: 0 };
                }
              })
            );
            
            // Calculate totals for this period
            const periodTotal = periodResults.reduce(
              (acc, curr) => ({
                volume: acc.volume + curr.volume,
                payment: acc.payment + curr.payment
              }),
              { volume: 0, payment: 0 }
            );
            
            if (periodTotal.volume > 0) {
              console.log(`[${TARGET_DATE} P${period}] Total: ${periodTotal.volume.toFixed(2)} MWh, £${periodTotal.payment.toFixed(2)}`);
            }
            
            totalVolume += periodTotal.volume;
            totalPayment += periodTotal.payment;
            
            return periodTotal;
          } catch (error) {
            console.error(`Error processing period ${period} for date ${TARGET_DATE}:`, error);
            return { volume: 0, payment: 0 };
          }
        })());
      }
      
      // Wait for all period promises to complete
      await Promise.all(periodPromises);
    }
    
    console.log(`\n=== Reingestion Summary for ${TARGET_DATE} ===`);
    console.log(`Records processed: ${recordsProcessed}`);
    console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    const endTime = Date.now();
    console.log('\n============================================');
    console.log('CURTAILMENT REINGESTION COMPLETED');
    console.log(`Duration: ${((endTime - startTime) / 1000).toFixed(2)} seconds`);
    console.log('============================================\n');
    console.log('NOTE: This process only updated the curtailment_records table.');
    console.log('To update summary tables and Bitcoin calculations, run a separate script.');
    
    process.exit(0);
  } catch (error) {
    console.error('\nREINGESTION FAILED:', error);
    process.exit(1);
  }
});
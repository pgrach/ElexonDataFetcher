/**
 * Specialized script to reingest data for 2025-03-24
 * 
 * This script handles:
 * 1. Reingesting curtailment data from Elexon API for 2025-03-24
 * 2. Updates only the curtailment_records table without affecting other tables
 * 3. Provides detailed logging on the process and results
 */

import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { eq } from "drizzle-orm";
import { fetchBidsOffers } from "../services/elexon";
import path from "path";
import fs from "fs/promises";
import { fileURLToPath } from 'url';

// Constants
const TARGET_DATE = '2025-03-24';
const BATCH_SIZE = 12; // Process 12 periods at a time

// Path setup for BMU mapping
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BMU_MAPPING_PATH = path.join(__dirname, "../../data/bmu_mapping.json");

// In-memory caches
let windFarmBmuIds: Set<string> | null = null;
let bmuLeadPartyMap: Map<string, string> | null = null;

/**
 * Load wind farm BMU IDs and lead party names from the mapping file
 */
async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    if (windFarmBmuIds === null || bmuLeadPartyMap === null) {
      console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
      const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
      const bmuMapping = JSON.parse(mappingContent);
      console.log(`Loaded ${bmuMapping.length} BMU mappings`);

      windFarmBmuIds = new Set(
        bmuMapping
          .filter((bmu: any) => bmu.fuelType === "WIND")
          .map((bmu: any) => bmu.elexonBmUnit)
      );

      bmuLeadPartyMap = new Map(
        bmuMapping
          .filter((bmu: any) => bmu.fuelType === "WIND")
          .map((bmu: any) => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
      );

      console.log(`Found ${windFarmBmuIds.size} wind farm BMUs`);
    }

    if (!windFarmBmuIds || !bmuLeadPartyMap) {
      throw new Error('Failed to initialize BMU mappings');
    }

    return windFarmBmuIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

/**
 * Reingest all curtailment records for 2025-03-24
 */
export async function reingestCurtailmentRecords(): Promise<void> {
  console.log(`\n=== Starting reingestion for ${TARGET_DATE} ===`);
  
  const validWindFarmIds = await loadWindFarmIds();
  let totalVolume = 0;
  let totalPayment = 0;
  let recordsProcessed = 0;

  // Step 1: Clear existing records for the target date
  console.log(`Clearing existing records for ${TARGET_DATE}...`);
  const deleteResult = await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  console.log(`Cleared existing records`);

  // Create an array to store all inserted record IDs for verification
  const insertedRecordIds: (string | number)[] = [];

  // Step 2: Process all 48 periods in batches
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
            validWindFarmIds.has(record.id)
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
                const result = await db.insert(curtailmentRecords).values({
                  settlementDate: TARGET_DATE,
                  settlementPeriod: period,
                  farmId: record.id,
                  leadPartyName: bmuLeadPartyMap?.get(record.id) || 'Unknown',
                  volume: record.volume.toString(), // Keep the original negative value
                  payment: payment.toString(),
                  originalPrice: record.originalPrice.toString(),
                  finalPrice: record.finalPrice.toString(),
                  soFlag: record.soFlag,
                  cadlFlag: record.cadlFlag
                }).returning({ id: curtailmentRecords.id });

                if (result && result[0]) {
                  insertedRecordIds.push(result[0].id.toString());
                  recordsProcessed++;
                }

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
  console.log(`Note: Only curtailment_records table was updated.`);
  console.log(`To update summary tables, please run the corresponding update scripts.`);
}

// Only run the script directly if it's the main module
if (require.main === module) {
  (async () => {
    try {
      console.log('Starting reingestion script for 2025-03-24...');
      
      const startTime = Date.now();
      await reingestCurtailmentRecords();
      const endTime = Date.now();
      
      console.log(`\nReingestion completed in ${((endTime - startTime) / 1000).toFixed(2)} seconds.`);
      process.exit(0);
    } catch (error) {
      console.error('Error during reingestion:', error);
      process.exit(1);
    }
  })();
}
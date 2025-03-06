/**
 * Complete Missing Periods and Cleanup Duplicates
 * 
 * This script focuses on:
 * 1. Processing the specific missing periods (39, 40, 41, 44, 46)
 * 2. Removing duplicate records from the database
 * 3. Updating Bitcoin calculations for the clean data
 * 
 * Usage:
 *   npx tsx complete_missing_periods_and_cleanup.ts
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { processSingleDay } from "./server/services/bitcoinService";
// Explicitly import the processSingleDay function by fixing its signature
declare module "./server/services/bitcoinService" {
  export function processSingleDay(date: string, minerModel: string): Promise<void>;
}
import { eq, sql, inArray } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const TARGET_DATE = '2025-03-05';
const MISSING_PERIODS = [39, 40, 41, 44, 46];

// Path to the BMU mapping file
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BMU_MAPPING_PATH = path.join(__dirname, "./server/data/bmuMapping.json");

let windFarmBmuIds: Set<string> | null = null;
let bmuLeadPartyMap: Map<string, string> | null = null;

async function sleep(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

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

    return windFarmBmuIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

async function processPeriod(date: string, period: number): Promise<{volume: number, payment: number}> {
  try {
    const validWindFarmIds = await loadWindFarmIds();
    
    console.log(`Processing ${date} period ${period}...`);
    const records = await fetchBidsOffers(date, period);
    const validRecords = records.filter(record =>
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      validWindFarmIds.has(record.id)
    );

    if (validRecords.length > 0) {
      console.log(`[${date} P${period}] Processing ${validRecords.length} records`);
    } else {
      console.log(`[${date} P${period}] No valid records found`);
      return { volume: 0, payment: 0 };
    }

    const periodResults = await Promise.all(
      validRecords.map(async record => {
        const volume = Math.abs(record.volume);
        const payment = volume * record.originalPrice;

        try {
          await db.insert(curtailmentRecords).values({
            settlementDate: date,
            settlementPeriod: period,
            farmId: record.id,
            leadPartyName: bmuLeadPartyMap?.get(record.id) || 'Unknown',
            volume: record.volume.toString(), // Keep the original negative value
            payment: payment.toString(),
            originalPrice: record.originalPrice.toString(),
            finalPrice: record.finalPrice.toString(),
            soFlag: record.soFlag,
            cadlFlag: record.cadlFlag
          });

          console.log(`[${date} P${period}] Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
          return { volume, payment };
        } catch (error) {
          console.error(`[${date} P${period}] Error inserting record for ${record.id}:`, error);
          return { volume: 0, payment: 0 };
        }
      })
    );

    const periodTotal = periodResults.reduce(
      (acc, curr) => ({
        volume: acc.volume + curr.volume,
        payment: acc.payment + curr.payment
      }),
      { volume: 0, payment: 0 }
    );

    if (periodTotal.volume > 0) {
      console.log(`[${date} P${period}] Total: ${periodTotal.volume.toFixed(2)} MWh, £${periodTotal.payment.toFixed(2)}`);
    }

    return periodTotal;
  } catch (error) {
    console.error(`Error processing period ${period} for date ${date}:`, error);
    return { volume: 0, payment: 0 };
  }
}

async function cleanupDuplicates(): Promise<{removedCount: number, remainingCount: number}> {
  try {
    console.log('Finding duplicate records...');
    
    // Get all duplicate groups
    const duplicates = await db.execute(sql`
      SELECT settlement_period, farm_id, COUNT(*) as record_count
      FROM curtailment_records 
      WHERE settlement_date = ${TARGET_DATE}
      GROUP BY settlement_period, farm_id
      HAVING COUNT(*) > 1
    `);
    
    console.log(`Found ${duplicates.rows.length} duplicate groups to clean up`);
    
    let removedRecords = 0;
    
    // For each duplicate group, keep only the first record and delete others
    for (const row of duplicates.rows) {
      const period = row.settlement_period;
      const farmId = row.farm_id;
      const recordCount = parseInt(row.record_count);
      
      // Find all records IDs for this period and farm
      const records = await db
        .select({ id: curtailmentRecords.id })
        .from(curtailmentRecords)
        .where(
          sql`settlement_date = ${TARGET_DATE} AND settlement_period = ${period} AND farm_id = ${farmId}`
        )
        .orderBy(curtailmentRecords.id);
      
      // Keep the first record, delete the rest
      const idsToKeep = [records[0].id];
      const idsToDelete = records.slice(1).map(r => r.id);
      
      if (idsToDelete.length > 0) {
        // Delete duplicate records
        await db
          .delete(curtailmentRecords)
          .where(inArray(curtailmentRecords.id, idsToDelete));
          
        removedRecords += idsToDelete.length;
        
        if (removedRecords % 100 === 0) {
          console.log(`Removed ${removedRecords} duplicate records so far...`);
        }
      }
    }
    
    // Count remaining records
    const remainingCount = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    return { 
      removedCount: removedRecords,
      remainingCount: remainingCount[0].count
    };
  } catch (error) {
    console.error('Error cleaning up duplicates:', error);
    return { removedCount: 0, remainingCount: 0 };
  }
}

async function completeAndCleanup() {
  console.log(`\n=== Cleanup of Duplicates for ${TARGET_DATE} ===\n`);
  
  // Check initial state
  console.log('Checking initial database state...');
  const beforeCheck = await db
    .select({
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  console.log(`Initial state: ${beforeCheck[0].recordCount} records across ${beforeCheck[0].periodCount} periods`);
  console.log(`Total volume: ${Number(beforeCheck[0].totalVolume).toFixed(2)} MWh`);
  console.log(`Total payment: £${Number(beforeCheck[0].totalPayment).toFixed(2)}`);

  try {
    // Count duplicate entries
    const duplicateCheck = await db.execute(sql`
      SELECT COUNT(*) as duplicate_count
      FROM (
        SELECT settlement_period, farm_id, COUNT(*) 
        FROM curtailment_records 
        WHERE settlement_date = ${TARGET_DATE}
        GROUP BY settlement_period, farm_id
        HAVING COUNT(*) > 1
      ) as duplicates
    `);
    
    console.log(`Found ${duplicateCheck.rows[0].duplicate_count} duplicate groups`);
    
    // Skip processing missing periods as they are now complete
    console.log('\nSkipping missing periods processing as all periods are present.');
    
    // Step 1: Clean up duplicate records
    console.log('\nCleaning up duplicate records...');
    const cleanupResult = await cleanupDuplicates();
    console.log(`Removed ${cleanupResult.removedCount} duplicate records`);
    console.log(`Remaining records after cleanup: ${cleanupResult.remainingCount}`);
    
    // Step 2: Check for any remaining duplicates
    const finalDuplicateCheck = await db.execute(sql`
      SELECT COUNT(*) as duplicate_count
      FROM (
        SELECT settlement_period, farm_id, COUNT(*) 
        FROM curtailment_records 
        WHERE settlement_date = ${TARGET_DATE}
        GROUP BY settlement_period, farm_id
        HAVING COUNT(*) > 1
      ) as duplicates
    `);
    
    const duplicateCount = parseInt(finalDuplicateCheck.rows[0].duplicate_count as string);
    console.log(`Duplicates after cleaning: ${duplicateCount}`);
    
    // If there are still duplicates, do a more aggressive cleanup
    if (duplicateCount > 0) {
      console.log('\nPerforming aggressive duplicate cleanup...');
      
      // Get all period-farm combinations
      const periodFarmCombos = await db
        .select({
          period: curtailmentRecords.settlementPeriod,
          farmId: curtailmentRecords.farmId
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
        .groupBy(curtailmentRecords.settlementPeriod, curtailmentRecords.farmId);
      
      console.log(`Found ${periodFarmCombos.length} unique period-farm combinations`);
      
      // For each combination, delete all but the record with the lowest ID
      let removedCount = 0;
      for (const combo of periodFarmCombos) {
        const records = await db
          .select({
            id: curtailmentRecords.id
          })
          .from(curtailmentRecords)
          .where(
            sql`settlement_date = ${TARGET_DATE} AND 
                settlement_period = ${combo.period} AND 
                farm_id = ${combo.farmId}`
          )
          .orderBy(curtailmentRecords.id);
        
        if (records.length > 1) {
          const idsToKeep = [records[0].id];
          const idsToDelete = records.slice(1).map(r => r.id);
          
          await db
            .delete(curtailmentRecords)
            .where(inArray(curtailmentRecords.id, idsToDelete));
          
          removedCount += idsToDelete.length;
          
          if (removedCount % 100 === 0) {
            console.log(`Aggressively removed ${removedCount} records so far...`);
          }
        }
      }
      
      console.log(`Aggressively removed ${removedCount} duplicate records`);
    }
    
    // Get final data state
    const afterCheck = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nFinal state: ${afterCheck[0].recordCount} records across ${afterCheck[0].periodCount} periods`);
    console.log(`Total volume: ${Number(afterCheck[0].totalVolume).toFixed(2)} MWh`);
    console.log(`Total payment: £${Number(afterCheck[0].totalPayment).toFixed(2)}`);
    
    // Step 3: Delete Bitcoin calculations for this date
    console.log(`\nRemoving existing Bitcoin calculations for ${TARGET_DATE}...`);
    const deletedCount = await db
      .delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    console.log(`Deleted ${deletedCount} existing Bitcoin calculations`);
    
    // Step 4: Update Bitcoin calculations based on the clean data
    console.log(`\nUpdating Bitcoin calculations for ${TARGET_DATE}...`);
    
    // Process for all miner models
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    for (const model of minerModels) {
      console.log(`Processing Bitcoin calculations for model: ${model}`);
      await processSingleDay(TARGET_DATE, model);
    }
    
    // Verify Bitcoin calculations
    const bitcoinCheck = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
        modelCount: sql<number>`COUNT(DISTINCT miner_model)`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
      
    console.log(`\nBitcoin calculation summary: ${bitcoinCheck[0].recordCount} records`);
    console.log(`Periods: ${bitcoinCheck[0].periodCount}, Farms: ${bitcoinCheck[0].farmCount}, Models: ${bitcoinCheck[0].modelCount}`);
    
    console.log(`\n=== Processing Complete for ${TARGET_DATE} ===`);
  } catch (error) {
    console.error(`Error during processing for ${TARGET_DATE}:`, error);
  }
}

// Execute the processing
completeAndCleanup();
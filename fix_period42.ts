/**
 * Fix Missing Curtailment Records for Period 42 on March 5, 2025
 * 
 * This script is specifically designed to fix missing records in period 42
 * for March 5, 2025, which appears to be incomplete in the database.
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { fetchBidsOffers, delay } from './server/services/elexon';
import { eq, and, sql } from 'drizzle-orm';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// Constants
const TARGET_DATE = '2025-03-05';
const TARGET_PERIOD = 42;
const BMU_MAPPING_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), 'server/data/bmuMapping.json');

let windFarmBmuIds: Set<string> | null = null;
let bmuLeadPartyMap: Map<string, string> | null = null;

// Utility functions
async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    if (windFarmBmuIds === null || bmuLeadPartyMap === null) {
      console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
      const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
      const bmuMapping = JSON.parse(mappingContent);
      console.log(`Loaded ${bmuMapping.length} wind farm BMU IDs`);

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

async function fixPeriod42() {
  try {
    console.log(`Processing period 42 for date ${TARGET_DATE}`);
    
    // Make sure wind farm IDs are loaded
    const validWindFarmIds = await loadWindFarmIds();
    
    // First, get existing records for period 42
    const existingRecords = await db.select({
      farmId: curtailmentRecords.farmId,
      volume: curtailmentRecords.volume
    })
    .from(curtailmentRecords)
    .where(
      and(
        sql`${curtailmentRecords.settlementDate}::text = ${TARGET_DATE}`,
        eq(curtailmentRecords.settlementPeriod, TARGET_PERIOD)
      )
    );
    
    console.log(`Found ${existingRecords.length} existing records for period ${TARGET_PERIOD}`);
    
    // Create a set of existing farm IDs
    const existingFarmIds = new Set(existingRecords.map(record => record.farmId));
    
    // Fetch data from Elexon API
    const bidsOffers = await fetchBidsOffers(TARGET_DATE, TARGET_PERIOD);
    
    if (!bidsOffers || bidsOffers.length === 0) {
      console.log(`No data returned from API for period ${TARGET_PERIOD}`);
      return { added: 0, skipped: 0, errors: 0 };
    }
    
    console.log(`Retrieved ${bidsOffers.length} records from API for period ${TARGET_PERIOD}`);
    
    // Filter to keep only curtailment records for wind farms (where volume is negative)
    const curtailments = bidsOffers.filter(offer => 
      offer.volume < 0 && 
      validWindFarmIds.has(offer.id)
    );
    
    console.log(`Found ${curtailments.length} wind farm curtailment records for period ${TARGET_PERIOD}`);
    
    let added = 0;
    let skipped = 0;
    let errors = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each curtailment record
    for (const curtailment of curtailments) {
      try {
        // Skip if this farm ID already exists for this period
        if (existingFarmIds.has(curtailment.id)) {
          console.log(`Skipping existing record for ${curtailment.id}`);
          skipped++;
          continue;
        }
        
        const volume = curtailment.volume;
        const payment = Math.abs(volume) * curtailment.originalPrice;
        totalVolume += Math.abs(volume);
        totalPayment += payment;
        
        // Insert new record
        await db.insert(curtailmentRecords).values({
          settlementDate: TARGET_DATE,
          settlementPeriod: TARGET_PERIOD,
          farmId: curtailment.id,
          leadPartyName: bmuLeadPartyMap?.get(curtailment.id) || 'Unknown',
          volume: volume.toString(),
          payment: payment.toString(),
          originalPrice: curtailment.originalPrice.toString(),
          finalPrice: curtailment.finalPrice.toString(),
          soFlag: curtailment.soFlag,
          cadlFlag: curtailment.cadlFlag === null ? false : curtailment.cadlFlag
        });
        
        console.log(`Added record for ${curtailment.id}: ${Math.abs(volume)} MWh, £${payment}`);
        added++;
      } catch (err) {
        console.error(`Error processing record for ${curtailment.id}:`, err);
        errors++;
      }
    }
    
    // If we've added records, update the daily summary
    if (added > 0) {
      console.log(`Added ${added} records: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
      
      // Update the daily summary to reflect the new total
      const newTotals = await db.select({
        totalVolume: sql<string>`SUM(volume::numeric)`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(sql`${curtailmentRecords.settlementDate}::text = ${TARGET_DATE}`);
      
      if (newTotals[0].totalVolume && newTotals[0].totalPayment) {
        await db.update(dailySummaries)
          .set({
            totalCurtailedEnergy: Math.abs(Number(newTotals[0].totalVolume)).toString(),
            totalPayment: newTotals[0].totalPayment
          })
          .where(sql`${dailySummaries.summaryDate}::text = ${TARGET_DATE}`);
          
        console.log(`Updated daily summary for ${TARGET_DATE}`);
      }
    }
    
    return { added, skipped, errors };
  } catch (err) {
    console.error(`Error processing period ${TARGET_PERIOD}:`, err);
    return { added: 0, skipped: 0, errors: 1 };
  }
}

async function main() {
  console.log(`🔄 Starting fix for period 42 on ${TARGET_DATE}`);
  
  // Get initial count for period 42
  const initialCount = await db.select({ count: sql<number>`count(*)` })
    .from(curtailmentRecords)
    .where(
      and(
        sql`${curtailmentRecords.settlementDate}::text = ${TARGET_DATE}`,
        eq(curtailmentRecords.settlementPeriod, TARGET_PERIOD)
      )
    );
  
  console.log(`Initial record count for period 42: ${initialCount[0].count}`);
  
  // Fix period 42
  const result = await fixPeriod42();
  
  // Get final count
  const finalCount = await db.select({ count: sql<number>`count(*)` })
    .from(curtailmentRecords)
    .where(
      and(
        sql`${curtailmentRecords.settlementDate}::text = ${TARGET_DATE}`,
        eq(curtailmentRecords.settlementPeriod, TARGET_PERIOD)
      )
    );
  
  console.log("\n===== Processing Summary =====");
  console.log(`Records added: ${result.added}`);
  console.log(`Records skipped (already exist): ${result.skipped}`);
  console.log(`Errors: ${result.errors}`);
  console.log(`Final record count for period 42: ${finalCount[0].count}`);
  
  // After data is fixed, we should also run the reconciliation
  console.log("\nNext steps:");
  console.log("1. Run the reconciliation for 2025-03-05 to ensure Bitcoin calculations are updated");
  console.log("   npx tsx server/services/historicalReconciliation.ts date 2025-03-05");
}

// Run the main function
main()
  .then(() => {
    console.log("Processing complete");
    process.exit(0);
  })
  .catch((err) => {
    console.error("Error during processing:", err);
    process.exit(1);
  });
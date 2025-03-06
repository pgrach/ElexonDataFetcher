/**
 * Batch Process Remaining High-Priority Periods for March 5, 2025
 * 
 * This script focuses on processing the next set of high-priority periods
 * to accelerate the data restoration process.
 */

import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { fetchBidsOffers, delay } from './server/services/elexon';
import { eq, and, sql } from 'drizzle-orm';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// Constants
const TARGET_DATE = '2025-03-05';
const PRIORITY_PERIODS = [48]; // Remaining periods to process
const DELAY_BETWEEN_PERIODS = 50; // 0.05 second delay between API calls
const BMU_MAPPING_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), 'server/data/bmuMapping.json');

let windFarmBmuIds: Set<string> | null = null;
let bmuLeadPartyMap: Map<string, string> | null = null;

// Load wind farm BMU IDs
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

    return windFarmBmuIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

// Process a single period
async function processPeriod(period: number): Promise<{
  processed: number;
  added: number;
  totalVolume: number;
  totalPayment: number;
}> {
  try {
    console.log(`Processing period ${period} for date ${TARGET_DATE}`);
    
    // Check if this period has already been processed
    const existingRecords = await db.select({ count: sql<number>`count(*)` })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    if (existingRecords[0].count > 0) {
      console.log(`Period ${period} already has ${existingRecords[0].count} records. Skipping.`);
      return { processed: 0, added: 0, totalVolume: 0, totalPayment: 0 };
    }
    
    // Load wind farm IDs
    const validWindFarmIds = await loadWindFarmIds();
    
    // Fetch data from Elexon API
    const bidsOffers = await fetchBidsOffers(TARGET_DATE, period);
    
    if (!bidsOffers || bidsOffers.length === 0) {
      console.log(`No data returned for period ${period}`);
      return { processed: 0, added: 0, totalVolume: 0, totalPayment: 0 };
    }
    
    console.log(`Retrieved ${bidsOffers.length} records for period ${period}`);
    
    // Filter to keep only curtailment records for wind farms (where volume is negative)
    const curtailments = bidsOffers.filter(offer => 
      offer.volume < 0 && 
      validWindFarmIds.has(offer.id)
    );
    
    console.log(`Found ${curtailments.length} wind farm curtailment records for period ${period}`);
    
    let added = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each curtailment record
    for (const curtailment of curtailments) {
      try {
        const volume = curtailment.volume;
        const payment = Math.abs(volume) * curtailment.finalPrice; // Using finalPrice for payment
        totalVolume += Math.abs(volume);
        totalPayment += payment;
        
        // Insert new record
        await db.insert(curtailmentRecords).values([{
          settlementDate: TARGET_DATE,
          settlementPeriod: period,
          farmId: curtailment.id,
          leadPartyName: bmuLeadPartyMap?.get(curtailment.id) || 'Unknown',
          volume: volume.toString(),
          payment: payment.toString(),
          originalPrice: curtailment.originalPrice.toString(),
          finalPrice: curtailment.finalPrice.toString(),
          soFlag: curtailment.soFlag,
          cadlFlag: curtailment.cadlFlag === null ? false : curtailment.cadlFlag
        }]);
        
        added++;
      } catch (err) {
        console.error(`Error processing record for period ${period}:`, err);
      }
    }
    
    if (added > 0) {
      console.log(`[${TARGET_DATE} P${period}] Added ${added} records: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    }
    
    return { processed: curtailments.length, added, totalVolume, totalPayment };
  } catch (err) {
    console.error(`Error processing period ${period}:`, err);
    return { processed: 0, added: 0, totalVolume: 0, totalPayment: 0 };
  }
}

// Main function
async function main() {
  console.log(`🔄 Starting batch processing for periods ${PRIORITY_PERIODS.join(', ')} of ${TARGET_DATE}`);
  
  // Get current database stats
  const initialStats = await db.select({
    recordCount: sql<number>`count(*)`,
    periodCount: sql<number>`count(distinct settlement_period)`,
    totalVolume: sql<number>`sum(abs(volume::numeric))`,
    totalPayment: sql<number>`sum(payment::numeric)`
  })
  .from(curtailmentRecords)
  .where(sql`${curtailmentRecords.settlementDate}::text = ${TARGET_DATE}`);
  
  console.log(`Current database stats: ${initialStats[0].recordCount} records across ${initialStats[0].periodCount} periods`);
  console.log(`Total volume: ${initialStats[0].totalVolume} MWh, Payment: £${initialStats[0].totalPayment}`);
  
  // Process priority periods
  console.log(`\nProcessing batch of priority periods: ${PRIORITY_PERIODS.join(', ')}`);
  const results = {
    totalProcessed: 0,
    totalAdded: 0,
    totalVolume: 0,
    totalPayment: 0
  };
  
  for (const period of PRIORITY_PERIODS) {
    try {
      const result = await processPeriod(period);
      
      // Aggregate results
      results.totalProcessed += result.processed;
      results.totalAdded += result.added;
      results.totalVolume += result.totalVolume;
      results.totalPayment += result.totalPayment;
      
      console.log(`Completed period ${period}`);
      await delay(DELAY_BETWEEN_PERIODS);
    } catch (error) {
      console.error(`Failed to process period ${period}:`, error);
    }
  }
  
  // Get final database stats
  const finalStats = await db.select({
    recordCount: sql<number>`count(*)`,
    periodCount: sql<number>`count(distinct settlement_period)`,
    totalVolume: sql<number>`sum(abs(volume::numeric))`,
    totalPayment: sql<number>`sum(payment::numeric)`
  })
  .from(curtailmentRecords)
  .where(sql`${curtailmentRecords.settlementDate}::text = ${TARGET_DATE}`);
  
  console.log(`\n===== Processing Summary =====`);
  console.log(`Total records added: ${results.totalAdded}`);
  console.log(`Total volume added: ${results.totalVolume.toFixed(2)} MWh`);
  console.log(`Total payment added: £${results.totalPayment.toFixed(2)}`);
  
  console.log(`\n===== Final Database State =====`);
  console.log(`Total records: ${finalStats[0].recordCount}`);
  console.log(`Total periods: ${finalStats[0].periodCount}`);
  console.log(`Total volume: ${finalStats[0].totalVolume} MWh`);
  console.log(`Total payment: £${finalStats[0].totalPayment}`);
  
  // Calculate progress percentage
  const TARGET_VOLUME = 105247.85; // MWh from the Elexon API
  const currentVolume = Number(finalStats[0].totalVolume || 0);
  const percentComplete = (currentVolume / TARGET_VOLUME) * 100;
  
  console.log(`\n===== Progress =====`);
  console.log(`Target volume: ${TARGET_VOLUME.toFixed(2)} MWh`);
  console.log(`Current volume: ${currentVolume.toFixed(2)} MWh`);
  console.log(`Completion: ${percentComplete.toFixed(2)}%`);
  
  // Provide next steps based on progress
  console.log(`\n===== Next Steps =====`);
  if (percentComplete >= 90) {
    console.log(`✅ Successfully processed over 90% of the target volume.`);
    console.log(`Run the Bitcoin reconciliation script: npx tsx reconcile_march5_2025.ts`);
  } else if (percentComplete >= 75) {
    console.log(`🟡 Good progress! ${percentComplete.toFixed(2)}% of target volume processed.`);
    console.log(`Process more periods or run the Bitcoin reconciliation: npx tsx reconcile_march5_2025.ts`);
  } else {
    console.log(`🟠 Progress: ${percentComplete.toFixed(2)}% of target volume. More processing needed.`);
    console.log(`Run the next batch with updated PRIORITY_PERIODS in this script.`);
  }
}

// Run the main function
main()
  .then(() => {
    console.log("Batch processing complete");
    process.exit(0);
  })
  .catch((err) => {
    console.error("Error during batch processing:", err);
    process.exit(1);
  });
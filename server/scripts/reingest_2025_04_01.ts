/**
 * Script for reingesting all settlement period data for April 1, 2025
 * 
 * This script handles the complete reingestion process:
 * 1. Clears all existing data for April 1, 2025
 * 2. Fetches fresh data from the Elexon API for all 48 settlement periods
 * 3. Updates the daily summary for April 1, 2025
 * 4. Recalculates the monthly summary for April 2025
 * 5. Updates the yearly summary for 2025
 * 6. Regenerates all Bitcoin mining calculations for that date
 * 7. Verifies that the data is complete after processing
 */

import { db } from "@db";
import {
  curtailmentRecords,
  dailySummaries,
  monthlySummaries,
  yearlySummaries,
  bitcoinDailySummaries
} from "@db/schema";
import { eq, and, sql } from "drizzle-orm";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';
import { fetchBidsOffers } from "../services/elexon";

// Constants
const TARGET_DATE = '2025-04-01';
const TARGET_MONTH = '2025-04';
const TARGET_YEAR = '2025';
const BMU_MAPPING_PATH = path.join(process.cwd(), "data/bmu_mapping.json");

// Variables to store wind farm IDs and lead party information
let windFarmBmuIds: Set<string> | null = null;
let bmuLeadPartyMap: Map<string, string> | null = null;

/**
 * Load wind farm BMU IDs from the mapping file
 */
async function loadWindFarmIds(): Promise<Set<string>> {
  try {
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
    return windFarmBmuIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

/**
 * Process curtailment data for the target date
 */
async function processCurtailmentData(): Promise<{
  recordsProcessed: number;
  totalVolume: number;
  totalPayment: number;
}> {
  const BATCH_SIZE = 12;
  const validWindFarmIds = await loadWindFarmIds();
  let totalVolume = 0;
  let totalPayment = 0;
  let recordsProcessed = 0;

  console.log(`\n=== Processing curtailment for ${TARGET_DATE} ===`);

  // Clear existing records for the date to prevent partial updates
  console.log(`Clearing existing records for ${TARGET_DATE}...`);
  await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));

  // Also clear Bitcoin calculations for the target date
  console.log(`Clearing existing Bitcoin calculations for ${TARGET_DATE}...`);
  await db.delete(bitcoinDailySummaries)
    .where(eq(bitcoinDailySummaries.date, TARGET_DATE));

  // Process all 48 periods in batches
  for (let startPeriod = 1; startPeriod <= 48; startPeriod += BATCH_SIZE) {
    const endPeriod = Math.min(startPeriod + BATCH_SIZE - 1, 48);
    console.log(`Processing periods ${startPeriod}-${endPeriod}...`);
    
    const periodPromises = [];

    for (let period = startPeriod; period <= endPeriod; period++) {
      periodPromises.push((async () => {
        try {
          const records = await fetchBidsOffers(TARGET_DATE, period);
          const validRecords = records.filter(record =>
            record.volume < 0 &&
            (record.soFlag || record.cadlFlag) &&
            validWindFarmIds.has(record.id)
          );

          if (validRecords.length > 0) {
            console.log(`[${TARGET_DATE} P${period}] Processing ${validRecords.length} records`);
          }

          const periodTotal = { volume: 0, payment: 0 };

          for (const record of validRecords) {
            const bmuId = record.id;
            const leadParty = bmuLeadPartyMap?.get(bmuId) || 'Unknown';
            const absVolume = Math.abs(record.volume);
            
            // Insert record with standardized payment calculation
            await db.insert(curtailmentRecords).values({
              id: `${TARGET_DATE}_${period}_${bmuId}_${Math.random().toString(36).substring(2, 15)}`,
              settlementDate: TARGET_DATE,
              settlementPeriod: period,
              leadParty: leadParty,
              bmuId: bmuId,
              volume: record.volume.toString(),
              payment: (record.price * absVolume).toString(),
              price: record.price.toString(),
              flagBool: record.soFlag ? "SO" : "CADL",
              timeStamp: new Date().toISOString()
            });

            periodTotal.volume += absVolume;
            periodTotal.payment += record.price * absVolume;
            recordsProcessed++;
          }

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

  return { recordsProcessed, totalVolume, totalPayment };
}

/**
 * Update daily, monthly, and yearly summaries
 */
async function updateSummaries(): Promise<void> {
  console.log(`\n=== Updating summary tables for ${TARGET_DATE} ===`);
  
  // Update daily summary
  console.log(`Updating daily summary for ${TARGET_DATE}...`);
  const dailyStats = await db
    .select({
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));

  if (dailyStats.length > 0 && dailyStats[0].totalVolume !== null) {
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.date, TARGET_DATE));
      
    await db.insert(dailySummaries).values({
      id: `${TARGET_DATE}_summary`,
      date: TARGET_DATE,
      energy: dailyStats[0].totalVolume,
      payment: dailyStats[0].totalPayment,
      timeStamp: new Date().toISOString()
    });
    
    console.log(`Daily summary updated: ${dailyStats[0].totalVolume} MWh, £${dailyStats[0].totalPayment}`);
  } else {
    console.log(`No data found for ${TARGET_DATE}, creating empty daily summary`);
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.date, TARGET_DATE));
      
    await db.insert(dailySummaries).values({
      id: `${TARGET_DATE}_summary`,
      date: TARGET_DATE,
      energy: '0',
      payment: '0',
      timeStamp: new Date().toISOString()
    });
  }
  
  // Update monthly summary
  console.log(`Updating monthly summary for ${TARGET_MONTH}...`);
  const monthlyStats = await db
    .select({
      totalVolume: sql<string>`SUM(energy::numeric)`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(dailySummaries)
    .where(sql`date LIKE ${TARGET_MONTH + '%'}`);
    
  if (monthlyStats.length > 0 && monthlyStats[0].totalVolume !== null) {
    await db.delete(monthlySummaries)
      .where(eq(monthlySummaries.month, TARGET_MONTH));
      
    await db.insert(monthlySummaries).values({
      id: `${TARGET_MONTH}_summary`,
      month: TARGET_MONTH,
      energy: monthlyStats[0].totalVolume,
      payment: monthlyStats[0].totalPayment,
      timeStamp: new Date().toISOString()
    });
    
    console.log(`Monthly summary updated: ${monthlyStats[0].totalVolume} MWh, £${monthlyStats[0].totalPayment}`);
  }
  
  // Update yearly summary
  console.log(`Updating yearly summary for ${TARGET_YEAR}...`);
  const yearlyStats = await db
    .select({
      totalVolume: sql<string>`SUM(energy::numeric)`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(monthlySummaries)
    .where(sql`month LIKE ${TARGET_YEAR + '%'}`);
    
  if (yearlyStats.length > 0 && yearlyStats[0].totalVolume !== null) {
    await db.delete(yearlySummaries)
      .where(eq(yearlySummaries.year, TARGET_YEAR));
      
    await db.insert(yearlySummaries).values({
      id: `${TARGET_YEAR}_summary`,
      year: TARGET_YEAR,
      energy: yearlyStats[0].totalVolume,
      payment: yearlyStats[0].totalPayment,
      timeStamp: new Date().toISOString()
    });
    
    console.log(`Yearly summary updated: ${yearlyStats[0].totalVolume} MWh, £${yearlyStats[0].totalPayment}`);
  }
}

/**
 * Update Bitcoin mining calculations for the target date
 */
async function updateBitcoinCalculations(): Promise<void> {
  console.log(`\n=== Updating Bitcoin calculations for ${TARGET_DATE} ===`);
  
  try {
    // Import Bitcoin service dynamically to avoid circular dependencies
    const { processSingleDay } = await import("../services/bitcoinService");

    // Define miner models to process
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    // Process each miner model
    for (const minerModel of minerModels) {
      console.log(`Processing ${minerModel} calculations...`);
      await processSingleDay(TARGET_DATE, minerModel);
    }
    
    console.log(`Bitcoin calculations updated for ${TARGET_DATE}`);
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    throw error;
  }
}

/**
 * Verify data integrity after processing
 */
async function verifyDataIntegrity(): Promise<{
  recordCount: number;
  periodCount: number;
  totalVolume: string;
  totalPayment: string;
  dailySummary: { energy: string; payment: string; };
  bitcoinCalculations: { minerModel: string; bitcoinMined: string; }[];
}> {
  console.log(`\n=== Verifying data integrity for ${TARGET_DATE} ===`);
  
  // Check curtailment records
  const records = await db
    .select({
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
  console.log(`Records Check: ${records[0].recordCount} records in ${records[0].periodCount} periods`);
  console.log(`Volume: ${parseFloat(records[0].totalVolume || '0').toFixed(2)} MWh, Payment: £${parseFloat(records[0].totalPayment || '0').toFixed(2)}`);
  
  // Check daily summary
  const summary = await db
    .select({
      energy: sql<string>`energy`,
      payment: sql<string>`payment`
    })
    .from(dailySummaries)
    .where(eq(dailySummaries.date, TARGET_DATE));
  
  console.log(`Daily Summary: Energy: ${parseFloat(summary[0]?.energy || '0').toFixed(2)} MWh, Payment: £${parseFloat(summary[0]?.payment || '0').toFixed(2)}`);
  
  // Check Bitcoin calculations
  const bitcoinResults = [];
  const minerModels = ['S19J_PRO', 'S9', 'M20S'];
  
  for (const minerModel of minerModels) {
    const btcCalc = await db
      .select({
        bitcoinMined: sql<string>`bitcoin_mined`
      })
      .from(bitcoinDailySummaries)
      .where(
        and(
          eq(bitcoinDailySummaries.date, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        )
      );
      
    if (btcCalc.length > 0) {
      console.log(`${minerModel} Bitcoin Mined: ${parseFloat(btcCalc[0]?.bitcoinMined || '0').toFixed(8)} BTC`);
      bitcoinResults.push({
        minerModel,
        bitcoinMined: btcCalc[0]?.bitcoinMined || '0'
      });
    } else {
      console.log(`${minerModel}: No Bitcoin calculation found`);
      bitcoinResults.push({
        minerModel,
        bitcoinMined: '0'
      });
    }
  }
  
  return {
    recordCount: records[0].recordCount,
    periodCount: records[0].periodCount,
    totalVolume: records[0].totalVolume || '0',
    totalPayment: records[0].totalPayment || '0',
    dailySummary: {
      energy: summary[0]?.energy || '0',
      payment: summary[0]?.payment || '0'
    },
    bitcoinCalculations: bitcoinResults
  };
}

/**
 * Main function to run the entire reingestion process
 */
async function runReingestionProcess(): Promise<void> {
  const startTime = Date.now();
  
  console.log('\n============================================');
  console.log(`STARTING COMPLETE REINGESTION FOR ${TARGET_DATE}`);
  console.log('============================================\n');
  
  try {
    // Step 1: Process curtailment data
    const { recordsProcessed, totalVolume, totalPayment } = await processCurtailmentData();
    
    // Step 2: Update summary tables
    await updateSummaries();
    
    // Step 3: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 4: Verify data integrity
    const verificationResults = await verifyDataIntegrity();
    
    // Print summary
    console.log('\n=== Reingestion Summary ===');
    console.log(`Records processed: ${recordsProcessed}`);
    console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);
    console.log(`Settlement periods covered: ${verificationResults.periodCount}/48`);
    
    const endTime = Date.now();
    console.log('\n============================================');
    console.log('REINGESTION COMPLETED SUCCESSFULLY');
    console.log(`Duration: ${((endTime - startTime) / 1000).toFixed(2)} seconds`);
    console.log('============================================\n');
    
    // Detail the verification results
    console.log('=== Verification Results ===');
    console.log(`Curtailment Records: ${verificationResults.recordCount}`);
    console.log(`Periods Covered: ${verificationResults.periodCount}/48`);
    console.log(`Total Energy: ${parseFloat(verificationResults.totalVolume).toFixed(2)} MWh`);
    console.log(`Total Payment: £${parseFloat(verificationResults.totalPayment).toFixed(2)}`);
    console.log(`Daily Summary: Energy=${parseFloat(verificationResults.dailySummary.energy).toFixed(2)} MWh, Payment=£${parseFloat(verificationResults.dailySummary.payment).toFixed(2)}`);
    
    console.log('\nBitcoin Mining Calculations:');
    for (const calc of verificationResults.bitcoinCalculations) {
      console.log(`${calc.minerModel}: ${parseFloat(calc.bitcoinMined).toFixed(8)} BTC`);
    }
    
  } catch (error) {
    console.error('\nREINGESTION FAILED:', error);
    process.exit(1);
  }
}

// Run the reingestion process (ESM compatible)
runReingestionProcess()
  .then(() => process.exit(0))
  .catch(error => {
    console.error('Unhandled error during reingestion:', error);
    process.exit(1);
  });
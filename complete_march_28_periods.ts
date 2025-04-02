/**
 * Complete March 28, 2025 Settlement Periods
 * 
 * This script ensures that all 48 settlement periods for March 28, 2025 
 * are processed and stored in the database. It specifically targets
 * any missing periods (17-48), ensures they are fetched from the Elexon API,
 * and updates all relevant summary tables.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, and, sql, not, inArray } from "drizzle-orm";
import { fetchBidsOffers } from "./server/services/elexon";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';
import { format } from 'date-fns';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TARGET_DATE = '2025-03-28';
// Set these to control which periods to process (for batch processing)
// Process just a few periods at a time
const START_PERIOD = 41; 
const END_PERIOD = 48;
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");

// Utility function to delay between API calls
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
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

// Get missing settlement periods
async function getMissingPeriods(): Promise<number[]> {
  console.log(`Checking for missing settlement periods on ${TARGET_DATE}...`);
  
  // Get all existing periods
  const existingPeriods = await db
    .select({ period: curtailmentRecords.settlementPeriod })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .groupBy(curtailmentRecords.settlementPeriod);
  
  const existingPeriodNumbers = existingPeriods.map(r => r.period);
  console.log(`Found ${existingPeriodNumbers.length} existing periods: ${existingPeriodNumbers.join(', ')}`);
  
  // Determine which periods are missing (within our target range)
  const targetPeriods = Array.from(
    { length: END_PERIOD - START_PERIOD + 1 }, 
    (_, i) => START_PERIOD + i
  );
  
  const missingPeriods = targetPeriods.filter(p => !existingPeriodNumbers.includes(p));
  
  console.log(`Missing ${missingPeriods.length} periods in range ${START_PERIOD}-${END_PERIOD}: ${missingPeriods.join(', ')}`);
  return missingPeriods;
}

// Process a single period
async function processPeriod(period: number, windFarmIds: Set<string>, bmuLeadPartyMap: Map<string, string>): Promise<{
  volume: number;
  payment: number;
}> {
  console.log(`Processing period ${period} for ${TARGET_DATE}...`);
  
  try {
    const records = await fetchBidsOffers(TARGET_DATE, period);
    const validRecords = records.filter(record =>
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      windFarmIds.has(record.id)
    );
    
    if (validRecords.length > 0) {
      console.log(`[${TARGET_DATE} P${period}] Processing ${validRecords.length} records`);
    } else {
      console.log(`[${TARGET_DATE} P${period}] No valid curtailment records found`);
    }
    
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
          
          console.log(`[${TARGET_DATE} P${period}] Added record for ${record.id}: ${volume} MWh, £${payment}`);
          return { volume, payment };
        } catch (error) {
          console.error(`[${TARGET_DATE} P${period}] Error inserting record for ${record.id}:`, error);
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
      console.log(`[${TARGET_DATE} P${period}] Total: ${periodTotal.volume.toFixed(2)} MWh, £${periodTotal.payment.toFixed(2)}`);
    }
    
    return periodTotal;
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return { volume: 0, payment: 0 };
  }
}

// Update all summary tables
async function updateSummaries(): Promise<void> {
  try {
    console.log(`Updating summary records for ${TARGET_DATE}...`);
    
    // Calculate totals from curtailment records
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (!totals[0] || !totals[0].totalCurtailedEnergy) {
      console.error('Error: No curtailment records found to create summary');
      return;
    }
    
    // Update daily summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totals[0].totalCurtailedEnergy,
      totalPayment: totals[0].totalPayment,
      totalWindGeneration: '0',
      windOnshoreGeneration: '0',
      windOffshoreGeneration: '0',
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totals[0].totalCurtailedEnergy,
        totalPayment: totals[0].totalPayment,
        lastUpdated: new Date()
      }
    });
    
    console.log(`Daily summary updated for ${TARGET_DATE}:`);
    console.log(`- Energy: ${totals[0].totalCurtailedEnergy} MWh`);
    console.log(`- Payment: £${totals[0].totalPayment}`);
    
    // Update monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7);
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${yearMonth + '-01'}::date)`);
    
    if (monthlyTotals[0].totalCurtailedEnergy && monthlyTotals[0].totalPayment) {
      await db.insert(monthlySummaries).values({
        yearMonth,
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
        totalPayment: monthlyTotals[0].totalPayment,
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [monthlySummaries.yearMonth],
        set: {
          totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
          totalPayment: monthlyTotals[0].totalPayment,
          updatedAt: new Date()
        }
      });
      
      console.log(`Monthly summary updated for ${yearMonth}:`);
      console.log(`- Energy: ${monthlyTotals[0].totalCurtailedEnergy} MWh`);
      console.log(`- Payment: £${monthlyTotals[0].totalPayment}`);
    }
    
    // Update yearly summary
    const year = TARGET_DATE.substring(0, 4);
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${year + '-01-01'}::date)`);
    
    if (yearlyTotals[0].totalCurtailedEnergy && yearlyTotals[0].totalPayment) {
      await db.insert(yearlySummaries).values({
        year,
        totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
        totalPayment: yearlyTotals[0].totalPayment,
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [yearlySummaries.year],
        set: {
          totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
          totalPayment: yearlyTotals[0].totalPayment,
          updatedAt: new Date()
        }
      });
      
      console.log(`Yearly summary updated for ${year}:`);
      console.log(`- Energy: ${yearlyTotals[0].totalCurtailedEnergy} MWh`);
      console.log(`- Payment: £${yearlyTotals[0].totalPayment}`);
    }
  } catch (error) {
    console.error('Error updating summaries:', error);
    throw error;
  }
}

// Update Bitcoin calculations for the date
async function updateBitcoinCalculations(): Promise<void> {
  console.log(`Updating Bitcoin calculations for ${TARGET_DATE}...`);
  
  try {
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    for (const minerModel of minerModels) {
      await processSingleDay(TARGET_DATE, minerModel);
      console.log(`- Processed ${minerModel}`);
    }
    
    console.log('Bitcoin calculations updated successfully');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    throw error;
  }
}

// Process a batch of periods
async function processBatch(
  periods: number[],
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  volume: number;
  payment: number;
}> {
  let totalVolume = 0;
  let totalPayment = 0;
  
  for (const period of periods) {
    const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
    totalVolume += result.volume;
    totalPayment += result.payment;
    await delay(500); // Add delay between API calls to avoid rate limits
  }
  
  return { volume: totalVolume, payment: totalPayment };
}

// Main function
async function main(): Promise<void> {
  console.log(`=== Completing March 28, 2025 Settlement Periods ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    // Step 1: Get missing periods
    const missingPeriods = await getMissingPeriods();
    
    if (missingPeriods.length === 0) {
      console.log('No missing periods found. All 48 settlement periods are already processed.');
    } else {
      // Step 2: Load BMU mappings
      const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
      
      // Step 3: Process periods in batches of 5
      const BATCH_SIZE = 5;
      for (let i = 0; i < missingPeriods.length; i += BATCH_SIZE) {
        const batch = missingPeriods.slice(i, i + BATCH_SIZE);
        console.log(`Processing batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(missingPeriods.length / BATCH_SIZE)}: Periods ${batch.join(', ')}`);
        
        await processBatch(batch, windFarmIds, bmuLeadPartyMap);
        console.log(`Batch ${Math.floor(i / BATCH_SIZE) + 1} completed`);
        
        // Refresh database connection to avoid timeouts
        await db.execute(sql`SELECT 1`);
      }
      
      // Step 4: Update all summary tables
      await updateSummaries();
      
      // Step 5: Update Bitcoin calculations
      await updateBitcoinCalculations();
    }
    
    // Step 6: Verify periods
    const finalPeriods = await db
      .select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
      
    console.log(`\nFinal period count: ${finalPeriods.length} of 48`);
    console.log(`Periods: ${finalPeriods.map(r => r.period).join(', ')}`);
    
    console.log(`\nUpdate completed successfully at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Error during update process:', error);
    process.exit(1);
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
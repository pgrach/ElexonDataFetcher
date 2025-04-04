/**
 * Complete Reingest for March 21, 2025
 * 
 * This script completely removes all settlement period data for March 21, 2025
 * and then reingests all 48 settlement periods from the Elexon API.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import { fetchBidsOffers } from "./server/services/elexon";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TARGET_DATE = '2025-03-21';
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

// First clear the existing data for the target date
async function clearExistingData(): Promise<void> {
  console.log(`Clearing all existing data for ${TARGET_DATE}...`);
  
  // First, delete from curtailment_records
  const deleteResult = await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  console.log(`Deleted ${deleteResult.rowCount} curtailment records for ${TARGET_DATE}`);
  
  // Then delete from daily_summaries
  const summaryDeleteResult = await db.delete(dailySummaries)
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
  
  console.log(`Deleted ${summaryDeleteResult.rowCount} daily summary records for ${TARGET_DATE}`);
  
  return;
}

// Process a single settlement period
async function processPeriod(
  period: number, 
  windFarmIds: Set<string>, 
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  volume: number;
  payment: number;
  recordCount: number;
}> {
  console.log(`Processing period ${period} for ${TARGET_DATE}...`);
  
  try {
    // Get data from Elexon
    const records = await fetchBidsOffers(TARGET_DATE, period);
    const validRecords = records.filter(record =>
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      windFarmIds.has(record.id)
    );
    
    console.log(`[${TARGET_DATE} P${period}] Records: ${validRecords.length} (${validRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0).toFixed(2)} MWh, £${validRecords.reduce((sum, r) => sum + Math.abs(r.volume) * r.originalPrice, 0).toFixed(2)})`);
    
    if (validRecords.length > 0) {
      console.log(`[${TARGET_DATE} P${period}] Processing ${validRecords.length} records`);
    } else {
      console.log(`[${TARGET_DATE} P${period}] No valid curtailment records found`);
      return { volume: 0, payment: 0, recordCount: 0 };
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
    
    console.log(`[${TARGET_DATE} P${period}] Total: ${periodTotal.volume.toFixed(2)} MWh, £${periodTotal.payment.toFixed(2)}`);
    
    return { 
      volume: periodTotal.volume, 
      payment: periodTotal.payment,
      recordCount: validRecords.length
    };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return { volume: 0, payment: 0, recordCount: 0 };
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
  recordCount: number;
}> {
  let totalVolume = 0;
  let totalPayment = 0;
  let totalRecordCount = 0;
  
  for (const period of periods) {
    const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
    totalVolume += result.volume;
    totalPayment += result.payment;
    totalRecordCount += result.recordCount;
    
    // Add delay between periods to avoid API rate limits
    await delay(500); 
  }
  
  return { 
    volume: totalVolume, 
    payment: totalPayment,
    recordCount: totalRecordCount
  };
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

// Update Bitcoin calculations
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

// Main function
async function main(): Promise<void> {
  console.log(`=== Complete Reingest for March 21, 2025 ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  console.log(`Target date: ${TARGET_DATE}`);
  
  try {
    // Step 1: Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Step 2: Clear existing data
    await clearExistingData();
    
    // Step 3: Process all 48 periods
    console.log('Processing all 48 settlement periods...');
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    
    // Group periods into batches to avoid timeouts
    const BATCH_SIZE = 4;
    const batches = [];
    for (let i = 0; i < allPeriods.length; i += BATCH_SIZE) {
      batches.push(allPeriods.slice(i, i + BATCH_SIZE));
    }
    
    console.log(`Split into ${batches.length} batches of ${BATCH_SIZE} periods each`);
    
    let totalVolume = 0;
    let totalPayment = 0;
    let totalRecords = 0;
    
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      console.log(`Processing batch ${i+1}/${batches.length}: Periods ${batch.join(', ')}`);
      
      const batchResult = await processBatch(batch, windFarmIds, bmuLeadPartyMap);
      totalVolume += batchResult.volume;
      totalPayment += batchResult.payment;
      totalRecords += batchResult.recordCount;
      
      console.log(`Batch ${i+1} completed: ${batchResult.recordCount} records, ${batchResult.volume.toFixed(2)} MWh, £${batchResult.payment.toFixed(2)}`);
      console.log(`Running total: ${totalRecords} records, ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
      
      // Make sure to refresh the DB connection to avoid timeouts
      await db.execute(sql`SELECT 1`);
      
      // Delay between batches
      if (i < batches.length - 1) {
        const delayTime = 1000;
        console.log(`Waiting ${delayTime/1000} second before next batch...`);
        await delay(delayTime);
      }
    }
    
    // Step 4: Update all summary tables
    await updateSummaries();
    
    // Step 5: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 6: Verify the final state
    const finalStatus = await db
      .select({
        periodCount: sql`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        recordCount: sql`COUNT(*)`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nFinal Status for ${TARGET_DATE}:`);
    console.log(`- Settlement Periods: ${finalStatus[0].periodCount}/48`);
    console.log(`- Records: ${finalStatus[0].recordCount}`);
    console.log(`- Total Volume: ${finalStatus[0].totalVolume} MWh`);
    console.log(`- Total Payment: £${finalStatus[0].totalPayment}`);
    
    // Check if we have all 48 periods
    if (Number(finalStatus[0].periodCount) === 48) {
      console.log('SUCCESS: All 48 settlement periods are now in the database!');
    } else {
      console.log(`WARNING: Expected 48 periods, but found ${finalStatus[0].periodCount}`);
      
      // List the missing periods
      const existingPeriods = await db
        .select({ period: curtailmentRecords.settlementPeriod })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
        .groupBy(curtailmentRecords.settlementPeriod);
        
      const existingPeriodNumbers = existingPeriods.map(r => r.period);
      const missingPeriods = allPeriods.filter(p => !existingPeriodNumbers.includes(p));
      
      console.log(`Missing ${missingPeriods.length} periods: ${missingPeriods.join(', ')}`);
    }
    
    console.log(`\nReingest completed successfully at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Error during reingest process:', error);
    process.exit(1);
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
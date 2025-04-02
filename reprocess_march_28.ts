/**
 * Reprocess March 28 Data
 * 
 * This script reprocesses all settlement periods for March 28, 2025
 * using the updated Elexon service that includes both soFlag and cadlFlag records.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TARGET_DATE = '2025-03-28';
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

// Step 1: Clear existing data for March 28 to avoid duplicates
async function clearExistingData(): Promise<void> {
  console.log(`\nStep 1: Clearing existing data for ${TARGET_DATE}...`);
  
  try {
    // Clear curtailment records
    const deleteResult = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Cleared curtailment records for ${TARGET_DATE}`);
    
    // Clear daily summary
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`Cleared daily summary for ${TARGET_DATE}`);
    
    // Also clear Bitcoin calculations
    try {
      const { historicalBitcoinCalculations } = await import('./db/schema');
      await db.delete(historicalBitcoinCalculations)
        .where(eq(historicalBitcoinCalculations.calculationDate, TARGET_DATE));
      console.log(`Cleared Bitcoin calculations for ${TARGET_DATE}`);
    } catch (error) {
      console.warn('Note: Unable to clear Bitcoin calculations. This is not critical and will be handled later.');
    }
  } catch (error) {
    console.error('Error clearing existing data:', error);
    throw error;
  }
}

// Fetch data from Elexon API for a specific period
async function fetchElexonData(period: number): Promise<any[]> {
  try {
    console.log(`Fetching data for period ${period}...`);
    
    // Make parallel requests for bids and offers
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${TARGET_DATE}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      }),
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${TARGET_DATE}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      })
    ]).catch(error => {
      console.error(`Error fetching data for period ${period}:`, error.message);
      return [{ data: { data: [] } }, { data: { data: [] } }];
    });
    
    // Combine and return the raw data
    return [
      ...(bidsResponse.data?.data || []), 
      ...(offersResponse.data?.data || [])
    ];
  } catch (error) {
    console.error(`Error fetching data for period ${period}:`, error);
    return [];
  }
}

// Process data for a specific period
async function processPeriod(
  period: number,
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  recordCount: number;
  totalVolume: number;
  totalPayment: number;
}> {
  try {
    // Fetch raw data from Elexon
    const rawRecords = await fetchElexonData(period);
    console.log(`Retrieved ${rawRecords.length} raw records for period ${period}`);
    
    // Filter for valid wind farm records with curtailment (negative volume)
    // Include both soFlag and cadlFlag records
    const validRecords = rawRecords.filter(record => 
      record.volume < 0 && 
      (record.soFlag || record.cadlFlag) && 
      windFarmIds.has(record.id)
    );
    
    console.log(`Found ${validRecords.length} valid curtailment records for period ${period}`);
    
    // Insert the records
    let recordCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const record of validRecords) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice * -1;
      
      try {
        await db.insert(curtailmentRecords).values({
          settlementDate: TARGET_DATE,
          settlementPeriod: period,
          farmId: record.id,
          leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
          volume: record.volume.toString(), // Keep original negative value
          payment: payment.toString(),
          originalPrice: record.originalPrice.toString(),
          finalPrice: record.finalPrice.toString(),
          soFlag: record.soFlag,
          cadlFlag: record.cadlFlag
        });
        
        recordCount++;
        totalVolume += volume;
        totalPayment += payment;
      } catch (error) {
        console.error(`Error inserting record for ${record.id}:`, error);
      }
    }
    
    console.log(`Processed ${recordCount} records for period ${period} (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
    
    return {
      recordCount,
      totalVolume,
      totalPayment
    };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return {
      recordCount: 0,
      totalVolume: 0,
      totalPayment: 0
    };
  }
}

// Step 2: Process all periods
async function processAllPeriods(): Promise<{
  totalRecords: number;
  totalVolume: number;
  totalPayment: number;
}> {
  console.log(`\nStep 2: Processing all 48 periods for ${TARGET_DATE}...`);
  
  try {
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each period one by one
    for (let period = 1; period <= 48; period++) {
      const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
      
      totalRecords += result.recordCount;
      totalVolume += result.totalVolume;
      totalPayment += result.totalPayment;
      
      // Add a short delay to avoid rate limits
      await delay(500);
    }
    
    console.log(`\nProcessed all 48 periods:`);
    console.log(`- Total Records: ${totalRecords}`);
    console.log(`- Total Volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`- Total Payment: £${totalPayment.toFixed(2)}`);
    
    return {
      totalRecords,
      totalVolume,
      totalPayment
    };
  } catch (error) {
    console.error('Error processing periods:', error);
    throw error;
  }
}

// Step 3: Create daily summary
async function createDailySummary(): Promise<void> {
  console.log(`\nStep 3: Creating daily summary for ${TARGET_DATE}...`);
  
  try {
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
    
    // Create daily summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totals[0].totalCurtailedEnergy,
      totalPayment: totals[0].totalPayment,
      totalWindGeneration: '0', // We don't have this data yet
      windOnshoreGeneration: '0',
      windOffshoreGeneration: '0',
      lastUpdated: new Date()
    });
    
    console.log(`Created daily summary for ${TARGET_DATE}:`);
    console.log(`- Energy: ${totals[0].totalCurtailedEnergy} MWh`);
    console.log(`- Payment: £${totals[0].totalPayment}`);
  } catch (error) {
    console.error('Error creating daily summary:', error);
    throw error;
  }
}

// Step 4: Update monthly and yearly summaries
async function updateMonthlySummary(yearMonth: string): Promise<void> {
  console.log(`\nStep 4a: Updating monthly summary for ${yearMonth}...`);
  
  try {
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
      
      console.log(`Updated monthly summary for ${yearMonth}:`);
      console.log(`- Energy: ${monthlyTotals[0].totalCurtailedEnergy} MWh`);
      console.log(`- Payment: £${monthlyTotals[0].totalPayment}`);
    }
  } catch (error) {
    console.error(`Error updating monthly summary for ${yearMonth}:`, error);
    throw error;
  }
}

async function updateYearlySummary(year: string): Promise<void> {
  console.log(`\nStep 4b: Updating yearly summary for ${year}...`);
  
  try {
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
      
      console.log(`Updated yearly summary for ${year}:`);
      console.log(`- Energy: ${yearlyTotals[0].totalCurtailedEnergy} MWh`);
      console.log(`- Payment: £${yearlyTotals[0].totalPayment}`);
    }
  } catch (error) {
    console.error(`Error updating yearly summary for ${year}:`, error);
    throw error;
  }
}

// Step 5: Update Bitcoin calculations
async function updateBitcoinCalculations(): Promise<void> {
  console.log(`\nStep 5: Updating Bitcoin calculations...`);
  
  try {
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    for (const minerModel of minerModels) {
      await processSingleDay(TARGET_DATE, minerModel);
      console.log(`- Processed Bitcoin calculations for ${minerModel}`);
    }
    
    console.log('Bitcoin calculations updated successfully');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    throw error;
  }
}

// Step 6: Verify data
async function verifyUpdate(): Promise<void> {
  console.log(`\nStep 6: Verifying update...`);
  
  try {
    // Check curtailment records
    const recordCount = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Check number of periods
    const periodCount = await db
      .select({ count: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Check totals
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Verification Results:`);
    console.log(`- Total Records: ${recordCount[0].count}`);
    console.log(`- Unique Periods: ${periodCount[0].count}/48`);
    console.log(`- Total Energy: ${totals[0].totalCurtailedEnergy} MWh`);
    console.log(`- Total Payment: £${totals[0].totalPayment}`);
    
    // Compare with expected values
    const expectedVolume = 99904.22;
    const expectedPayment = -3784089.62;
    
    const volumeDiff = Math.abs(expectedVolume - parseFloat(totals[0].totalCurtailedEnergy));
    const paymentDiff = Math.abs(expectedPayment - parseFloat(totals[0].totalPayment));
    
    console.log(`\nComparison to Expected Values:`);
    console.log(`- Expected Energy: ${expectedVolume} MWh (Diff: ${volumeDiff.toFixed(2)} MWh, ${((volumeDiff / expectedVolume) * 100).toFixed(2)}%)`);
    console.log(`- Expected Payment: £${expectedPayment} (Diff: £${paymentDiff.toFixed(2)}, ${((paymentDiff / Math.abs(expectedPayment)) * 100).toFixed(2)}%)`);
  } catch (error) {
    console.error('Error verifying update:', error);
    throw error;
  }
}

// Main function
async function main(): Promise<void> {
  console.log(`=== Reprocessing Data for ${TARGET_DATE} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    // Step 1: Clear existing data
    await clearExistingData();
    
    // Step 2: Process all periods
    await processAllPeriods();
    
    // Step 3: Create daily summary
    await createDailySummary();
    
    // Step 4: Update monthly and yearly summaries
    const yearMonth = TARGET_DATE.substring(0, 7);
    const year = TARGET_DATE.substring(0, 4);
    await updateMonthlySummary(yearMonth);
    await updateYearlySummary(year);
    
    // Step 5: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 6: Verify update
    await verifyUpdate();
    
    console.log(`\nReprocessing completed successfully at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Error during reprocessing:', error);
    process.exit(1);
  }
}

// Execute main function
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
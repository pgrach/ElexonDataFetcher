/**
 * Fetch Missing Elexon Data
 * 
 * This script identifies and fixes missing data by ensuring we are correctly
 * fetching and processing all valid curtailment records including both SO Flag
 * and CADL Flag records.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, and, sql, inArray } from "drizzle-orm";
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

// Make request to Elexon API
async function makeElexonRequest(url: string, date: string, period: number): Promise<any> {
  try {
    console.log(`Requesting ${url}`);
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000 // 30 second timeout
    });
    return response;
  } catch (error) {
    if (axios.isAxiosError(error) && error.response?.status === 429) {
      console.log(`[${date} P${period}] Rate limited, retrying after delay...`);
      await delay(60000); // Wait 1 minute on rate limit
      return makeElexonRequest(url, date, period);
    }
    throw error;
  }
}

// Fetch bids and offers from Elexon API with correct filtering
async function fetchCompleteBidsOffers(date: string, period: number): Promise<any[]> {
  try {
    // Make parallel requests for bids and offers
    const [bidsResponse, offersResponse] = await Promise.all([
      makeElexonRequest(
        `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`,
        date,
        period
      ),
      makeElexonRequest(
        `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`,
        date,
        period
      )
    ]).catch(error => {
      console.error(`[${date} P${period}] Error fetching data:`, error.message);
      return [{ data: { data: [] } }, { data: { data: [] } }];
    });

    if (!bidsResponse.data?.data || !offersResponse.data?.data) {
      console.error(`[${date} P${period}] Invalid API response format`);
      return [];
    }

    // Include both soFlag and cadlFlag records
    const allBids = bidsResponse.data.data || [];
    const allOffers = offersResponse.data.data || [];
    
    // Log the initial responses
    console.log(`Raw bids response: ${allBids.length} records`);
    console.log(`Raw offers response: ${allOffers.length} records`);
    
    return [...allBids, ...allOffers];
  } catch (error) {
    console.error(`[${date} P${period}] Error fetching data:`, error);
    return [];
  }
}

// Process a period
async function processPeriodCompletely(
  period: number, 
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  volume: number;
  payment: number;
}> {
  console.log(`\nProcessing period ${period} for ${TARGET_DATE}...`);
  
  try {
    // Get raw records with both soFlag and cadlFlag
    const rawRecords = await fetchCompleteBidsOffers(TARGET_DATE, period);
    console.log(`[${TARGET_DATE} P${period}] Raw records: ${rawRecords.length}`);
    
    // Filter for negative volume and wind farms
    const validRecords = rawRecords.filter(record =>
      record.volume < 0 && 
      (record.soFlag || record.cadlFlag) && 
      windFarmIds.has(record.id)
    );
    
    console.log(`[${TARGET_DATE} P${period}] Valid records: ${validRecords.length}`);
    
    // Get existing records for this period
    const existingRecords = await db
      .select({ farmId: curtailmentRecords.farmId })
      .from(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        eq(curtailmentRecords.settlementPeriod, period)
      ));
    
    const existingFarmIds = new Set(existingRecords.map(r => r.farmId));
    console.log(`[${TARGET_DATE} P${period}] Existing farm records: ${existingFarmIds.size}`);
    
    // Filter for new records
    const newRecords = validRecords.filter(record => !existingFarmIds.has(record.id));
    console.log(`[${TARGET_DATE} P${period}] New records to add: ${newRecords.length}`);
    
    if (newRecords.length === 0) {
      console.log(`[${TARGET_DATE} P${period}] No new records to process.`);
      return { volume: 0, payment: 0 };
    }
    
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process and insert new records
    for (const record of newRecords) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
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
      
      console.log(`[${TARGET_DATE} P${period}] Added record for ${record.id}: ${volume} MWh, £${payment}`);
      totalVolume += volume;
      totalPayment += payment;
    }
    
    console.log(`[${TARGET_DATE} P${period}] Total added: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    return { volume: totalVolume, payment: totalPayment };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return { volume: 0, payment: 0 };
  }
}

// Update all summary tables
async function updateSummaries(): Promise<void> {
  try {
    console.log(`\nUpdating summary records for ${TARGET_DATE}...`);
    
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
  console.log(`\nUpdating Bitcoin calculations for ${TARGET_DATE}...`);
  
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

// Compare existing data with target data
async function compareWithTargetData(): Promise<void> {
  const currentTotals = await db
    .select({
      totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  console.log(`\nCurrent Data for ${TARGET_DATE}:`);
  console.log(`- Energy: ${currentTotals[0].totalCurtailedEnergy} MWh`);
  console.log(`- Payment: £${currentTotals[0].totalPayment}`);
  
  const targetEnergy = 99904.22;
  const targetPayment = -3784089.62;
  
  console.log(`\nTarget Data for ${TARGET_DATE}:`);
  console.log(`- Energy: ${targetEnergy} MWh`);
  console.log(`- Payment: £${targetPayment}`);
  
  const energyDiff = targetEnergy - parseFloat(currentTotals[0].totalCurtailedEnergy);
  const paymentDiff = targetPayment - parseFloat(currentTotals[0].totalPayment);
  
  console.log(`\nData Gap:`);
  console.log(`- Energy: ${energyDiff.toFixed(2)} MWh`);
  console.log(`- Payment: £${paymentDiff.toFixed(2)}`);
}

// Main function
async function main(): Promise<void> {
  console.log(`=== Fixing Missing Data for ${TARGET_DATE} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    // Step 0: Compare with target data
    await compareWithTargetData();
    
    // Step 1: Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Step 2: Process only specific periods where data might be missing
    // Focus on later periods (33-48) which might have incomplete data
    let totalAddedVolume = 0;
    let totalAddedPayment = 0;
    
    // Process specific periods where data might be incomplete
    const periodsToCheck = [33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48];
    
    for (const period of periodsToCheck) {
      const result = await processPeriodCompletely(period, windFarmIds, bmuLeadPartyMap);
      totalAddedVolume += result.volume;
      totalAddedPayment += result.payment;
      await delay(500); // Add delay between API calls to avoid rate limits
    }
    
    console.log(`\nTotal added data: ${totalAddedVolume.toFixed(2)} MWh, £${totalAddedPayment.toFixed(2)}`);
    
    // Step 3: Update all summary tables
    await updateSummaries();
    
    // Step 4: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 5: Verify data
    const finalTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nFinal Data for ${TARGET_DATE}:`);
    console.log(`- Energy: ${finalTotals[0].totalCurtailedEnergy} MWh`);
    console.log(`- Payment: £${finalTotals[0].totalPayment}`);
    
    console.log(`\nUpdate completed successfully at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Error during data fix process:', error);
    process.exit(1);
  }
}

// Execute main function
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
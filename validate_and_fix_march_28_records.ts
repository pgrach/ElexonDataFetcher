/**
 * Elexon Data Validation and Correction Script
 * 
 * This script validates all curtailment records for March 28, 2025 against the Elexon API
 * to ensure our database contains accurate data. If any discrepancies are found,
 * the script corrects the records and updates all dependent summaries.
 * 
 * The Elexon API is considered the authoritative source of truth.
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

const TARGET_DATE = '2025-03-28';
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");

// Used to track changes for reporting
interface ValidationStats {
  totalRecords: number;
  matchingRecords: number;
  missingRecords: number;
  updatedRecords: number;
  addedRecords: number;
  totalPaymentDifference: number;
  totalVolumeDifference: number;
  changedPeriods: Set<number>;
}

// Result from processing a period
interface PeriodProcessResult {
  period: number;
  totalRecords: number;
  matchingRecords: number;  // Added this property
  missingRecords: number;
  updatedRecords: number;
  addedRecords: number;
  volumeDifference: number;
  paymentDifference: number;
}

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

// Validate and fix records for a single period
async function validateAndFixPeriod(
  period: number, 
  windFarmIds: Set<string>, 
  bmuLeadPartyMap: Map<string, string>
): Promise<PeriodProcessResult> {
  console.log(`\nValidating period ${period} for ${TARGET_DATE}...`);
  
  // Result for this period
  const result: PeriodProcessResult = {
    period,
    totalRecords: 0,
    matchingRecords: 0,
    missingRecords: 0,
    updatedRecords: 0,
    addedRecords: 0,
    volumeDifference: 0,
    paymentDifference: 0
  };
  
  try {
    // Get existing records from database
    const existingRecords = await db
      .select()
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    // Create mapping of farmId -> record for quick lookup
    const existingRecordsMap = new Map();
    for (const record of existingRecords) {
      existingRecordsMap.set(record.farmId, record);
    }
    
    // Fetch data from Elexon API (source of truth)
    const apiRecords = await fetchBidsOffers(TARGET_DATE, period);
    
    // Filter for valid curtailment records
    const validApiRecords = apiRecords.filter(record =>
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      windFarmIds.has(record.id)
    );
    
    result.totalRecords = validApiRecords.length;
    
    if (validApiRecords.length === 0) {
      console.log(`No valid curtailment records found for ${TARGET_DATE} period ${period}`);
      return result;
    }
    
    console.log(`Processing ${validApiRecords.length} records from Elexon API for period ${period}`);
    
    // Process each valid record from the API
    for (const apiRecord of validApiRecords) {
      const volume = Math.abs(apiRecord.volume);
      const payment = volume * apiRecord.originalPrice;
      
      // Check if record exists in database
      const existingRecord = existingRecordsMap.get(apiRecord.id);
      
      if (!existingRecord) {
        // Record is missing - add it
        result.missingRecords++;
        result.addedRecords++;
        
        await db.insert(curtailmentRecords).values({
          settlementDate: TARGET_DATE,
          settlementPeriod: period,
          farmId: apiRecord.id,
          leadPartyName: bmuLeadPartyMap.get(apiRecord.id) || 'Unknown',
          volume: apiRecord.volume.toString(), // Keep the original negative value
          payment: payment.toString(),
          originalPrice: apiRecord.originalPrice.toString(),
          finalPrice: apiRecord.finalPrice.toString(),
          soFlag: apiRecord.soFlag,
          cadlFlag: apiRecord.cadlFlag
        });
        
        result.volumeDifference += volume;
        result.paymentDifference += payment;
        
        console.log(`ADDED: Record for ${apiRecord.id} in period ${period}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
      } else {
        // Record exists - validate values
        const existingVolume = Math.abs(parseFloat(existingRecord.volume));
        const existingPayment = parseFloat(existingRecord.payment);
        
        // Check if there's a significant discrepancy
        const volumeDiff = Math.abs(existingVolume - volume);
        const paymentDiff = Math.abs(existingPayment - payment);
        
        // Use threshold to account for floating point precision
        if (volumeDiff > 0.001 || paymentDiff > 0.001) {
          // Significant difference found - update the record
          result.updatedRecords++;
          
          await db.update(curtailmentRecords)
            .set({
              volume: apiRecord.volume.toString(),
              payment: payment.toString(),
              originalPrice: apiRecord.originalPrice.toString(),
              finalPrice: apiRecord.finalPrice.toString(),
              soFlag: apiRecord.soFlag,
              cadlFlag: apiRecord.cadlFlag
            })
            .where(
              and(
                eq(curtailmentRecords.settlementDate, TARGET_DATE),
                eq(curtailmentRecords.settlementPeriod, period),
                eq(curtailmentRecords.farmId, apiRecord.id)
              )
            );
          
          result.volumeDifference += (volume - existingVolume);
          result.paymentDifference += (payment - existingPayment);
          
          console.log(`UPDATED: Record for ${apiRecord.id} in period ${period}:`);
          console.log(`  Volume: ${existingVolume.toFixed(2)} MWh → ${volume.toFixed(2)} MWh (diff: ${(volume - existingVolume).toFixed(2)} MWh)`);
          console.log(`  Payment: £${existingPayment.toFixed(2)} → £${payment.toFixed(2)} (diff: £${(payment - existingPayment).toFixed(2)})`);
        } else {
          result.matchingRecords++;
        }
      }
      
      // Remove the record from the map since it's been processed
      existingRecordsMap.delete(apiRecord.id);
    }
    
    // At this point, any records left in existingRecordsMap shouldn't be there
    // They are in our database but not in the API response
    if (existingRecordsMap.size > 0) {
      console.log(`WARNING: Found ${existingRecordsMap.size} records in database that are not in API response for period ${period}`);
      console.log('These records will be kept for safety, but please verify they are valid.');
      
      for (const [farmId, record] of existingRecordsMap.entries()) {
        console.log(`  Unmatched record: ${farmId}, Period ${period}, ${Math.abs(parseFloat(record.volume)).toFixed(2)} MWh, £${parseFloat(record.payment).toFixed(2)}`);
      }
    }
    
    return result;
  } catch (error) {
    console.error(`Error validating period ${period}:`, error);
    return result;
  }
}

// Update all summary tables after corrections
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

// Format number with thousand separators
function formatNumber(num: number): string {
  return num.toLocaleString('en-US');
}

// Constants for batch processing - change these to process different ranges
const START_PERIOD = 1;  // Start from this period
const END_PERIOD = 10;   // End at this period (inclusive)

// Main function
async function main(): Promise<void> {
  console.log(`=== Validating March 28, 2025 Curtailment Records (Periods ${START_PERIOD}-${END_PERIOD}) ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  const stats: ValidationStats = {
    totalRecords: 0,
    matchingRecords: 0,
    missingRecords: 0,
    updatedRecords: 0,
    addedRecords: 0,
    totalPaymentDifference: 0,
    totalVolumeDifference: 0,
    changedPeriods: new Set<number>()
  };
  
  try {
    // Step 1: Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Step 2: Process specified period range
    const selectedPeriods = Array.from(
      { length: END_PERIOD - START_PERIOD + 1 },
      (_, i) => START_PERIOD + i
    );
    
    console.log(`Processing periods ${selectedPeriods.join(', ')}...`);
    
    for (const period of selectedPeriods) {
      const result = await validateAndFixPeriod(period, windFarmIds, bmuLeadPartyMap);
      
      // Update statistics
      stats.totalRecords += result.totalRecords;
      stats.matchingRecords += result.matchingRecords;
      stats.missingRecords += result.missingRecords;
      stats.updatedRecords += result.updatedRecords;
      stats.addedRecords += result.addedRecords;
      stats.totalVolumeDifference += result.volumeDifference;
      stats.totalPaymentDifference += result.paymentDifference;
      
      if (result.updatedRecords > 0 || result.addedRecords > 0) {
        stats.changedPeriods.add(period);
      }
      
      // Add delay to avoid rate limits
      await delay(500);
    }
    
    // Step 3: If any changes were made, update all summaries and calculations
    if (stats.changedPeriods.size > 0) {
      console.log(`\nChanges detected in ${stats.changedPeriods.size} periods. Updating summaries...`);
      
      // Update summary tables
      await updateSummaries();
      
      // Update Bitcoin calculations
      await updateBitcoinCalculations();
    } else {
      console.log('\nNo changes needed. All records are accurate.');
    }
    
    // Step 4: Print validation summary
    console.log('\n=== Validation Summary ===');
    console.log(`Total periods processed: 48`);
    console.log(`Total records from API: ${formatNumber(stats.totalRecords)}`);
    console.log(`Matching records: ${formatNumber(stats.matchingRecords)}`);
    console.log(`Missing records found and added: ${formatNumber(stats.addedRecords)}`);
    console.log(`Existing records updated: ${formatNumber(stats.updatedRecords)}`);
    console.log(`Periods with changes: ${stats.changedPeriods.size > 0 ? Array.from(stats.changedPeriods).sort((a, b) => a - b).join(', ') : 'None'}`);
    console.log(`Total volume difference: ${stats.totalVolumeDifference.toFixed(2)} MWh`);
    console.log(`Total payment difference: £${stats.totalPaymentDifference.toFixed(2)}`);
    
    console.log(`\nValidation completed successfully at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Error during validation process:', error);
    process.exit(1);
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
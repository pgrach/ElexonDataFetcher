/**
 * Reingest March 21, 2025 Settlement Periods
 * 
 * This script completely removes all settlement period data for March 21, 2025
 * and then reingests all 48 settlement periods from the Elexon API.
 * 
 * The goal is to ensure the total payment matches the expected amount of £1,240,439.58
 * and Energy Curtailed 50,518.72 MWh according to Elexon API data.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, and, sql, between } from "drizzle-orm";
import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// ES module support for __dirname equivalent
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const TARGET_DATE = '2025-03-21';
const EXPECTED_TOTAL_PAYMENT = 1240439.58; // Expected total in GBP
const EXPECTED_TOTAL_ENERGY = 50518.72; // Expected total in MWh
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");
const API_THROTTLE_MS = 1000; // Time between API calls to avoid rate limiting
const LOG_FILE = `reingest_${TARGET_DATE}.log`;
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const MAX_RETRIES = 3;
const BATCH_SIZE = 6; // Process periods in batches

// Helper function to delay execution
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Logger function
function log(message: string, type: "info" | "success" | "warning" | "error" = "info"): void {
  const timestamp = new Date().toISOString().replace('T', ' ').substring(0, 19);
  let formattedMessage: string;
  
  switch (type) {
    case "success":
      formattedMessage = `✓ [${timestamp}] ${message}`;
      console.log(`\x1b[32m${formattedMessage}\x1b[0m`);
      break;
    case "warning":
      formattedMessage = `⚠ [${timestamp}] ${message}`;
      console.log(`\x1b[33m${formattedMessage}\x1b[0m`);
      break;
    case "error":
      formattedMessage = `✖ [${timestamp}] ${message}`;
      console.log(`\x1b[31m${formattedMessage}\x1b[0m`);
      break;
    default:
      formattedMessage = `ℹ [${timestamp}] ${message}`;
      console.log(`\x1b[36m${formattedMessage}\x1b[0m`);
  }
  
  // Append to log file
  fs.appendFile(LOG_FILE, `${formattedMessage}\n`).catch(() => {});
}

// Load BMU mappings 
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`, "info");
  
  try {
    const data = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(data);
    
    const windFarmIds = new Set<string>();
    const bmuLeadPartyMap = new Map<string, string>();
    
    for (const bmu of bmuMapping) {
      windFarmIds.add(bmu.elexonBmUnit);
      bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName);
    }
    
    log(`Found ${windFarmIds.size} wind farm BMUs`, "success");
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    log(`Error loading BMU mapping: ${error}`, "error");
    throw new Error(`Failed to load BMU mappings: ${error}`);
  }
}

// Clear existing data for the target date
async function clearExistingData(): Promise<void> {
  log(`Clearing existing data for ${TARGET_DATE}...`, "info");
  
  try {
    // Clear all curtailment records
    const deletedRecords = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .returning({ id: curtailmentRecords.id });
    
    log(`Cleared ${deletedRecords.length} existing curtailment records`, "success");
    
    // Clear daily summary
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    log(`Cleared daily summary for ${TARGET_DATE}`, "success");
  } catch (error) {
    log(`Error clearing existing data: ${error}`, "error");
    throw new Error(`Failed to clear existing data: ${error}`);
  }
}

// Process a single settlement period
async function processPeriod(
  period: number,
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>,
  attempt: number = 1
): Promise<{
  success: boolean;
  records: number;
  volume: number;
  payment: number;
}> {
  log(`Processing period ${period} (attempt ${attempt})`, "info");
  
  try {
    // Fetch data from the Elexon API - make parallel requests for bids and offers (open API - no key needed)
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(`${API_BASE_URL}/balancing/settlement/stack/all/bid/${TARGET_DATE}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      }),
      axios.get(`${API_BASE_URL}/balancing/settlement/stack/all/offer/${TARGET_DATE}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      })
    ]);

    // Combine both datasets
    const bidsData = bidsResponse.data.data || [];
    const offersData = offersResponse.data.data || [];
    const data = [...bidsData, ...offersData];
    
    // Filter to keep only valid wind farm records
    const validRecords = data.filter((record: any) => {
      return windFarmIds.has(record.id) && record.volume < 0 && record.soFlag; // Negative volume indicates curtailment
    });
    
    const totalVolume = validRecords.reduce((sum: number, record: any) => sum + Math.abs(record.volume), 0);
    const totalPayment = validRecords.reduce((sum: number, record: any) => sum + (Math.abs(record.volume) * record.originalPrice), 0);
    
    log(`[${TARGET_DATE} P${period}] Records: ${validRecords.length} (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
    
    let recordsAdded = 0;
    let totalVolumeAdded = 0;
    let totalPaymentAdded = 0;
    
    // Get the unique farm IDs for this period
    const uniqueFarmIds = [...new Set(validRecords.map((record: any) => record.id))];
    
    // Clear all existing records for this period
    try {
      const deleteResult = await db.delete(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, TARGET_DATE),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );
      
      log(`Period ${period}: Cleared existing records before insertion`, "info");
    } catch (error) {
      log(`Period ${period}: Error clearing existing records: ${error}`, "error");
    }
    
    // Prepare all records for bulk insertion
    const recordsToInsert = validRecords.map((record: any) => {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      // Track totals for return value
      totalVolumeAdded += volume;
      totalPaymentAdded += payment;
      
      return {
        settlementDate: TARGET_DATE,
        settlementPeriod: period,
        farmId: record.id,
        leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
        volume: record.volume.toString(), // Keep negative value to indicate curtailment
        payment: payment.toString(),
        originalPrice: record.originalPrice.toString(),
        finalPrice: record.finalPrice.toString(),
        soFlag: record.soFlag || false,
        cadlFlag: record.cadlFlag || false
      };
    });
    
    // Insert all records in a single transaction if there are any
    if (recordsToInsert.length > 0) {
      try {
        await db.insert(curtailmentRecords).values(recordsToInsert);
        recordsAdded = recordsToInsert.length;
        
        // Log summary for visibility
        log(`Period ${period}: Added ${recordsAdded} records (${totalVolumeAdded.toFixed(2)} MWh, £${totalPaymentAdded.toFixed(2)})`, "success");
        
      } catch (error) {
        log(`Period ${period}: Error bulk inserting records: ${error}`, "error");
      }
    }
    
    return { 
      success: true, 
      records: recordsAdded,
      volume: totalVolumeAdded,
      payment: totalPaymentAdded
    };
    
  } catch (error) {
    log(`Error processing period ${period}: ${error}`, "error");
    
    // Retry logic
    if (attempt < MAX_RETRIES) {
      log(`Retrying period ${period} in ${API_THROTTLE_MS/1000} seconds... (attempt ${attempt + 1}/${MAX_RETRIES})`, "warning");
      await delay(API_THROTTLE_MS);
      return processPeriod(period, windFarmIds, bmuLeadPartyMap, attempt + 1);
    }
    
    return { 
      success: false, 
      records: 0,
      volume: 0,
      payment: 0
    };
  }
}

// Process a batch of periods
async function processBatch(
  periods: number[],
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  success: boolean;
  processed: number;
  total: number;
  records: number;
  volume: number;
  payment: number;
}> {
  log(`Processing batch: periods ${periods[0]}-${periods[periods.length - 1]}`);
  
  let totalRecords = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  let periodsProcessed = 0;
  let totalPeriods = periods.length;
  let success = true;
  
  for (const period of periods) {
    const result = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
    
    if (result.success) {
      periodsProcessed++;
      totalRecords += result.records;
      totalVolume += result.volume;
      totalPayment += result.payment;
    } else {
      success = false;
      log(`Failed to process period ${period} after ${MAX_RETRIES} attempts`, "error");
    }
    
    // Add a delay between periods to avoid rate limiting
    await delay(API_THROTTLE_MS);
  }
  
  log(`Batch ${periods[0]}-${periods[periods.length - 1]} complete:`, success ? "success" : "warning");
  log(`- Periods processed: ${periodsProcessed}/${totalPeriods}`);
  log(`- Records added: ${totalRecords}`);
  log(`- Total volume: ${totalVolume.toFixed(2)} MWh`);
  log(`- Total payment: £${totalPayment.toFixed(2)}`);
  
  return {
    success,
    processed: periodsProcessed,
    total: totalPeriods,
    records: totalRecords,
    volume: totalVolume,
    payment: totalPayment
  };
}

// Update daily, monthly, and yearly summaries
async function updateSummaries(): Promise<void> {
  log(`Updating summary tables for ${TARGET_DATE}...`, "info");
  
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
      throw new Error('No curtailment records found to create summary');
    }
    
    const totalEnergy = parseFloat(totals[0].totalCurtailedEnergy);
    const totalPayment = parseFloat(totals[0].totalPayment);
    
    // Insert or update the daily summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totals[0].totalCurtailedEnergy,
      totalPayment: totals[0].totalPayment,
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totals[0].totalCurtailedEnergy,
        totalPayment: totals[0].totalPayment,
        lastUpdated: new Date()
      }
    });
    
    log('Daily summary updated successfully:', "success");
    log(`- Energy: ${totalEnergy.toFixed(2)} MWh`);
    log(`- Payment: £${totalPayment.toFixed(2)}`);
    
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
      
      log(`Monthly summary updated for ${yearMonth}:`, "success");
      log(`- Energy: ${parseFloat(monthlyTotals[0].totalCurtailedEnergy).toFixed(2)} MWh`);
      log(`- Payment: £${parseFloat(monthlyTotals[0].totalPayment).toFixed(2)}`);
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
      
      log(`Yearly summary updated for ${year}:`, "success");
      log(`- Energy: ${parseFloat(yearlyTotals[0].totalCurtailedEnergy).toFixed(2)} MWh`);
      log(`- Payment: £${parseFloat(yearlyTotals[0].totalPayment).toFixed(2)}`);
    }
  } catch (error) {
    log(`Error updating summaries: ${error}`, "error");
    throw new Error(`Failed to update summaries: ${error}`);
  }
}

// Update Bitcoin calculations
async function updateBitcoinCalculations(): Promise<void> {
  log(`Updating Bitcoin calculations for ${TARGET_DATE}...`, "info");
  
  try {
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    for (const minerModel of minerModels) {
      await processSingleDay(TARGET_DATE, minerModel);
      log(`Processed Bitcoin calculations for ${minerModel}`, "success");
    }
    
    log('Bitcoin calculations updated successfully', "success");
  } catch (error) {
    log(`Error updating Bitcoin calculations: ${error}`, "error");
    throw new Error(`Failed to update Bitcoin calculations: ${error}`);
  }
}

// Verify the results match expected values
async function verifyResults(): Promise<boolean> {
  log(`Verifying results for ${TARGET_DATE}...`, "info");
  
  try {
    const dailySummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    if (dailySummary.length === 0) {
      log(`No daily summary found for ${TARGET_DATE}`, "error");
      return false;
    }
    
    const actualEnergy = parseFloat(dailySummary[0].totalCurtailedEnergy?.toString() || '0');
    const actualPayment = parseFloat(dailySummary[0].totalPayment?.toString() || '0');
    
    const energyDiff = Math.abs(actualEnergy - EXPECTED_TOTAL_ENERGY);
    const paymentDiff = Math.abs(actualPayment - EXPECTED_TOTAL_PAYMENT);
    
    const energyMatch = energyDiff < 1; // Allow small difference (less than 1 MWh)
    const paymentMatch = paymentDiff < 1; // Allow small difference (less than £1)
    
    log(`Results verification:`, energyMatch && paymentMatch ? "success" : "warning");
    log(`- Energy: ${actualEnergy.toFixed(2)} MWh (Expected: ${EXPECTED_TOTAL_ENERGY.toFixed(2)} MWh, Diff: ${energyDiff.toFixed(2)} MWh)`);
    log(`- Payment: £${actualPayment.toFixed(2)} (Expected: £${EXPECTED_TOTAL_PAYMENT.toFixed(2)}, Diff: £${paymentDiff.toFixed(2)})`);
    
    return energyMatch && paymentMatch;
  } catch (error) {
    log(`Error verifying results: ${error}`, "error");
    return false;
  }
}

// Main function to orchestrate the entire reingestion process
async function main(): Promise<void> {
  const startTime = Date.now();
  
  log(`=== Starting complete data reingest for ${TARGET_DATE} ===`, "info");
  log(`Target values: ${EXPECTED_TOTAL_ENERGY.toFixed(2)} MWh, £${EXPECTED_TOTAL_PAYMENT.toFixed(2)}`, "info");
  
  try {
    // Step 1: Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Step 2: Clear existing data
    await clearExistingData();
    
    // Step 3: Process all 48 settlement periods in batches
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    let periodsProcessed = 0;
    
    // Create batches of settlement periods
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    const batches = [];
    for (let i = 0; i < allPeriods.length; i += BATCH_SIZE) {
      batches.push(allPeriods.slice(i, i + BATCH_SIZE));
    }
    
    // Process each batch
    for (let i = 0; i < batches.length; i++) {
      log(`Processing batch ${i + 1} of ${batches.length}`);
      
      const batchResult = await processBatch(batches[i], windFarmIds, bmuLeadPartyMap);
      
      periodsProcessed += batchResult.processed;
      totalRecords += batchResult.records;
      totalVolume += batchResult.volume;
      totalPayment += batchResult.payment;
      
      // Small delay between batches
      await delay(2000);
    }
    
    // Step 4: Update all summary tables
    await updateSummaries();
    
    // Step 5: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 6: Verify the results
    const verified = await verifyResults();
    
    // Final summary
    const executionTime = (Date.now() - startTime) / 1000;
    log(`=== Reingestion complete for ${TARGET_DATE} ===`, verified ? "success" : "warning");
    log(`- Execution time: ${executionTime.toFixed(1)} seconds`);
    log(`- Periods processed: ${periodsProcessed}/48`);
    log(`- Records added: ${totalRecords}`);
    log(`- Total volume: ${totalVolume.toFixed(2)} MWh`);
    log(`- Total payment: £${totalPayment.toFixed(2)}`);
    log(`- Verification: ${verified ? 'Passed ✓' : 'Failed ✗'}`);
    
  } catch (error) {
    log(`Fatal error during processing: ${error}`, "error");
    process.exit(1);
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
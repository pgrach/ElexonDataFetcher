#!/usr/bin/env tsx
/**
 * Script to fix missing periods 11 and 12 for 2025-03-11
 * 
 * This script focuses specifically on reingesting data for the missing periods
 * identified by the check_2025_03_11.ts script.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, sql, count, and, between } from "drizzle-orm";
import { fetchBidsOffers } from "./server/services/elexon";
import { processSingleDay } from "./server/services/bitcoinService";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

// Configuration
const DATE = "2025-03-11";
const MISSING_PERIODS = [11, 12]; // The specific periods identified as missing
const BMU_MAPPING_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), "server", "data", "bmuMapping.json");

// Helper function for logging
function log(message: string, type: "info" | "success" | "warning" | "error" = "info") {
  const colors = {
    info: "\x1b[36m•\x1b[0m",     // Blue dot
    success: "\x1b[32m✓\x1b[0m",  // Green checkmark
    warning: "\x1b[33m⚠\x1b[0m",  // Yellow warning
    error: "\x1b[31m✗\x1b[0m"     // Red X
  };

  console.log(`${colors[type]} ${message}`);
}

// Load wind farm mapping
async function loadBmuMappings(): Promise<{
  windFarmIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`, "info");
  
  const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
  const bmuMapping = JSON.parse(mappingContent);
  
  const windFarmIds = new Set(
    bmuMapping
      .filter((bmu: any) => bmu.fuelType === "WIND")
      .map((bmu: any) => bmu.elexonBmUnit)
  );
  
  const bmuLeadPartyMap = new Map(
    bmuMapping
      .filter((bmu: any) => bmu.fuelType === "WIND")
      .map((bmu: any) => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
  );
  
  log(`Loaded ${windFarmIds.size} wind farm BMU IDs`, "info");
  
  return { windFarmIds, bmuLeadPartyMap };
}

// Process a single period
async function processPeriod(
  period: number,
  windFarmIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{ recordCount: number; totalVolume: number; totalPayment: number }> {
  log(`Processing period ${period}`, "info");
  
  // Clear any existing records for this period to prevent duplicates
  await db.delete(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, DATE),
        eq(curtailmentRecords.settlementPeriod, period)
      )
    );
  
  // Fetch new data from Elexon API
  const records = await fetchBidsOffers(DATE, period);
  const validRecords = records.filter(record => 
    record.volume < 0 &&
    (record.soFlag || record.cadlFlag) &&
    windFarmIds.has(record.id)
  );
  
  let periodStats = {
    recordCount: 0,
    totalVolume: 0,
    totalPayment: 0
  };
  
  if (validRecords.length > 0) {
    log(`[${DATE} P${period}] Processing ${validRecords.length} records`, "info");
    
    for (const record of validRecords) {
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      try {
        await db.insert(curtailmentRecords).values({
          settlementDate: DATE,
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
        
        log(`[${DATE} P${period}] Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`, "success");
        
        periodStats.recordCount++;
        periodStats.totalVolume += volume;
        periodStats.totalPayment += payment;
      } catch (error) {
        log(`[${DATE} P${period}] Error inserting record for ${record.id}: ${error}`, "error");
      }
    }
    
    log(`[${DATE} P${period}] Total: ${periodStats.totalVolume.toFixed(2)} MWh, £${periodStats.totalPayment.toFixed(2)}`, "success");
  } else {
    log(`[${DATE} P${period}] No valid records found`, "warning");
  }
  
  return periodStats;
}

// Update daily summary after processing
async function updateDailySummary(): Promise<void> {
  log(`Updating daily summary for ${DATE}`, "info");
  
  const totalStats = await db
    .select({
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE));
  
  if (totalStats[0]) {
    await db.insert(dailySummaries).values({
      summaryDate: DATE,
      totalCurtailedEnergy: totalStats[0].totalVolume,
      totalPayment: totalStats[0].totalPayment
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totalStats[0].totalVolume,
        totalPayment: totalStats[0].totalPayment
      }
    });
    
    log(`Updated daily summary: ${Number(totalStats[0].totalVolume).toFixed(2)} MWh, £${Number(totalStats[0].totalPayment).toFixed(2)}`, "success");
  }
}

// Update Bitcoin calculations
async function updateBitcoinCalculations(): Promise<void> {
  log(`Updating Bitcoin calculations for ${DATE}`, "info");
  
  const mainerModels = ['S19J_PRO', 'S9', 'M20S'];
  
  for (const minerModel of mainerModels) {
    log(`Processing Bitcoin calculations for ${minerModel}`, "info");
    await processSingleDay(DATE, minerModel);
  }
  
  log(`Bitcoin calculations updated`, "success");
}

// Verify results after processing
async function verifyResults(): Promise<void> {
  // Check if we now have all 48 periods
  const periodCheck = await db
    .select({
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE));
  
  if (periodCheck[0]?.periodCount === 48) {
    log(`Successfully processed all 48 periods for ${DATE}`, "success");
  } else {
    log(`Warning: After processing, we still have only ${periodCheck[0]?.periodCount || 0}/48 periods`, "warning");
    
    // Find which periods are still missing
    const existingPeriods = await db
      .select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE))
      .groupBy(curtailmentRecords.settlementPeriod);
    
    const existingPeriodNumbers = existingPeriods.map(p => p.period);
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    const missingPeriodList = allPeriods.filter(p => !existingPeriodNumbers.includes(p));
    
    if (missingPeriodList.length > 0) {
      log(`Still missing periods: ${missingPeriodList.join(', ')}`, "warning");
    }
  }
}

// Main function
async function main() {
  try {
    log(`Starting to fix missing periods ${MISSING_PERIODS.join(', ')} for ${DATE}`, "info");
    
    // Load BMU mappings
    const { windFarmIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Process each missing period
    let totalRecordsAdded = 0;
    let totalVolumeAdded = 0;
    let totalPaymentAdded = 0;
    
    for (const period of MISSING_PERIODS) {
      const periodStats = await processPeriod(period, windFarmIds, bmuLeadPartyMap);
      totalRecordsAdded += periodStats.recordCount;
      totalVolumeAdded += periodStats.totalVolume;
      totalPaymentAdded += periodStats.totalPayment;
    }
    
    // Update the daily summary with new totals
    await updateDailySummary();
    
    // Update Bitcoin calculations with the new data
    await updateBitcoinCalculations();
    
    // Verify results
    await verifyResults();
    
    log(`Processing complete: Added ${totalRecordsAdded} records, ${totalVolumeAdded.toFixed(2)} MWh, £${totalPaymentAdded.toFixed(2)}`, "success");
  } catch (error) {
    log(`Error during processing: ${error}`, "error");
    process.exit(1);
  }
}

// Run the script
main();
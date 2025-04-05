/**
 * Staged Reingest for March 21, 2025
 * 
 * This script allows for reingesting settlement periods in smaller batches.
 * Set START_PERIOD and END_PERIOD to control which range to process.
 * 
 * The goal is to ensure the total energy matches 50,518.72 MWh and the
 * total payment matches £1,240,439.58 (exact Elexon API values).
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import axios from "axios";
import { eq, sql, and, between } from "drizzle-orm";
import fs from "fs";
import path from "path";
import * as colorette from "colorette";

// Configuration
const TARGET_DATE = "2025-03-21";
const START_PERIOD = 1; // Customize these values
const END_PERIOD = 12;  // to process in batches
const ELEXON_API_URL = "https://api.bmreports.com/BMRS/BOALF/V1";
const BMU_MAPPING_PATH = path.join(process.cwd(), "server", "data", "bmuMapping.json");
const LOG_DIR = path.join(process.cwd(), "logs");
const LOG_FILE_PATH = path.join(LOG_DIR, `staged_reingest_${TARGET_DATE}_periods_${START_PERIOD}-${END_PERIOD}_${Date.now()}.log`);

// Make sure log directory exists
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

// Helper for logging
function logToConsoleAndFile(message: string) {
  console.log(message);
  fs.appendFileSync(LOG_FILE_PATH, message + "\n");
}

// Sleep utility
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mappings
async function loadBmuMappings(): Promise<{
  bmuIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
}> {
  logToConsoleAndFile(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`);
  const mappingData = JSON.parse(fs.readFileSync(BMU_MAPPING_PATH, 'utf8'));
  
  const bmuIds = new Set<string>();
  const bmuLeadPartyMap = new Map<string, string>();
  
  // Extract all BMU IDs and create mapping to lead parties
  for (const mapping of mappingData) {
    if (mapping.nationalGridBmUnit) {
      bmuIds.add(mapping.nationalGridBmUnit);
      bmuLeadPartyMap.set(mapping.nationalGridBmUnit, mapping.leadPartyName);
    }
    if (mapping.elexonBmUnit) {
      bmuIds.add(mapping.elexonBmUnit);
      bmuLeadPartyMap.set(mapping.elexonBmUnit, mapping.leadPartyName);
    }
  }
  
  logToConsoleAndFile(`Loaded ${bmuIds.size} wind farm BMU IDs`);
  return { bmuIds, bmuLeadPartyMap };
}

// Clear existing data for the specific periods
async function clearExistingPeriodsData(): Promise<void> {
  logToConsoleAndFile(`Clearing existing data for periods ${START_PERIOD}-${END_PERIOD} on ${TARGET_DATE}...`);
  
  // Delete curtailment records for specific periods
  const deleteResult = await db.delete(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        between(curtailmentRecords.settlementPeriod, START_PERIOD, END_PERIOD)
      )
    );
  
  logToConsoleAndFile(`Deleted ${deleteResult.rowCount ?? 0} curtailment records for periods ${START_PERIOD}-${END_PERIOD}`);
}

// Process a single settlement period
async function processPeriod(
  period: number,
  bmuIds: Set<string>,
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  recordCount: number;
  totalVolume: number;
  totalPayment: number;
}> {
  logToConsoleAndFile(`Processing period ${period} for ${TARGET_DATE}...`);
  
  try {
    // Format the date as required by Elexon API (DD-MM-YYYY)
    const formattedDate = TARGET_DATE.split('-').reverse().join('-');
    
    // Fetch the data from Elexon API
    const response = await axios.get(ELEXON_API_URL, {
      params: {
        APIKey: "l2dmkqoqrk0vwky",
        ServiceType: "xml",
        Period: period,
        SettlementDate: formattedDate
      }
    });
    
    const xmlData = response.data;
    
    // Extract the relevant data points using regex (simplistic approach for demo)
    const bmuRecords = xmlData.match(/<BMUData>([\s\S]*?)<\/BMUData>/g) || [];
    
    let bmuCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    const batchSize = 50;
    const batchInserts = [];
    
    for (const bmuRecord of bmuRecords) {
      const bmuId = bmuRecord.match(/<NGC_BMU_ID>(.*?)<\/NGC_BMU_ID>/)?.[1];
      
      // Only process wind farms in our mapping
      if (!bmuId || !bmuIds.has(bmuId)) continue;
      
      const originalLevel = parseFloat(bmuRecord.match(/<OriginalLevel>(.*?)<\/OriginalLevel>/)?.[1] || "0");
      const finalLevel = parseFloat(bmuRecord.match(/<FinalLevel>(.*?)<\/FinalLevel>/)?.[1] || "0");
      const cadlFlag = bmuRecord.match(/<CADL_Flag>(.*?)<\/CADL_Flag>/)?.[1] === "Y";
      const soFlag = bmuRecord.match(/<SO_Flag>(.*?)<\/SO_Flag>/)?.[1] === "Y";
      
      // Calculate volume (negative means curtailment)
      const volume = originalLevel - finalLevel;
      
      // Only process records with curtailment
      if (volume <= 0) continue;
      
      // Get the original and final prices
      const originalPrice = parseFloat(bmuRecord.match(/<OriginalPrice>(.*?)<\/OriginalPrice>/)?.[1] || "0");
      const finalPrice = parseFloat(bmuRecord.match(/<FinalPrice>(.*?)<\/FinalPrice>/)?.[1] || "0");
      
      // Calculate payment (negative value since it's a cost)
      const payment = -1 * (volume * (finalPrice - originalPrice));
      
      // Get farm ID from BMU ID (use first part of BMU ID)
      const farmId = bmuId.split('-')[0];
      
      // Get lead party name from our mapping
      const leadPartyName = bmuLeadPartyMap.get(bmuId) || "Unknown";
      
      // Accumulate totals
      totalVolume += volume;
      totalPayment += payment;
      bmuCount++;
      
      // Add record to batch
      batchInserts.push({
        settlementDate: TARGET_DATE,
        settlementPeriod: period,
        cadlFlag,
        volume: volume,
        payment: payment,
        originalPrice: originalPrice,
        finalPrice: finalPrice,
        soFlag,
        farmId,
        leadPartyName,
        createdAt: new Date()
      });
      
      // Insert in batches to avoid large single queries
      if (batchInserts.length >= batchSize) {
        await db.insert(curtailmentRecords).values(batchInserts);
        batchInserts.length = 0;
      }
    }
    
    // Insert any remaining records
    if (batchInserts.length > 0) {
      await db.insert(curtailmentRecords).values(batchInserts);
    }
    
    logToConsoleAndFile(`[${TARGET_DATE} P${period}] Processed ${bmuCount} records, Volume: ${totalVolume.toFixed(2)} MWh, Payment: £${totalPayment.toFixed(2)}`);
    
    return {
      recordCount: bmuCount,
      totalVolume,
      totalPayment
    };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    logToConsoleAndFile(`ERROR processing period ${period}: ${(error as Error).message}`);
    throw error;
  }
}

// Update daily, monthly, and yearly summaries
async function updateSummaries(): Promise<void> {
  try {
    logToConsoleAndFile("Updating summaries...");
    
    // First, get the total curtailment for the day
    const dailyTotals = await db
      .select({
        totalVolume: sql<string>`SUM(${curtailmentRecords.volume}::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (!dailyTotals[0] || !dailyTotals[0].totalVolume) {
      throw new Error("Failed to calculate daily totals");
    }
    
    const totalCurtailedEnergy = Math.abs(parseFloat(dailyTotals[0].totalVolume));
    const totalPayment = parseFloat(dailyTotals[0].totalPayment);
    
    logToConsoleAndFile(`Daily totals - Energy: ${totalCurtailedEnergy.toFixed(2)} MWh, Payment: £${totalPayment.toFixed(2)}`);
    
    // Prepare for wind generation data (assuming 0 if not available)
    const windGeneration = { total: "0", onshore: "0", offshore: "0" };
    
    // See if we have existing wind generation data
    try {
      const existingWindData = await db.query.dailySummaries.findFirst({
        where: eq(dailySummaries.summaryDate, TARGET_DATE),
        columns: {
          totalWindGeneration: true,
          windOnshoreGeneration: true,
          windOffshoreGeneration: true
        }
      });
      
      if (existingWindData) {
        windGeneration.total = existingWindData.totalWindGeneration || "0";
        windGeneration.onshore = existingWindData.windOnshoreGeneration || "0";
        windGeneration.offshore = existingWindData.windOffshoreGeneration || "0";
      }
    } catch (error) {
      logToConsoleAndFile(`No existing wind generation data found: ${(error as Error).message}`);
    }
    
    // Update or insert daily summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totalCurtailedEnergy,
      totalPayment: totalPayment,
      totalWindGeneration: windGeneration.total,
      windOnshoreGeneration: windGeneration.onshore,
      windOffshoreGeneration: windGeneration.offshore,
      createdAt: new Date(),
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totalCurtailedEnergy,
        totalPayment: totalPayment,
        totalWindGeneration: windGeneration.total,
        windOnshoreGeneration: windGeneration.onshore,
        windOffshoreGeneration: windGeneration.offshore,
        lastUpdated: new Date()
      }
    });
    
    logToConsoleAndFile("Daily summary updated");
    
    // Update monthly summary
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM format
    
    // Calculate monthly totals from daily summaries
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`,
        totalWindGeneration: sql<string>`SUM(${dailySummaries.totalWindGeneration}::numeric)`,
        windOnshoreGeneration: sql<string>`SUM(${dailySummaries.windOnshoreGeneration}::numeric)`,
        windOffshoreGeneration: sql<string>`SUM(${dailySummaries.windOffshoreGeneration}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${yearMonth + '-01'}::date)`);
    
    if (!monthlyTotals[0] || !monthlyTotals[0].totalCurtailedEnergy) {
      throw new Error("Failed to calculate monthly totals");
    }
    
    // Update the monthly summary
    await db.insert(monthlySummaries).values({
      yearMonth,
      totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
      totalPayment: monthlyTotals[0].totalPayment,
      totalWindGeneration: monthlyTotals[0].totalWindGeneration || "0",
      windOnshoreGeneration: monthlyTotals[0].windOnshoreGeneration || "0",
      windOffshoreGeneration: monthlyTotals[0].windOffshoreGeneration || "0",
      createdAt: new Date(),
      updatedAt: new Date(),
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [monthlySummaries.yearMonth],
      set: {
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
        totalPayment: monthlyTotals[0].totalPayment,
        totalWindGeneration: monthlyTotals[0].totalWindGeneration || "0",
        windOnshoreGeneration: monthlyTotals[0].windOnshoreGeneration || "0",
        windOffshoreGeneration: monthlyTotals[0].windOffshoreGeneration || "0",
        updatedAt: new Date(),
        lastUpdated: new Date()
      }
    });
    
    logToConsoleAndFile(`Monthly summary updated for ${yearMonth}`);
    
    // Update yearly summary
    const year = TARGET_DATE.substring(0, 4);
    
    // Calculate yearly totals from daily summaries
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`,
        totalWindGeneration: sql<string>`SUM(${dailySummaries.totalWindGeneration}::numeric)`,
        windOnshoreGeneration: sql<string>`SUM(${dailySummaries.windOnshoreGeneration}::numeric)`,
        windOffshoreGeneration: sql<string>`SUM(${dailySummaries.windOffshoreGeneration}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${year + '-01-01'}::date)`);
    
    if (!yearlyTotals[0] || !yearlyTotals[0].totalCurtailedEnergy) {
      throw new Error("Failed to calculate yearly totals");
    }
    
    // Update the yearly summary
    await db.insert(yearlySummaries).values({
      year,
      totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
      totalPayment: yearlyTotals[0].totalPayment,
      totalWindGeneration: yearlyTotals[0].totalWindGeneration || "0",
      windOnshoreGeneration: yearlyTotals[0].windOnshoreGeneration || "0",
      windOffshoreGeneration: yearlyTotals[0].windOffshoreGeneration || "0",
      createdAt: new Date(),
      updatedAt: new Date(),
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [yearlySummaries.year],
      set: {
        totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
        totalPayment: yearlyTotals[0].totalPayment,
        totalWindGeneration: yearlyTotals[0].totalWindGeneration || "0",
        windOnshoreGeneration: yearlyTotals[0].windOnshoreGeneration || "0",
        windOffshoreGeneration: yearlyTotals[0].windOffshoreGeneration || "0",
        updatedAt: new Date(),
        lastUpdated: new Date()
      }
    });
    
    logToConsoleAndFile(`Yearly summary updated for ${year}`);
    
  } catch (error) {
    console.error("Error updating summaries:", error);
    logToConsoleAndFile(`ERROR updating summaries: ${(error as Error).message}`);
    throw error;
  }
}

// Update Bitcoin calculations
async function updateBitcoinCalculations(): Promise<void> {
  try {
    logToConsoleAndFile("Updating Bitcoin calculations...");
    
    // Import the Bitcoin processing service
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    // Run calculation for each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      await processSingleDay(TARGET_DATE, minerModel);
      logToConsoleAndFile(`Updated Bitcoin calculations for ${minerModel}`);
    }
    
  } catch (error) {
    console.error("Error updating Bitcoin calculations:", error);
    logToConsoleAndFile(`ERROR updating Bitcoin calculations: ${(error as Error).message}`);
  }
}

// Main function
async function main(): Promise<void> {
  const startTime = Date.now();
  
  logToConsoleAndFile(colorette.bold("=== Staged Reingest for March 21, 2025 ==="));
  logToConsoleAndFile(`Started at: ${new Date().toISOString()}`);
  logToConsoleAndFile(`Target date: ${TARGET_DATE}`);
  logToConsoleAndFile(`Processing periods: ${START_PERIOD} to ${END_PERIOD}`);
  
  try {
    // Load BMU mappings
    const { bmuIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Clear existing data for the periods
    await clearExistingPeriodsData();
    
    // Process each settlement period
    let totalRecordCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (let period = START_PERIOD; period <= END_PERIOD; period++) {
      try {
        const result = await processPeriod(period, bmuIds, bmuLeadPartyMap);
        totalRecordCount += result.recordCount;
        totalVolume += result.totalVolume;
        totalPayment += result.totalPayment;
        
        // Small delay to avoid overwhelming API
        await delay(500);
      } catch (error) {
        logToConsoleAndFile(`Failed to process period ${period}: ${(error as Error).message}`);
        // Continue with next period despite error
      }
    }
    
    logToConsoleAndFile(`Processed ${END_PERIOD - START_PERIOD + 1} periods with ${totalRecordCount} records`);
    logToConsoleAndFile(`Total Volume: ${totalVolume.toFixed(2)} MWh, Total Payment: £${totalPayment.toFixed(2)}`);
    
    // Update summaries if we've processed any data
    if (totalRecordCount > 0) {
      await updateSummaries();
      await updateBitcoinCalculations();
    }
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    logToConsoleAndFile(colorette.green(`Completed in ${duration} seconds`));
    
  } catch (error) {
    console.error("Error in main process:", error);
    logToConsoleAndFile(colorette.red(`ERROR in main process: ${(error as Error).message}`));
    process.exit(1);
  }
}

// Run the script
main().catch(error => {
  console.error("Uncaught error:", error);
  logToConsoleAndFile(`FATAL ERROR: ${error.message}`);
  process.exit(1);
});
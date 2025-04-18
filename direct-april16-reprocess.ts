/**
 * Direct Reprocessing Script for 2025-04-16
 * 
 * This script implements a direct approach to reprocessing curtailment data
 * with simplified error handling and fewer dependencies.
 * 
 * Run with: npx tsx direct-april16-reprocess.ts
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";
import axios from "axios";
import { processSingleDay } from "./server/services/bitcoinService";
import { minerModels } from "./server/types/bitcoin";

const TARGET_DATE = "2025-04-16";
const MINER_MODEL_KEYS = Object.keys(minerModels);
const REQUEST_TIMEOUT_MS = 60000; // 60 seconds

// Delay utility function
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Simple BMU mapping - hardcoded for brevity
const BMU_MAPPING = [
  { id: "T_MOWWO-1", leadParty: "Moray Offshore Windfarm (West) Limited" },
  { id: "T_MOWWO-4", leadParty: "Moray Offshore Windfarm (West) Limited" },
  { id: "T_GORDW-2", leadParty: "Gordonbush Extension Wind Farm Limited" },
  { id: "T_SGRWO-1", leadParty: "Seagreen Wind Energy Limited" },
  { id: "T_SGRWO-3", leadParty: "Seagreen Wind Energy Limited" },
  { id: "T_SGRWO-6", leadParty: "Seagreen Wind Energy Limited" },
  { id: "T_KTMWO-1", leadParty: "Kintyre Wind Limited" },
  { id: "T_BGLO-1", leadParty: "Beauly-Glendoe-A Limited" },
  { id: "T_WLNWO-1", leadParty: "Walney Extension Ltd" },
  { id: "T_WLNWO-2", leadParty: "Walney Extension Ltd" },
  { id: "T_AWGWO-3", leadParty: "Ajos WindGen GmbH" },
  { id: "T_FDGWO-2", leadParty: "Firth-District Wind Generation Limited" },
  { id: "E_KLBWO-2", leadParty: "Kilbraur Wind Energy Ltd" },
  { id: "E_BMLWO-1", leadParty: "Beinn Mhor Power Limited" },
  { id: "T_BDDWO-1", leadParty: "Beinn an Tuirc Wind 3 Limited" },
  { id: "T_DOVWO-1", leadParty: "Dorenell Wind Farm Limited" },
  { id: "T_HWLWO-1", leadParty: "Halsary Wind Farm Limited" },
  { id: "T_CLSWO-1", leadParty: "Clashindarroch Wind Farm Limited" },
  { id: "T_SLRWO-1", leadParty: "Solwaybank Energy Limited" },
  { id: "T_HKPWO-1", leadParty: "Heckenpfad GmbH" },
  { id: "T_STRWO-1", leadParty: "Strathy South Wind Farm Limited" },
  { id: "T_VDVWO-1", leadParty: "Vindeby Wind Limited" }
];

// Get lead party name for a BMU ID
function getLeadPartyName(bmuId: string): string {
  const mapping = BMU_MAPPING.find(item => item.id === bmuId);
  return mapping ? mapping.leadParty : "Unknown";
}

// Get list of all BMU IDs
function getAllBmuIds(): string[] {
  return BMU_MAPPING.map(item => item.id);
}

// Fetch bid/offer data from Elexon API
async function fetchBidsOffers(date: string, period: number): Promise<any[]> {
  try {
    console.log(`Fetching data for ${date} period ${period}...`);
    
    // Base URL for Elexon API
    const baseUrl = "https://data.elexon.co.uk/bmrs/api/v1";
    
    // Build URL for the request
    const url = `${baseUrl}/datasets/FORDAI/settlement-periods/${date}/${period}?format=json`;
    
    console.log(`API URL: ${url}`);
    
    // Make the request with increased timeout
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: REQUEST_TIMEOUT_MS  // Increased timeout
    });
    
    if (!response.data || !Array.isArray(response.data.data)) {
      console.error("Invalid response format");
      return [];
    }
    
    console.log(`Received ${response.data.data.length} records for period ${period}`);
    
    // Process API response
    return response.data.data.map((item: any) => ({
      settlementDate: date,
      settlementPeriod: period,
      id: item.bmUnit,
      volume: item.volume,
      soFlag: item.soFlag === "Y",
      cadlFlag: item.cadlFlag === "Y" ? true : (item.cadlFlag === "N" ? false : null),
      originalPrice: item.originalPrice,
      finalPrice: item.finalPrice
    }));
  } catch (error) {
    console.error(`Error fetching data for period ${period}:`, error);
    return [];
  }
}

// Process curtailment data for a specific period
async function processPeriod(period: number): Promise<{ records: number, volume: number, payment: number }> {
  try {
    console.log(`\nProcessing period ${period}/48...`);
    
    // Fetch data from Elexon
    const allRecords = await fetchBidsOffers(TARGET_DATE, period);
    
    // Filter to valid curtailment records
    const validBmuIds = getAllBmuIds();
    const validRecords = allRecords.filter(record => 
      record.volume < 0 &&
      (record.soFlag || record.cadlFlag) &&
      validBmuIds.includes(record.id)
    );
    
    if (validRecords.length === 0) {
      console.log(`No valid curtailment records for period ${period}`);
      return { records: 0, volume: 0, payment: 0 };
    }
    
    console.log(`Found ${validRecords.length} valid curtailment records for period ${period}`);
    
    // Process records
    let recordsInserted = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const record of validRecords) {
      // Calculate values
      const volume = Math.abs(record.volume);
      const payment = volume * record.originalPrice;
      
      try {
        // Insert record into database
        await db.insert(curtailmentRecords).values({
          settlementDate: TARGET_DATE,
          settlementPeriod: period,
          farmId: record.id,
          leadPartyName: getLeadPartyName(record.id),
          volume: record.volume.toString(),
          payment: payment.toString(),
          originalPrice: record.originalPrice.toString(),
          finalPrice: record.finalPrice.toString(),
          soFlag: record.soFlag,
          cadlFlag: record.cadlFlag
        });
        
        recordsInserted++;
        totalVolume += volume;
        totalPayment += payment;
        
        console.log(`[P${period}] Added record for ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
      } catch (error) {
        console.error(`[P${period}] Error inserting record for ${record.id}:`, error);
      }
    }
    
    console.log(`[P${period}] Summary: ${recordsInserted}/${validRecords.length} records, ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    
    return { records: recordsInserted, volume: totalVolume, payment: totalPayment };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return { records: 0, volume: 0, payment: 0 };
  }
}

// Main function to process all periods
async function processAllPeriods(): Promise<void> {
  console.log(`\n=== Starting Direct Reprocessing for ${TARGET_DATE} ===\n`);
  const startTime = new Date();
  
  // Delete existing records first
  try {
    console.log(`Deleting existing curtailment records for ${TARGET_DATE}...`);
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  } catch (error) {
    console.error("Error deleting existing records:", error);
  }
  
  // Process each period
  let totalRecords = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  
  for (let period = 1; period <= 48; period++) {
    try {
      const result = await processPeriod(period);
      
      totalRecords += result.records;
      totalVolume += result.volume;
      totalPayment += result.payment;
      
      // Add a small delay between periods to avoid overwhelming the API
      if (period < 48) {
        await delay(1000);
      }
    } catch (error) {
      console.error(`Error in period ${period}:`, error);
    }
  }
  
  // Update daily summary
  try {
    console.log(`\nUpdating daily summary for ${TARGET_DATE}...`);
    
    // Delete existing summary
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    // Count distinct periods and farms
    const countResult = await db.select({
      periodCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      farmCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.farmId})`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Insert new summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totalVolume,
      totalPayment: -totalPayment, // Use negative payment as per schema
      periodCount: Number(countResult[0]?.periodCount || 0),
      farmCount: Number(countResult[0]?.farmCount || 0),
      recordCount: totalRecords,
      lastUpdated: new Date()
    });
    
    console.log(`Daily summary updated for ${TARGET_DATE}`);
  } catch (error) {
    console.error("Error updating daily summary:", error);
  }
  
  // Final verification
  try {
    const verification = await db.select({
      record_count: sql<string>`COUNT(*)`,
      period_count: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      farm_count: sql<string>`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
      total_volume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      total_payment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\n=== Verification ===`);
    console.log(`Records in database: ${verification[0].record_count}`);
    console.log(`Periods in database: ${verification[0].period_count}`);
    console.log(`Farms in database: ${verification[0].farm_count}`);
    console.log(`Total volume in database: ${Number(verification[0].total_volume).toFixed(2)} MWh`);
    console.log(`Total payment in database: £${Number(verification[0].total_payment).toFixed(2)}`);
  } catch (error) {
    console.error("Error during verification:", error);
  }
  
  // Process Bitcoin calculations
  try {
    console.log(`\n=== Processing Bitcoin Calculations ===`);
    
    for (const minerModel of MINER_MODEL_KEYS) {
      try {
        console.log(`Processing ${minerModel}...`);
        const result = await processSingleDay(TARGET_DATE, minerModel);
        if (result && result.success) {
          console.log(`✓ Successfully processed ${minerModel}: ${result.bitcoinMined.toFixed(8)} BTC (£${result.valueGbp.toFixed(2)})`);
        } else {
          console.log(`No calculations for ${minerModel}`);
        }
      } catch (error) {
        console.error(`Error processing Bitcoin for ${minerModel}:`, error);
      }
    }
  } catch (error) {
    console.error("Error processing Bitcoin calculations:", error);
  }
  
  // Calculate execution time
  const endTime = new Date();
  const executionTimeMs = endTime.getTime() - startTime.getTime();
  console.log(`\n=== Reprocessing Completed ===`);
  console.log(`Total execution time: ${(executionTimeMs / 1000).toFixed(2)} seconds`);
}

// Run the main function
processAllPeriods().then(() => {
  console.log("Processing completed");
  process.exit(0);
}).catch(error => {
  console.error("Unexpected error:", error);
  process.exit(1);
});
/**
 * Test Reingest for March 21, 2025
 * 
 * This script processes 4 settlement periods for March 21, 2025
 * as a quick test to ensure the reingestion process is working correctly.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, and, sql, between } from "drizzle-orm";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TARGET_DATE = '2025-03-21';
const START_PERIOD = 1;
const END_PERIOD = 4; // Just test 4 periods
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");
const ELEXON_API_URL = "https://api.bmreports.com/BMRS/BOALF/V1";

// Utility function to delay between API calls
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mapping to get valid wind farm IDs
async function loadBmuMappings(): Promise<{
  bmuIds: Set<string>;
  bmuLeadPartyMap: Map<string, string>;
}> {
  try {
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    
    const bmuIds = new Set<string>();
    const bmuLeadPartyMap = new Map<string, string>();
    
    for (const mapping of bmuMapping) {
      if (mapping.nationalGridBmUnit) {
        bmuIds.add(mapping.nationalGridBmUnit);
        bmuLeadPartyMap.set(mapping.nationalGridBmUnit, mapping.leadPartyName || 'Unknown');
      }
      if (mapping.elexonBmUnit) {
        bmuIds.add(mapping.elexonBmUnit);
        bmuLeadPartyMap.set(mapping.elexonBmUnit, mapping.leadPartyName || 'Unknown');
      }
    }
    
    console.log(`Loaded ${bmuIds.size} wind farm BMU IDs`);
    return { bmuIds, bmuLeadPartyMap };
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

// First clear the existing data for specific periods
async function clearExistingPeriodsData(): Promise<void> {
  console.log(`Clearing data for periods ${START_PERIOD}-${END_PERIOD} on ${TARGET_DATE}...`);
  
  // First, delete from curtailment_records for specific periods
  const deleteResult = await db.delete(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        between(curtailmentRecords.settlementPeriod, START_PERIOD, END_PERIOD)
      )
    );
  
  console.log(`Deleted ${deleteResult.rowCount} curtailment records for periods ${START_PERIOD}-${END_PERIOD}`);
  return;
}

// Process a single settlement period
async function processPeriod(
  period: number, 
  bmuIds: Set<string>, 
  bmuLeadPartyMap: Map<string, string>
): Promise<{
  volume: number;
  payment: number;
  recordCount: number;
}> {
  console.log(`Processing period ${period} for ${TARGET_DATE}...`);
  
  try {
    // Format the date as required by Elexon API (DD-MM-YYYY)
    const formattedDate = TARGET_DATE.split('-').reverse().join('-');
    
    // Fetch data from Elexon API
    const response = await axios.get(ELEXON_API_URL, {
      params: {
        APIKey: "l2dmkqoqrk0vwky",
        ServiceType: "xml",
        Period: period,
        SettlementDate: formattedDate
      }
    });
    
    const xmlData = response.data;
    
    // Extract the relevant data points using regex
    const bmuRecords = xmlData.match(/<BMUData>([\s\S]*?)<\/BMUData>/g) || [];
    
    let recordCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
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
      
      // Calculate payment (note: using originalPrice directly)
      const payment = volume * originalPrice;
      
      // Get farm ID from BMU ID
      const farmId = bmuId;
      
      // Get lead party name from our mapping
      const leadPartyName = bmuLeadPartyMap.get(bmuId) || "Unknown";
      
      try {
        await db.insert(curtailmentRecords).values({
          settlementDate: TARGET_DATE,
          settlementPeriod: period,
          farmId,
          leadPartyName,
          volume: volume * -1, // Store as negative value for curtailment
          payment,
          originalPrice,
          finalPrice,
          soFlag,
          cadlFlag
        });
        
        // Add to totals (using absolute value for volume)
        totalVolume += volume;
        totalPayment += payment;
        recordCount++;
        
        console.log(`[${TARGET_DATE} P${period}] Added record for ${farmId}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
      } catch (error) {
        console.error(`Error inserting record for ${bmuId}:`, error);
      }
    }
    
    console.log(`[${TARGET_DATE} P${period}] Total: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    
    return { 
      volume: totalVolume, 
      payment: totalPayment,
      recordCount
    };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return { volume: 0, payment: 0, recordCount: 0 };
  }
}

// Update daily summary
async function updateDailySummary(): Promise<void> {
  try {
    console.log(`Updating summary for ${TARGET_DATE}...`);
    
    // Calculate totals from curtailment records
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (!totals[0] || !totals[0].totalCurtailedEnergy) {
      console.log('No curtailment records found for the summary update.');
      return;
    }
    
    // Get existing wind generation data
    const existingData = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, TARGET_DATE),
      columns: {
        totalWindGeneration: true,
        windOnshoreGeneration: true,
        windOffshoreGeneration: true
      }
    });
    
    // Update daily summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totals[0].totalCurtailedEnergy,
      totalPayment: totals[0].totalPayment,
      totalWindGeneration: existingData?.totalWindGeneration || '0',
      windOnshoreGeneration: existingData?.windOnshoreGeneration || '0',
      windOffshoreGeneration: existingData?.windOffshoreGeneration || '0',
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
  } catch (error) {
    console.error('Error updating daily summary:', error);
  }
}

// Main function
async function main(): Promise<void> {
  const startTime = Date.now();
  
  console.log(`=== Test Reingest for March 21, 2025 (Periods ${START_PERIOD}-${END_PERIOD}) ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    // Step 1: Load BMU mappings
    const { bmuIds, bmuLeadPartyMap } = await loadBmuMappings();
    
    // Step 2: Clear existing data for specific periods
    await clearExistingPeriodsData();
    
    // Step 3: Process periods
    let totalVolume = 0;
    let totalPayment = 0;
    let totalRecords = 0;
    
    for (let period = START_PERIOD; period <= END_PERIOD; period++) {
      const result = await processPeriod(period, bmuIds, bmuLeadPartyMap);
      totalVolume += result.volume;
      totalPayment += result.payment;
      totalRecords += result.recordCount;
      
      // Small delay to avoid API rate limits
      if (period < END_PERIOD) {
        await delay(500);
      }
    }
    
    console.log(`\nProcessed ${END_PERIOD - START_PERIOD + 1} periods with ${totalRecords} records`);
    console.log(`Total Volume: ${totalVolume.toFixed(2)} MWh, Total Payment: £${totalPayment.toFixed(2)}`);
    
    // Step 4: Update daily summary 
    await updateDailySummary();
    
    // Step 5: Verify the updated data
    const totalReingested = await db
      .select({
        periodCount: sql`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        recordCount: sql`COUNT(*)`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nCurrent Status for ${TARGET_DATE}:`);
    console.log(`- Total Periods with Data: ${totalReingested[0].periodCount}`);
    console.log(`- Total Records: ${totalReingested[0].recordCount}`);
    console.log(`- Total Volume: ${totalReingested[0].totalVolume} MWh`);
    console.log(`- Total Payment: £${totalReingested[0].totalPayment}`);
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\nTest reingestion completed in ${duration} seconds`);
    
  } catch (error) {
    console.error('Error in main process:', error);
    process.exit(1);
  }
}

// Run the script
main().catch(error => {
  console.error("Uncaught error:", error);
  process.exit(1);
});
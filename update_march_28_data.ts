/**
 * Update March 28, 2025 Data
 * 
 * This script is designed to reingest and process curtailment data specifically for
 * March 28, 2025, ensuring all 48 settlement periods are queried from the Elexon API.
 * Currently, the database only has data for period 21 (10:00 hour), which makes
 * the hourly breakdown chart show curtailment only for that hour.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';
import { format } from 'date-fns';
import { processDailyCurtailment } from "./server/services/curtailment";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TARGET_DATE = '2025-03-28';
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");
const API_RATE_LIMIT = 200; // 200ms between requests to stay well under rate limits

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
    
    // Use type assertion for proper typing
    const windFarmIds = new Set<string>(
      bmuMapping.map((bmu: { elexonBmUnit: string }) => bmu.elexonBmUnit)
    );
    
    const bmuLeadPartyMap = new Map<string, string>();
    for (const bmu of bmuMapping as Array<{ elexonBmUnit: string, leadPartyName: string }>) {
      bmuLeadPartyMap.set(bmu.elexonBmUnit, bmu.leadPartyName);
    }
    
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    return { windFarmIds, bmuLeadPartyMap };
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

// Clear existing data for the target date
async function clearExistingData(): Promise<void> {
  console.log(`Clearing existing data for ${TARGET_DATE}...`);
  
  // Delete from curtailment_records first (parent table)
  const deletedRecords = await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .returning({ id: curtailmentRecords.id });
  
  console.log(`Deleted ${deletedRecords.length} existing curtailment records`);
  
  // Delete from daily_summaries (child table)
  await db.delete(dailySummaries)
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
  
  console.log(`Deleted daily summary record for ${TARGET_DATE}`);
}

// Process all settlement periods for the target date
async function processAllPeriods(): Promise<void> {
  console.log(`Started processing all periods for ${TARGET_DATE}`);
  
  // Use the existing service to process the date
  try {
    await processDailyCurtailment(TARGET_DATE);
    console.log(`Successfully processed all periods for ${TARGET_DATE}`);
  } catch (error) {
    console.error('Error processing periods:', error);
    throw error;
  }
}

// Run Bitcoin calculations for the date
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

// Verify the update with stats
async function verifyUpdate(): Promise<void> {
  console.log(`Verifying update for ${TARGET_DATE}...`);
  
  const stats = await db
    .select({
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  console.log('Update verification results:');
  console.log(`- Records: ${stats[0]?.recordCount || 0}`);
  console.log(`- Periods: ${stats[0]?.periodCount || 0}`);
  console.log(`- Volume: ${Number(stats[0]?.totalVolume || 0).toFixed(2)} MWh`);
  console.log(`- Payment: £${Number(stats[0]?.totalPayment || 0).toFixed(2)}`);
}

// Main function
async function main(): Promise<void> {
  console.log(`=== March 28, 2025 Data Update ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    // Step 1: Clear existing data
    await clearExistingData();
    
    // Step 2: Process all settlement periods
    await processAllPeriods();
    
    // Step 3: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 4: Verify the update
    await verifyUpdate();
    
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
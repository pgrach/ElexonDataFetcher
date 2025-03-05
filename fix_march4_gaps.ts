/**
 * Fix March 4, 2025 Data Gaps
 * 
 * This script uses direct SQL to add the missing periods for March 4, 2025.
 * It handles each period separately to avoid timing out.
 */

import { db } from "./db";
import { eq } from "drizzle-orm";
import { curtailmentRecords } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { processSingleDay } from "./server/services/bitcoinService";

const TARGET_DATE = '2025-03-04';
const MISSING_PERIODS = [16, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48];
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Function to process a single period
async function processOnePeriod(date: string, period: number): Promise<boolean> {
  try {
    console.log(`Processing period ${period} for ${date}...`);
    
    // First check if we already have records for this period
    const existingRecords = await db
      .select({ count: db.func.count() })
      .from(curtailmentRecords)
      .where(
        eq(curtailmentRecords.settlementDate, date) && 
        eq(curtailmentRecords.settlementPeriod, period)
      );
    
    const recordCount = Number(existingRecords[0]?.count || 0);
    if (recordCount > 0) {
      console.log(`Period ${period} already has ${recordCount} records. Skipping.`);
      return true;
    }
    
    // Fetch records from Elexon API
    console.log(`Fetching data from Elexon API for period ${period}...`);
    const bidOffers = await fetchBidsOffers(date, period);
    
    if (bidOffers.length === 0) {
      console.log(`No records found for period ${period}.`);
      return true; // Consider it processed even if empty
    }
    
    // Insert records directly
    console.log(`Adding ${bidOffers.length} records for period ${period}...`);
    for (const record of bidOffers) {
      const payment = Math.abs(record.volume) * record.originalPrice;
      
      await db.insert(curtailmentRecords).values({
        settlementDate: date,
        settlementPeriod: period,
        farmId: record.id,
        volume: record.volume.toString(),
        originalPrice: record.originalPrice.toString(),
        payment: (-payment).toString(), // Negative for payments
        leadPartyName: record.leadPartyName || null,
        soFlag: record.soFlag || false,
        cadlFlag: record.cadlFlag || false
      });
    }
    
    console.log(`Successfully added ${bidOffers.length} records for period ${period}`);
    return true;
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return false;
  }
}

// Update Bitcoin calculations after adding records
async function updateBitcoinCalculations(date: string): Promise<void> {
  try {
    console.log(`\nUpdating Bitcoin calculations for ${date}...`);
    
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing ${minerModel}...`);
      await processSingleDay(date, minerModel);
    }
    
    console.log(`Bitcoin calculations updated for ${date}`);
  } catch (error) {
    console.error(`Error updating Bitcoin calculations:`, error);
  }
}

// Main function
async function main() {
  try {
    console.log(`\n=== Fixing Missing Periods for ${TARGET_DATE} ===\n`);
    
    const results = [];
    // Process one period at a time
    for (const period of MISSING_PERIODS) {
      const success = await processOnePeriod(TARGET_DATE, period);
      results.push({ period, success });
    }
    
    // Display summary
    console.log("\n--- Processing Summary ---");
    results.forEach(result => {
      console.log(`Period ${result.period}: ${result.success ? '✅ Success' : '❌ Failed'}`);
    });
    
    // Count periods after processing
    const periodsAfter = await db
      .select({ periods: db.func.count() })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod);
    
    console.log(`\nAfter processing: ${periodsAfter.length}/48 periods covered`);
    
    // Update Bitcoin calculations if needed
    if (results.some(r => r.success)) {
      await updateBitcoinCalculations(TARGET_DATE);
    }
    
    console.log("\n=== Processing Complete ===\n");
  } catch (error) {
    console.error('Error during processing:', error);
  }
}

// Run the script
main();
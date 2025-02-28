/**
 * Demonstration script to fix Bitcoin calculations for a specific date range
 */

import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { sql, and, eq, between } from "drizzle-orm";
import { processSingleDay } from "./server/services/bitcoinService";
import { minerModels } from "./server/types/bitcoin";

// Configuration - process a small date range for demonstration
const START_DATE = "2025-01-01";
const END_DATE = "2025-01-05";
const MINER_MODEL_LIST = Object.keys(minerModels);

/**
 * Fix Bitcoin calculations for a specific date
 */
async function fixDate(date: string): Promise<void> {
  console.log(`\nProcessing date: ${date}`);
  
  // Get curtailment record count for validation
  const curtailmentResult = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  const curtailmentCount = curtailmentResult[0]?.count || 0;
  console.log(`Found ${curtailmentCount} curtailment records`);
  
  if (curtailmentCount === 0) {
    console.log("No curtailment records to process");
    return;
  }
  
  // Process each miner model
  for (const model of MINER_MODEL_LIST) {
    try {
      console.log(`Processing model: ${model}`);
      await processSingleDay(date, model);
      
      // Verify the records were created
      const bitcoinResult = await db
        .select({ count: sql<number>`COUNT(*)` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          eq(historicalBitcoinCalculations.minerModel, model)
        ));
      
      const bitcoinCount = bitcoinResult[0]?.count || 0;
      console.log(`Created ${bitcoinCount} Bitcoin calculations (${Math.round((bitcoinCount/curtailmentCount)*100)}% complete)`);
    } catch (error) {
      console.error(`Error processing ${model} for ${date}:`, error);
    }
  }
}

/**
 * Get dates within the specified range
 */
async function getDatesInRange(): Promise<string[]> {
  const result = await db.execute(sql`
    SELECT DISTINCT settlement_date::text as date
    FROM curtailment_records
    WHERE settlement_date BETWEEN ${START_DATE} AND ${END_DATE}
    ORDER BY settlement_date
  `);
  
  return result.rows.map(row => row.date);
}

/**
 * Main function to demonstrate the fix
 */
async function main() {
  console.log("=== Bitcoin Calculation Fix Demonstration ===");
  console.log(`Date range: ${START_DATE} to ${END_DATE}`);
  
  // Get all dates in the range
  const dates = await getDatesInRange();
  console.log(`Found ${dates.length} dates with curtailment records`);
  
  // Process each date
  for (const date of dates) {
    await fixDate(date);
  }
  
  console.log("\n=== Demonstration Complete ===");
}

main()
  .then(() => {
    console.log("Processing finished successfully");
    process.exit(0);
  })
  .catch(error => {
    console.error("Error during processing:", error);
    process.exit(1);
  });
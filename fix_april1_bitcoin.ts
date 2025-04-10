/**
 * Fix Bitcoin calculations for April 1, 2025
 * 
 * This script removes and recalculates Bitcoin mining calculations for 2025-04-01
 * using data from the curtailment_records table.
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import { processSingleDay } from "./server/services/bitcoinService";

// Target date for processing
const TARGET_DATE = "2025-04-01";

// Miner models to process
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

async function main() {
  try {
    console.log(`\n===== FIXING BITCOIN CALCULATIONS FOR ${TARGET_DATE} =====\n`);

    // Step 1: Analyze current state
    const curtailmentStats = await db.select({
      totalRecords: sql<number>`COUNT(*)::int`,
      totalPeriods: sql<number>`COUNT(DISTINCT settlement_period)::int`,
      totalFarms: sql<number>`COUNT(DISTINCT farm_id)::int`,
      totalEnergy: sql<string>`SUM(ABS(volume))::text`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));

    console.log(`Curtailment records for ${TARGET_DATE}:`, {
      records: curtailmentStats[0].totalRecords,
      periods: curtailmentStats[0].totalPeriods,
      farms: curtailmentStats[0].totalFarms,
      energy: Number(curtailmentStats[0].totalEnergy).toFixed(2) + " MWh"
    });

    // Check existing Bitcoin calculations before deletion
    for (const minerModel of MINER_MODELS) {
      const existingCalcs = await db.select({
        totalRecords: sql<number>`COUNT(*)::int`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`,
        difficulty: sql<string>`DISTINCT(difficulty::numeric)::text`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );

      console.log(`Current ${minerModel} calculations:`, {
        records: existingCalcs[0]?.totalRecords || 0,
        bitcoin: existingCalcs[0]?.totalBitcoin || "0",
        difficulty: existingCalcs[0]?.difficulty || "N/A"
      });
    }

    // Step 2: Process each miner model using the Bitcoin service
    console.log("\n=== Starting Bitcoin Recalculation ===\n");
    
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing ${minerModel}...`);
      
      // The processSingleDay function will:
      // 1. Delete existing calculations for this date and model
      // 2. Fetch the correct difficulty from DynamoDB
      // 3. Calculate Bitcoin for all curtailment records
      // 4. Update the daily summary table
      // 5. Update monthly and yearly summaries
      await processSingleDay(TARGET_DATE, minerModel);
      
      // Verify the new calculations
      const newCalcs = await db.select({
        totalRecords: sql<number>`COUNT(*)::int`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`,
        difficulty: sql<string>`DISTINCT(difficulty::numeric)::text`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
      
      console.log(`Updated ${minerModel} calculations:`, {
        records: newCalcs[0]?.totalRecords || 0,
        bitcoin: newCalcs[0]?.totalBitcoin || "0",
        difficulty: newCalcs[0]?.difficulty || "N/A"
      });
    }
    
    console.log("\n===== BITCOIN CALCULATIONS FIX COMPLETED =====\n");
    console.log("April 1, 2025 Bitcoin mining potential has been recalculated");
    console.log("Summary tables (daily, monthly, yearly) have been updated");
    
    process.exit(0);
  } catch (error) {
    console.error("Error fixing Bitcoin calculations:", error);
    process.exit(1);
  }
}

// Execute the main function
main().catch(error => {
  console.error("Unhandled error:", error);
  process.exit(1);
});
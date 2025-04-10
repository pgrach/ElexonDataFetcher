/**
 * Fix Bitcoin calculations for April 1, 2025
 * 
 * This script addresses an issue where only a small portion of curtailment records
 * were processed for April 1, 2025 Bitcoin calculations. The fix includes:
 * 
 * 1. Recalculating Bitcoin for all miner models (S19J_PRO, S9, M20S)
 * 2. Ensuring all 544 curtailment records are processed
 * 3. Updating monthly and yearly summaries
 */

import { db } from "../db";
import { 
  historicalBitcoinCalculations, 
  curtailmentRecords,
  bitcoinDailySummaries,
  bitcoinMonthlySummaries
} from "../db/schema";
import { eq, and, sql } from "drizzle-orm";
import { processSingleDay } from "../server/services/bitcoinService";

// Target date for processing
const TARGET_DATE = "2025-04-01";

// Miner models to process
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

async function main() {
  try {
    console.log(`\n===== FIXING BITCOIN CALCULATIONS FOR ${TARGET_DATE} =====\n`);

    // Analyze current state
    const curtailmentStats = await db.select({
      totalRecords: sql<number>`COUNT(*)::int`,
      totalPeriods: sql<number>`COUNT(DISTINCT settlement_period)::int`,
      totalEnergy: sql<string>`SUM(ABS(volume))::text`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));

    console.log(`Curtailment records for ${TARGET_DATE}:`, {
      records: curtailmentStats[0].totalRecords,
      periods: curtailmentStats[0].totalPeriods,
      energy: Number(curtailmentStats[0].totalEnergy).toFixed(2) + " MWh"
    });

    // Check existing Bitcoin calculations
    for (const minerModel of MINER_MODELS) {
      const existingCalcs = await db.select({
        totalRecords: sql<number>`COUNT(*)::int`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
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
        bitcoin: existingCalcs[0]?.totalBitcoin || "0"
      });
    }

    // Process each miner model
    console.log("\n=== Starting Recalculation ===\n");
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing ${minerModel}...`);
      
      // This function from bitcoinService will:
      // 1. Delete existing calculations
      // 2. Pull the correct difficulty from DynamoDB
      // 3. Calculate Bitcoin for all curtailment records
      // 4. Update the monthly and yearly summaries
      await processSingleDay(TARGET_DATE, minerModel);
      
      // Verify the new calculations
      const newCalcs = await db.select({
        totalRecords: sql<number>`COUNT(*)::int`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
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
        bitcoin: newCalcs[0]?.totalBitcoin || "0"
      });
    }
    
    console.log("\n=== Final Verification ===\n");
    
    // Verify Bitcoin in daily/monthly/yearly summaries
    const dailySummaries = await db.select({
      totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
    })
    .from(bitcoinDailySummaries)
    .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`Daily summary for ${TARGET_DATE}:`, {
      bitcoin: dailySummaries[0]?.totalBitcoin || "Not found"
    });
    
    const monthSummaries = await db.select({
      totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
    })
    .from(bitcoinMonthlySummaries)
    .where(eq(bitcoinMonthlySummaries.yearMonth, TARGET_DATE.substring(0, 7)));
    
    console.log(`Monthly summary for ${TARGET_DATE.substring(0, 7)}:`, {
      bitcoin: monthSummaries[0]?.totalBitcoin || "Not found"
    });
    
    console.log("\n===== FIX COMPLETED =====");
    console.log("All Bitcoin calculations have been updated for April 1, 2025");
    
    process.exit(0);
  } catch (error) {
    console.error("ERROR FIXING BITCOIN CALCULATIONS:", error);
    process.exit(1);
  }
}

// Fix already imported above

// Run the fix
main()
  .catch(error => {
    console.error("UNHANDLED ERROR:", error);
    process.exit(1);
  });
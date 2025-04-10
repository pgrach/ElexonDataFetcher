/**
 * Fix Bitcoin calculations for April 1, 2025
 * 
 * This script addresses the issue where only a small portion of curtailment records
 * were processed for Bitcoin calculations on April 1, 2025.
 */

import { db } from "../db";
import { 
  historicalBitcoinCalculations, 
  curtailmentRecords,
  bitcoinDailySummaries,
  bitcoinMonthlySummaries
} from "../db/schema";
import { eq, and, sql } from "drizzle-orm";
import { calculateBitcoin } from "../server/utils/bitcoin";
import { getDifficultyData } from "../server/services/dynamodbService";

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
    console.log("\n=== Starting Bitcoin Recalculation ===\n");
    
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing ${minerModel}...`);
      
      // Get the difficulty value for this date
      const difficulty = await getDifficultyData(TARGET_DATE);
      console.log(`Using difficulty ${difficulty} for ${TARGET_DATE}`);
      
      // Delete existing calculations to avoid duplicates
      await db.delete(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      console.log(`Deleted existing calculations for ${TARGET_DATE} and ${minerModel}`);
      
      // Get all curtailment records for this date
      const records = await db.select({
        settlementPeriod: curtailmentRecords.settlementPeriod,
        farmId: curtailmentRecords.farmId,
        leadPartyName: curtailmentRecords.leadPartyName,
        volume: curtailmentRecords.volume
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
      console.log(`Processing ${records.length} curtailment records`);
      
      // Calculate Bitcoin for each curtailment record
      let totalBitcoin = 0;
      const insertPromises = [];
      
      for (const record of records) {
        // Convert volume (MWh) to positive number for calculation
        const mwh = Math.abs(Number(record.volume));
        
        // Skip records with zero or invalid volume
        if (mwh <= 0 || isNaN(mwh)) {
          continue;
        }
        
        // Calculate Bitcoin mined
        const bitcoinMined = calculateBitcoin(mwh, minerModel, difficulty);
        totalBitcoin += bitcoinMined;
        
        // Insert the calculation record
        insertPromises.push(
          db.insert(historicalBitcoinCalculations).values({
            settlementDate: TARGET_DATE,
            settlementPeriod: Number(record.settlementPeriod),
            minerModel: minerModel,
            farmId: record.farmId,
            bitcoinMined: bitcoinMined.toString(),
            difficulty: difficulty.toString()
          })
        );
      }
      
      // Execute all inserts
      await Promise.all(insertPromises);
      
      console.log(`Successfully processed ${insertPromises.length} Bitcoin calculations`);
      console.log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
      
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
    
    // Update monthly summary for April 2025
    console.log("\n=== Updating Monthly Summary ===\n");
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    
    for (const minerModel of MINER_MODELS) {
      // Query the historical Bitcoin calculations for the month
      const result = await db.execute(sql`
        SELECT
          SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
          COUNT(DISTINCT settlement_date) as days_count
        FROM
          historical_bitcoin_calculations
        WHERE
          settlement_date >= ${yearMonth + '-01'}
          AND settlement_date <= ${yearMonth + '-30'}
          AND miner_model = ${minerModel}
      `);
      
      const data = result[0] as any;
      
      if (!data || !data.total_bitcoin) {
        console.log(`No Bitcoin data found for ${yearMonth} and ${minerModel}`);
        continue;
      }
      
      // Delete existing summary if any
      await db.execute(sql`
        DELETE FROM bitcoin_monthly_summaries
        WHERE year_month = ${yearMonth}
        AND miner_model = ${minerModel}
      `);
      
      // Insert new summary
      await db.insert(bitcoinMonthlySummaries).values({
        yearMonth: yearMonth,
        minerModel: minerModel,
        bitcoinMined: data.total_bitcoin.toString(),
        updatedAt: new Date()
      });
      
      console.log(`Monthly Bitcoin summary updated for ${yearMonth} and ${minerModel}: ${data.total_bitcoin} BTC`);
    }
    
    console.log("\n===== FIX COMPLETED =====");
    console.log("All Bitcoin calculations have been updated for April 1, 2025");
    
    process.exit(0);
  } catch (error) {
    console.error("ERROR FIXING BITCOIN CALCULATIONS:", error);
    process.exit(1);
  }
}

// Run the fix
main()
  .catch(error => {
    console.error("UNHANDLED ERROR:", error);
    process.exit(1);
  });
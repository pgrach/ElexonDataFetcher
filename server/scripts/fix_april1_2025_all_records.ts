/**
 * Fix April 1, 2025 Bitcoin Calculations
 * 
 * This script addresses an issue where not all curtailment records for April 1, 2025
 * were processed for Bitcoin mining calculations. The fix includes:
 * 
 * 1. Properly calculating Bitcoin for all 544 curtailment records
 * 2. Using the correct difficulty value (113757508810853)
 * 3. Updating all miner models (S19J_PRO, S9, M20S)
 * 4. Updating the monthly summaries
 */

import { db } from "../../db";
import { 
  historicalBitcoinCalculations, 
  curtailmentRecords,
  bitcoinMonthlySummaries,
  bitcoinDailySummaries
} from "../../db/schema";
import { eq, and, sql } from "drizzle-orm";
import { calculateBitcoin } from "../utils/bitcoin";

// Target date for processing
const TARGET_DATE = "2025-04-01";

// Miner models to process
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

// Known difficulty value from database
const DIFFICULTY = 113757508810853;

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

    // Get all curtailment records for April 1, 2025
    const records = await db.select({
      settlementPeriod: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      leadPartyName: curtailmentRecords.leadPartyName,
      volume: curtailmentRecords.volume
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Found ${records.length} curtailment records to process`);
    
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      console.log(`\n--- Processing ${minerModel} ---`);
      
      // Check existing calculations
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
      
      // To be extra careful with unique constraint violations, 
      // let's get the exact records we've already processed
      const existingRecordKeys = await db.select({
        settlementPeriod: historicalBitcoinCalculations.settlementPeriod,
        farmId: historicalBitcoinCalculations.farmId
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
      
      // Create a set of keys for fast lookup
      const processedKeys = new Set();
      for (const record of existingRecordKeys) {
        const key = `${record.settlementPeriod}:${record.farmId}`;
        processedKeys.add(key);
      }
      
      // Delete existing calculations to avoid duplicates
      await db.delete(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      console.log(`Deleted ${existingRecordKeys.length} existing ${minerModel} calculations`);
      
      // Calculate Bitcoin for each curtailment record
      let totalBitcoin = 0;
      const batchSize = 50;
      let currentBatch = [];
      
      for (const record of records) {
        // Convert volume (MWh) to positive number for calculation
        const mwh = Math.abs(Number(record.volume));
        
        // Skip records with zero or invalid volume
        if (mwh <= 0 || isNaN(mwh)) {
          continue;
        }
        
        // Calculate Bitcoin mined with the correct difficulty
        const bitcoinMined = calculateBitcoin(mwh, minerModel, DIFFICULTY);
        totalBitcoin += bitcoinMined;
        
        // Add to current batch
        currentBatch.push({
          settlementDate: TARGET_DATE,
          settlementPeriod: Number(record.settlementPeriod),
          minerModel: minerModel,
          farmId: record.farmId,
          bitcoinMined: bitcoinMined.toString(),
          difficulty: DIFFICULTY.toString()
        });
        
        // Insert in batches to avoid memory issues
        if (currentBatch.length >= batchSize) {
          await db.insert(historicalBitcoinCalculations).values(currentBatch);
          console.log(`Inserted batch of ${currentBatch.length} records`);
          currentBatch = [];
        }
      }
      
      // Insert any remaining records
      if (currentBatch.length > 0) {
        await db.insert(historicalBitcoinCalculations).values(currentBatch);
        console.log(`Inserted final batch of ${currentBatch.length} records`);
      }
      
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
    
    // Update daily summaries for April 1
    console.log("\n--- Updating Daily Summaries ---");
    
    // Delete existing daily summaries for April 1 (if any)
    await db.delete(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
    
    // Insert updated daily summaries for each miner model
    for (const minerModel of MINER_MODELS) {
      const dailyTotal = await db.select({
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
      
      if (dailyTotal[0]?.totalBitcoin) {
        await db.insert(bitcoinDailySummaries).values({
          summaryDate: TARGET_DATE,
          minerModel: minerModel,
          bitcoinMined: dailyTotal[0].totalBitcoin,
          updatedAt: new Date()
        });
        
        console.log(`Updated daily summary for ${minerModel}: ${dailyTotal[0].totalBitcoin} BTC`);
      }
    }
    
    // Update monthly summaries for April 2025
    console.log("\n--- Updating Monthly Summaries ---");
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    
    for (const minerModel of MINER_MODELS) {
      // Query all Bitcoin calculations for April 2025 for this miner model
      const monthlyResult = await db.execute(sql`
        SELECT SUM(bitcoin_mined::numeric) as total_bitcoin
        FROM historical_bitcoin_calculations
        WHERE settlement_date LIKE ${yearMonth + '-%'}
        AND miner_model = ${minerModel}
      `);
      
      const totalBitcoin = (monthlyResult[0] as any)?.total_bitcoin;
      
      if (totalBitcoin) {
        // Delete existing summary
        await db.delete(bitcoinMonthlySummaries)
          .where(
            and(
              eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
              eq(bitcoinMonthlySummaries.minerModel, minerModel)
            )
          );
        
        // Insert updated summary
        await db.insert(bitcoinMonthlySummaries).values({
          yearMonth: yearMonth,
          minerModel: minerModel,
          bitcoinMined: totalBitcoin.toString(),
          updatedAt: new Date()
        });
        
        console.log(`Updated monthly summary for ${minerModel}: ${totalBitcoin} BTC`);
      }
    }
    
    console.log("\n===== FIX COMPLETED =====");
    console.log(`All Bitcoin calculations for ${TARGET_DATE} have been successfully updated`);
    
  } catch (error) {
    console.error("ERROR FIXING BITCOIN CALCULATIONS:", error);
    process.exit(1);
  }
}

// Run the fix
main()
  .then(() => process.exit(0))
  .catch(error => {
    console.error("UNHANDLED ERROR:", error);
    process.exit(1);
  });
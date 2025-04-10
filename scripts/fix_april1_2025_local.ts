/**
 * Fix Bitcoin calculations for April 1, 2025 (Local Version)
 * 
 * This script removes and recalculates Bitcoin mining calculations for 2025-04-01
 * using data from the curtailment_records table and a fixed difficulty value.
 */

import { db } from "../db";
import { 
  historicalBitcoinCalculations, 
  curtailmentRecords,
  bitcoinDailySummaries,
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries
} from "../db/schema";
import { eq, and, sql, inArray } from "drizzle-orm";
import { calculateBitcoin } from "../server/utils/bitcoin";

// Target date for processing
const TARGET_DATE = "2025-04-01";

// Miner models to process
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

// Use the known difficulty for April 1, 2025 from existing records
const APRIL_1_DIFFICULTY = 113757508810853;

async function main() {
  try {
    console.log(`\n===== FIXING BITCOIN CALCULATIONS FOR ${TARGET_DATE} =====\n`);

    // Step 1: Analyze current state of curtailment records
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
    
    // Step 3: Process each miner model
    console.log("\n=== Starting Bitcoin Recalculation ===\n");
    
    for (const minerModel of MINER_MODELS) {
      console.log(`\nProcessing ${minerModel}...`);
      
      // Step 3.1: Delete existing calculations to avoid duplicates
      await db.delete(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      console.log(`Deleted existing calculations for ${TARGET_DATE} and ${minerModel}`);
      
      // Step 3.2: Get all curtailment records for this date
      const records = await db.select({
        settlementPeriod: curtailmentRecords.settlementPeriod,
        farmId: curtailmentRecords.farmId,
        leadPartyName: curtailmentRecords.leadPartyName,
        volume: curtailmentRecords.volume
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
      console.log(`Processing ${records.length} curtailment records`);
      
      // Step 3.3: Calculate Bitcoin for each curtailment record
      let totalBitcoin = 0;
      let processedRecords = 0;
      
      // Process in batches to avoid overwhelming the database
      const batchSize = 50;
      const batches = [];
      let currentBatch = [];
      
      for (const record of records) {
        // Convert volume (MWh) to positive number for calculation
        const mwh = Math.abs(Number(record.volume));
        
        // Skip records with zero or invalid volume
        if (mwh <= 0 || isNaN(mwh)) {
          continue;
        }
        
        // Calculate Bitcoin mined
        const bitcoinMined = calculateBitcoin(mwh, minerModel, APRIL_1_DIFFICULTY);
        totalBitcoin += bitcoinMined;
        
        // Add to current batch
        currentBatch.push({
          settlementDate: TARGET_DATE,
          settlementPeriod: Number(record.settlementPeriod),
          minerModel: minerModel,
          farmId: record.farmId,
          bitcoinMined: bitcoinMined.toString(),
          difficulty: APRIL_1_DIFFICULTY.toString()
        });
        
        // If batch is full, add to batches array and start a new batch
        if (currentBatch.length >= batchSize) {
          batches.push([...currentBatch]);
          currentBatch = [];
        }
      }
      
      // Add any remaining records to the batches array
      if (currentBatch.length > 0) {
        batches.push(currentBatch);
      }
      
      // Process each batch
      for (let i = 0; i < batches.length; i++) {
        try {
          // Use batch insert to avoid constraint errors
          await db.insert(historicalBitcoinCalculations)
            .values(batches[i])
            .onConflictDoNothing();
          
          processedRecords += batches[i].length;
          console.log(`Processed batch ${i+1}/${batches.length} (${processedRecords}/${records.length} records)`);
        } catch (error) {
          console.error(`Error processing batch ${i+1}:`, error);
          throw error;
        }
      }
      
      console.log(`Successfully processed ${processedRecords} Bitcoin calculations`);
      console.log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
      
      // Step 3.4: Delete existing daily summary for this date and miner model
      await db.delete(bitcoinDailySummaries)
        .where(
          and(
            eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
            eq(bitcoinDailySummaries.minerModel, minerModel)
          )
        );
        
      // Step 3.5: Create new daily summary
      await db.insert(bitcoinDailySummaries).values({
        summaryDate: TARGET_DATE,
        minerModel: minerModel,
        bitcoinMined: totalBitcoin.toString(),
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      console.log(`Updated daily summary for ${TARGET_DATE} and ${minerModel}`);
    }
    
    // Step 4: Update monthly summary for April 2025
    console.log("\n=== Updating Monthly Summary ===\n");
    
    const yearMonth = TARGET_DATE.substring(0, 7); // 2025-04
    
    // Step 4.1: Delete existing monthly summary records for April 2025
    await db.delete(bitcoinMonthlySummaries)
      .where(
        and(
          eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
          inArray(bitcoinMonthlySummaries.minerModel, MINER_MODELS)
        )
      );
    
    console.log(`Deleted existing monthly summaries for ${yearMonth}`);
    
    // Step 4.2: Create new monthly summaries based on updated daily data
    for (const minerModel of MINER_MODELS) {
      const monthlyStats = await db.select({
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
      })
      .from(bitcoinDailySummaries)
      .where(
        and(
          sql`summary_date::text LIKE ${yearMonth + '%'}`,
          eq(bitcoinDailySummaries.minerModel, minerModel)
        )
      );
      
      if (monthlyStats[0]?.totalBitcoin) {
        await db.insert(bitcoinMonthlySummaries).values({
          yearMonth: yearMonth,
          minerModel: minerModel,
          bitcoinMined: monthlyStats[0].totalBitcoin,
          createdAt: new Date(),
          updatedAt: new Date()
        });
        
        console.log(`Created monthly summary for ${yearMonth} and ${minerModel}`);
      }
    }
    
    // Step 5: Update yearly summary for 2025
    console.log("\n=== Updating Yearly Summary ===\n");
    
    const year = TARGET_DATE.substring(0, 4); // 2025
    
    // Step 5.1: Delete existing yearly summary records for 2025
    await db.delete(bitcoinYearlySummaries)
      .where(
        and(
          eq(bitcoinYearlySummaries.year, year),
          inArray(bitcoinYearlySummaries.minerModel, MINER_MODELS)
        )
      );
    
    console.log(`Deleted existing yearly summaries for ${year}`);
    
    // Step 5.2: Create new yearly summaries based on updated monthly data
    for (const minerModel of MINER_MODELS) {
      const yearlyStats = await db.select({
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
      })
      .from(bitcoinMonthlySummaries)
      .where(
        and(
          sql`year_month::text LIKE ${year + '%'}`,
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        )
      );
      
      if (yearlyStats[0]?.totalBitcoin) {
        await db.insert(bitcoinYearlySummaries).values({
          year: year,
          minerModel: minerModel,
          bitcoinMined: yearlyStats[0].totalBitcoin,
          createdAt: new Date(),
          updatedAt: new Date()
        });
        
        console.log(`Created yearly summary for ${year} and ${minerModel}`);
      }
    }
    
    // Step 6: Final verification
    console.log("\n=== Final Verification ===\n");
    
    // Verify Bitcoin in daily summaries
    const dailySummaries = await db.select({
      minerModel: bitcoinDailySummaries.minerModel,
      bitcoinMined: bitcoinDailySummaries.bitcoinMined
    })
    .from(bitcoinDailySummaries)
    .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`Daily summaries for ${TARGET_DATE}:`);
    for (const summary of dailySummaries) {
      console.log(`- ${summary.minerModel}: ${summary.bitcoinMined} BTC`);
    }
    
    console.log("\n===== BITCOIN CALCULATIONS FIX COMPLETED =====\n");
    console.log("All Bitcoin calculations have been updated for April 1, 2025");
    console.log("Summary tables (daily, monthly, yearly) have been updated");
    
    process.exit(0);
  } catch (error) {
    console.error("Error fixing Bitcoin calculations:", error);
    process.exit(1);
  }
}

// Execute the main function
main().catch((error) => {
  console.error("Unhandled error:", error);
  process.exit(1);
});
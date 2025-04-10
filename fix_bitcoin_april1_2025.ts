/**
 * Fix Bitcoin calculations for April 1, 2025
 * 
 * This script removes existing records from historical_bitcoin_calculations for 2025-04-01
 * and recalculates Bitcoin mining potential for all farm_ids and settlement periods based on
 * curtailment_records data.
 */

import { db } from "./db";
import { 
  historicalBitcoinCalculations, 
  curtailmentRecords,
  bitcoinDailySummaries,
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries
} from "./db/schema";
import { eq, and, sql, inArray } from "drizzle-orm";
import { calculateBitcoin } from "./server/utils/bitcoin";
import { getDifficultyData } from "./server/services/dynamodbService";

// Target date for processing
const TARGET_DATE = "2025-04-01";

// Miner models to process
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

async function main() {
  try {
    console.log(`\n===== FIXING BITCOIN CALCULATIONS FOR ${TARGET_DATE} =====\n`);

    // Step 1: Analyze current state of curtailment records
    const curtailmentStats = await db.select({
      totalRecords: sql<number>`COUNT(*)::int`,
      totalPeriods: sql<number>`COUNT(DISTINCT settlement_period)::int`,
      totalFarms: sql<number>`COUNT(DISTINCT farm_id)::int`,
      totalEnergy: sql<string>`SUM(ABS(volume::numeric))::text`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlement_date, TARGET_DATE));

    console.log(`Curtailment records for ${TARGET_DATE}:`, {
      records: curtailmentStats[0].totalRecords,
      periods: curtailmentStats[0].totalPeriods,
      farms: curtailmentStats[0].totalFarms,
      energy: Number(curtailmentStats[0].totalEnergy).toFixed(2) + " MWh"
    });

    // Step 2: Check existing Bitcoin calculations before deletion
    for (const minerModel of MINER_MODELS) {
      const existingCalcs = await db.select({
        totalRecords: sql<number>`COUNT(*)::int`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`,
        difficulty: sql<string>`DISTINCT(difficulty::numeric)::text`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlement_date, TARGET_DATE),
          eq(historicalBitcoinCalculations.miner_model, minerModel)
        )
      );

      console.log(`Current ${minerModel} calculations:`, {
        records: existingCalcs[0]?.totalRecords || 0,
        bitcoin: existingCalcs[0]?.totalBitcoin || "0",
        difficulty: existingCalcs[0]?.difficulty || "N/A"
      });
    }
    
    // Step 3: Process each miner model
    console.log("\n=== Starting Bitcoin Recalculation ===\n");
    
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing ${minerModel}...`);
      
      // Get the difficulty value from DynamoDB for this date
      const difficulty = await getDifficultyData(TARGET_DATE);
      console.log(`Using difficulty ${difficulty} for ${TARGET_DATE}`);
      
      // Delete existing calculations to avoid duplicates
      await db.delete(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlement_date, TARGET_DATE),
            eq(historicalBitcoinCalculations.miner_model, minerModel)
          )
        );
      
      console.log(`Deleted existing calculations for ${TARGET_DATE} and ${minerModel}`);
      
      // Get all curtailment records for this date
      const records = await db.select({
        settlement_period: curtailmentRecords.settlement_period,
        farm_id: curtailmentRecords.farm_id,
        lead_party_name: curtailmentRecords.lead_party_name,
        volume: curtailmentRecords.volume
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlement_date, TARGET_DATE));
      
      console.log(`Processing ${records.length} curtailment records`);
      
      // Calculate Bitcoin for each curtailment record
      let totalBitcoin = 0;
      const insertValues = [];
      
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
        
        // Add to values array for bulk insert
        insertValues.push({
          settlement_date: TARGET_DATE,
          settlement_period: Number(record.settlement_period),
          miner_model: minerModel,
          farm_id: record.farm_id,
          bitcoin_mined: bitcoinMined.toString(),
          difficulty: difficulty.toString()
        });
      }
      
      // Execute all inserts in batches to avoid overwhelming the database
      console.log(`Inserting ${insertValues.length} calculation records...`);
      const batchSize = 50;
      for (let i = 0; i < insertValues.length; i += batchSize) {
        const batch = insertValues.slice(i, i + batchSize);
        await db.insert(historicalBitcoinCalculations).values(batch);
        console.log(`Inserted batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(insertValues.length/batchSize)}`);
      }
      
      console.log(`Successfully processed ${insertValues.length} Bitcoin calculations for ${minerModel}`);
      console.log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
    }
    
    // Step 4: Verify the new calculations
    console.log("\n=== Verification of New Calculations ===\n");
    
    for (const minerModel of MINER_MODELS) {
      const newCalcs = await db.select({
        totalRecords: sql<number>`COUNT(*)::int`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`,
        difficulty: sql<string>`DISTINCT(difficulty::numeric)::text`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlement_date, TARGET_DATE),
          eq(historicalBitcoinCalculations.miner_model, minerModel)
        )
      );

      console.log(`Updated ${minerModel} calculations:`, {
        records: newCalcs[0]?.totalRecords || 0,
        bitcoin: newCalcs[0]?.totalBitcoin || "0",
        difficulty: newCalcs[0]?.difficulty || "N/A"
      });
    }
    
    // Step 5: Update daily summary for April 1, 2025
    console.log("\n=== Updating Daily Summary ===\n");
    
    // Delete existing daily summary records for April 1
    await db.delete(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summary_date, TARGET_DATE));
    
    console.log(`Deleted existing daily summaries for ${TARGET_DATE}`);
    
    // Create new daily summaries
    for (const minerModel of MINER_MODELS) {
      const dailyStats = await db.select({
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`,
        difficulty: sql<string>`DISTINCT(difficulty::numeric)::text`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlement_date, TARGET_DATE),
          eq(historicalBitcoinCalculations.miner_model, minerModel)
        )
      );
      
      if (dailyStats[0]?.totalBitcoin) {
        await db.insert(bitcoinDailySummaries).values({
          summaryDate: TARGET_DATE,
          minerModel: minerModel,
          bitcoinMined: dailyStats[0].totalBitcoin,
          createdAt: new Date(),
          updatedAt: new Date()
        });
        
        console.log(`Created daily summary for ${TARGET_DATE} and ${minerModel}`);
      }
    }
    
    // Step 6: Update monthly summary for April 2025
    console.log("\n=== Updating Monthly Summary ===\n");
    
    const yearMonth = TARGET_DATE.substring(0, 7); // 2025-04
    
    // Delete existing monthly summary records for April 2025
    await db.delete(bitcoinMonthlySummaries)
      .where(
        and(
          eq(bitcoinMonthlySummaries.year_month, yearMonth),
          inArray(bitcoinMonthlySummaries.miner_model, MINER_MODELS)
        )
      );
    
    console.log(`Deleted existing monthly summaries for ${yearMonth}`);
    
    // Create new monthly summaries based on updated daily data
    for (const minerModel of MINER_MODELS) {
      const monthlyStats = await db.select({
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`,
        avgDifficulty: sql<string>`AVG(difficulty::numeric)::text`
      })
      .from(bitcoinDailySummaries)
      .where(
        and(
          sql`summary_date::text LIKE ${yearMonth + '%'}`,
          eq(bitcoinDailySummaries.miner_model, minerModel)
        )
      );
      
      if (monthlyStats[0]?.totalBitcoin) {
        await db.insert(bitcoinMonthlySummaries).values({
          year_month: yearMonth,
          miner_model: minerModel,
          bitcoin_mined: monthlyStats[0].totalBitcoin,
          created_at: new Date(),
          updated_at: new Date()
        });
        
        console.log(`Created monthly summary for ${yearMonth} and ${minerModel}`);
      }
    }
    
    // Step 7: Update yearly summary for 2025
    console.log("\n=== Updating Yearly Summary ===\n");
    
    const year = TARGET_DATE.substring(0, 4); // 2025
    
    // Delete existing yearly summary records for 2025
    await db.delete(bitcoinYearlySummaries)
      .where(
        and(
          eq(bitcoinYearlySummaries.year, year),
          inArray(bitcoinYearlySummaries.miner_model, MINER_MODELS)
        )
      );
    
    console.log(`Deleted existing yearly summaries for ${year}`);
    
    // Create new yearly summaries based on updated monthly data
    for (const minerModel of MINER_MODELS) {
      const yearlyStats = await db.select({
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`,
        avgDifficulty: sql<string>`AVG(difficulty::numeric)::text`
      })
      .from(bitcoinMonthlySummaries)
      .where(
        and(
          sql`year_month::text LIKE ${year + '%'}`,
          eq(bitcoinMonthlySummaries.miner_model, minerModel)
        )
      );
      
      if (yearlyStats[0]?.totalBitcoin) {
        await db.insert(bitcoinYearlySummaries).values({
          year: year,
          miner_model: minerModel,
          bitcoin_mined: yearlyStats[0].totalBitcoin,
          created_at: new Date(),
          updated_at: new Date()
        });
        
        console.log(`Created yearly summary for ${year} and ${minerModel}`);
      }
    }
    
    console.log("\n===== BITCOIN CALCULATIONS FIX COMPLETED =====\n");
    
  } catch (error) {
    console.error("Error fixing Bitcoin calculations:", error);
  }
}

// Execute the main function
main().catch(console.error);
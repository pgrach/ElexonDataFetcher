/**
 * Fix Bitcoin calculations for April 2, 2025
 * 
 * This script removes existing records from historical_bitcoin_calculations for 2025-04-02
 * and recalculates Bitcoin mining potential for all farm_ids and settlement periods based on
 * curtailment_records data.
 */

import { db } from "./db";
import { 
  historicalBitcoinCalculations, 
  curtailmentRecords,
  bitcoinDailySummaries,
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries,
  dailySummaries
} from "./db/schema";
import { eq, and, sql, inArray } from "drizzle-orm";
import { calculateBitcoin } from "./server/utils/bitcoin";
// import { getDifficultyData } from "./server/services/dynamodbService";

// Target date for processing
const TARGET_DATE = "2025-04-02";

// Miner models to process
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

// Use the difficulty value we found in existing records
const DIFFICULTY = 113757508810853;

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
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));

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
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
      
      // Get difficulty in a separate query
      const difficultyQuery = await db.select({
        difficulty: historicalBitcoinCalculations.difficulty
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      )
      .limit(1);

      console.log(`Current ${minerModel} calculations:`, {
        records: existingCalcs[0]?.totalRecords || 0,
        bitcoin: existingCalcs[0]?.totalBitcoin || "0",
        difficulty: difficultyQuery[0]?.difficulty || "N/A"
      });
    }
    
    // Step 3: Process each miner model
    console.log("\n=== Starting Bitcoin Recalculation ===\n");
    
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing ${minerModel}...`);
      
      // Use fixed difficulty value from the existing records
      console.log(`Using difficulty ${DIFFICULTY} for ${TARGET_DATE}`);
      
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
      const insertValues = [];
      
      for (const record of records) {
        // Convert volume (MWh) to positive number for calculation
        const mwh = Math.abs(Number(record.volume));
        
        // Skip records with zero or invalid volume
        if (mwh <= 0 || isNaN(mwh)) {
          continue;
        }
        
        // Calculate Bitcoin mined
        const bitcoinMined = calculateBitcoin(mwh, minerModel, DIFFICULTY);
        totalBitcoin += bitcoinMined;
        
        // Add to values array for bulk insert
        insertValues.push({
          settlementDate: TARGET_DATE,
          settlementPeriod: Number(record.settlementPeriod),
          minerModel: minerModel,
          farmId: record.farmId,
          bitcoinMined: bitcoinMined.toString(),
          difficulty: DIFFICULTY.toString()
        });
      }
      
      // Execute all inserts in batches using onConflictDoUpdate to avoid unique constraint errors
      console.log(`Inserting ${insertValues.length} calculation records...`);
      const batchSize = 50;
      for (let i = 0; i < insertValues.length; i += batchSize) {
        const batch = insertValues.slice(i, i + batchSize);
        
        try {
          // Try to insert with onConflictDoNothing first to avoid errors
          await db.insert(historicalBitcoinCalculations)
            .values(batch)
            .onConflictDoNothing();
          
          console.log(`Inserted batch ${Math.floor(i/batchSize) + 1}/${Math.ceil(insertValues.length/batchSize)}`);
        } catch (error) {
          console.error(`Error inserting batch ${Math.floor(i/batchSize) + 1}:`, error);
          
          // Try inserting records one by one to identify problematic records
          for (const record of batch) {
            try {
              await db.insert(historicalBitcoinCalculations)
                .values(record)
                .onConflictDoNothing();
            } catch (recordError) {
              console.error(`Failed to insert record: ${JSON.stringify(record)}`, recordError);
            }
          }
        }
      }
      
      console.log(`Successfully processed ${insertValues.length} Bitcoin calculations for ${minerModel}`);
      console.log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
    }
    
    // Step 4: Verify the new calculations
    console.log("\n=== Verification of New Calculations ===\n");
    
    for (const minerModel of MINER_MODELS) {
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
      
      // Get difficulty in a separate query
      const difficultyQuery = await db.select({
        difficulty: historicalBitcoinCalculations.difficulty
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      )
      .limit(1);

      console.log(`Updated ${minerModel} calculations:`, {
        records: newCalcs[0]?.totalRecords || 0,
        bitcoin: newCalcs[0]?.totalBitcoin || "0",
        difficulty: difficultyQuery[0]?.difficulty || "N/A"
      });
    }
    
    // Step 5: Update daily summary for April 2, 2025
    console.log("\n=== Updating Daily Summary ===\n");
    
    // Delete existing daily summary records for April 2
    await db.delete(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`Deleted existing daily summaries for ${TARGET_DATE}`);
    
    // Create new daily summaries
    for (const minerModel of MINER_MODELS) {
      // Get total Bitcoin mined for the day
      const dailyStats = await db.select({
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
      
      // Get difficulty value in a separate query
      const difficultyQuery = await db.select({
        difficulty: historicalBitcoinCalculations.difficulty
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      )
      .limit(1);
      
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
          eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
          inArray(bitcoinMonthlySummaries.minerModel, MINER_MODELS)
        )
      );
    
    console.log(`Deleted existing monthly summaries for ${yearMonth}`);
    
    // Create new monthly summaries based on updated daily data
    for (const minerModel of MINER_MODELS) {
      // Get total Bitcoin mined for the month
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
    
    // Step 7: Update yearly summary for 2025
    console.log("\n=== Updating Yearly Summary ===\n");
    
    const year = TARGET_DATE.substring(0, 4); // 2025
    
    // Delete existing yearly summary records for 2025
    await db.delete(bitcoinYearlySummaries)
      .where(
        and(
          eq(bitcoinYearlySummaries.year, year),
          inArray(bitcoinYearlySummaries.minerModel, MINER_MODELS)
        )
      );
    
    console.log(`Deleted existing yearly summaries for ${year}`);
    
    // Create new yearly summaries based on updated monthly data
    for (const minerModel of MINER_MODELS) {
      // Get total Bitcoin mined for the year
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
    
    // Step 8: Verify data integrity
    console.log("\n=== Final Data Integrity Verification ===\n");
    
    // Get curtailment record stats
    const finalCurtailmentStats = await db.select({
      totalRecords: sql<number>`COUNT(*)::int`,
      totalPeriods: sql<number>`COUNT(DISTINCT settlement_period)::int`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
      totalPayment: sql<string>`SUM(payment::numeric)::text`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Get daily summary stats
    const summary = await db.select({
      totalCurtailedEnergy: dailySummaries.totalCurtailedEnergy,
      totalPayment: dailySummaries.totalPayment
    })
    .from(dailySummaries)
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    // Check if values are consistent
    const dbEnergy = Number(finalCurtailmentStats[0]?.totalVolume || 0);
    const dbPayment = Number(finalCurtailmentStats[0]?.totalPayment || 0);
    
    let summaryEnergy = 0;
    let summaryPayment = 0;
    
    if (summary.length > 0) {
      summaryEnergy = Number(summary[0].totalCurtailedEnergy);
      summaryPayment = Number(summary[0].totalPayment);
    }
    
    // Check if values are consistent (within 0.01 tolerance)
    const isEnergyConsistent = Math.abs(dbEnergy - summaryEnergy) < 0.01;
    const isPaymentConsistent = Math.abs(dbPayment - summaryPayment) < 0.01;
    const isConsistent = isEnergyConsistent && isPaymentConsistent;
    
    console.log(`Verification for ${TARGET_DATE}:`);
    console.log(`Records: ${finalCurtailmentStats[0]?.totalRecords || 0}`);
    console.log(`Settlement Periods: ${finalCurtailmentStats[0]?.totalPeriods || 0}`);
    console.log(`Data consistency: ${isConsistent ? 'CONSISTENT ✓' : 'INCONSISTENT ✗'}`);
    
    console.log('\nRaw database values:');
    console.log(`- Energy: ${dbEnergy.toFixed(2)} MWh`);
    console.log(`- Payment: £${dbPayment.toFixed(2)}`);
    
    console.log('Summary table values:');
    console.log(`- Energy: ${summaryEnergy.toFixed(2)} MWh`);
    console.log(`- Payment: £${summaryPayment.toFixed(2)}`);
    
    if (!isConsistent) {
      console.log('\nDifferences:');
      console.log(`- Energy diff: ${Math.abs(dbEnergy - summaryEnergy).toFixed(2)} MWh`);
      console.log(`- Payment diff: £${Math.abs(dbPayment - summaryPayment).toFixed(2)}`);
    }
    
    // Final Bitcoin calculation stats
    console.log("\n=== Final Bitcoin Calculation Summary ===\n");
    
    for (const minerModel of MINER_MODELS) {
      const finalStats = await db.select({
        recordCount: sql<number>`COUNT(*)::int`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)::int`,
        bitcoinMined: sql<string>`SUM(bitcoin_mined::numeric)::text`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
      
      console.log(`${minerModel}:`, {
        records: finalStats[0]?.recordCount || 0,
        periods: finalStats[0]?.periodCount || 0,
        farms: finalStats[0]?.farmCount || 0,
        bitcoin: Number(finalStats[0]?.bitcoinMined || 0).toFixed(8) + " BTC"
      });
    }
    
    console.log("\n===== BITCOIN CALCULATIONS FIX COMPLETED =====\n");
    
  } catch (error) {
    console.error("Error fixing Bitcoin calculations:", error);
  }
}

// Execute the main function
main().catch(console.error);
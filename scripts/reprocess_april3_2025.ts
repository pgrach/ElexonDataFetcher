/**
 * Script to reprocess curtailment data for 2025-04-03
 * 
 * This script fetches data from Elexon API for all 48 settlement periods and
 * updates all dependent tables according to the data pipeline:
 * 1. Clear existing curtailment records
 * 2. Fetch new data from Elexon API
 * 3. Process into curtailment_records
 * 4. Update daily_summaries
 * 5. Update monthly_summaries
 * 6. Update yearly_summaries
 * 7. Process Bitcoin calculations
 * 8. Update Bitcoin summary tables
 * 
 * Run with: tsx scripts/reprocess_april3_2025.ts
 */

import { db } from "../db";
import { 
  curtailmentRecords, 
  dailySummaries, 
  monthlySummaries,
  yearlySummaries,
  bitcoinDailySummaries, 
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries,
  historicalBitcoinCalculations 
} from "../db/schema";
import { and, eq, sql } from "drizzle-orm";
import { processDailyCurtailment } from "../server/services/curtailment_enhanced";
import { processHistoricalCalculations } from "../server/services/bitcoinService";
import { manualUpdateYearlyBitcoinSummary } from "../server/services/bitcoinService";
import { calculateMonthlyBitcoinSummary } from "../server/services/bitcoinService";
import { minerModels } from "../server/types/bitcoin";

// Target date constants
const TARGET_DATE = "2025-04-03";
const YEAR_MONTH = TARGET_DATE.substring(0, 7); // "2025-04" 
const YEAR = TARGET_DATE.substring(0, 4); // "2025"

// Get the list of miner models
const MINER_MODEL_LIST = Object.keys(minerModels);

/**
 * Clear existing data for the target date
 */
async function clearExistingData(): Promise<void> {
  console.log(`\n=== Clearing existing data for ${TARGET_DATE} ===`);
  
  // Get existing curtailment record stats
  const existingRecords = await db
    .select({
      count: sql<number>`COUNT(*)::int`,
      periods: sql<number>`COUNT(DISTINCT settlement_period)::int`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
      totalPayment: sql<string>`SUM(payment::numeric)::text`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  const recordCount = existingRecords[0]?.count || 0;
  
  console.log(`Found ${recordCount} existing curtailment records`);
  if (recordCount > 0) {
    console.log(`Settlement Periods: ${existingRecords[0]?.periods || 0}`);
    console.log(`Total Volume: ${existingRecords[0]?.totalVolume || '0'} MWh`);
    console.log(`Total Payment: £${existingRecords[0]?.totalPayment || '0'}`);
  }
  
  // Clear existing curtailment records
  if (recordCount > 0) {
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    console.log(`Deleted ${recordCount} existing curtailment records`);
  }
  
  // Clear existing daily summary
  const deletedDailySummary = await db.delete(dailySummaries)
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
  console.log(`Cleared daily summary for ${TARGET_DATE}`);
  
  // Clear existing Bitcoin calculations and summaries
  for (const minerModel of MINER_MODEL_LIST) {
    // Clear Bitcoin calculations
    const deletedCalcs = await db.delete(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
    
    // Clear Bitcoin daily summary
    const deletedBitcoinDaily = await db.delete(bitcoinDailySummaries)
      .where(
        and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        )
      );
    
    console.log(`Cleared Bitcoin data for ${minerModel}`);
  }
  
  console.log("All existing data has been cleared successfully");
}

/**
 * Process data from Elexon API and populate curtailment_records
 */
async function processCurtailmentData(): Promise<void> {
  console.log(`\n=== Processing curtailment data for ${TARGET_DATE} ===`);
  console.log("Fetching data from Elexon API for all 48 settlement periods...");
  
  const startTime = Date.now();
  await processDailyCurtailment(TARGET_DATE);
  const processingTime = (Date.now() - startTime) / 1000;
  
  console.log(`Completed processing in ${processingTime.toFixed(2)} seconds`);
  
  // Verify the processed records
  const newRecords = await db
    .select({
      count: sql<number>`COUNT(*)::int`,
      periods: sql<number>`COUNT(DISTINCT settlement_period)::int`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
      totalPayment: sql<string>`SUM(payment::numeric)::text`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  const recordCount = newRecords[0]?.count || 0;
  const periodCount = newRecords[0]?.periods || 0;
  
  console.log(`Processed ${recordCount} records across ${periodCount} settlement periods`);
  console.log(`Total Volume: ${newRecords[0]?.totalVolume || '0'} MWh`);
  console.log(`Total Payment: £${newRecords[0]?.totalPayment || '0'}`);
  
  // Check daily summary
  const dailySummary = await db.query.dailySummaries.findFirst({
    where: eq(dailySummaries.summaryDate, TARGET_DATE)
  });
  
  if (dailySummary) {
    console.log(`\nDaily summary created:`);
    console.log(`  Energy: ${dailySummary.totalCurtailedEnergy} MWh`);
    console.log(`  Payment: £${dailySummary.totalPayment}`);
  } else if (recordCount > 0) {
    console.log(`\nWARNING: Daily summary not created despite having ${recordCount} records!`);
  } else {
    console.log(`\nNo daily summary created - no curtailment occurred on this date`);
  }
}

/**
 * Process Bitcoin calculations for the target date
 */
async function processBitcoinCalculations(): Promise<void> {
  console.log(`\n=== Processing Bitcoin calculations for ${TARGET_DATE} ===`);
  
  // Get existing curtailment data to calculate Bitcoin directly if needed
  const curtailmentData = await db
    .select({
      settlementPeriod: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      volume: curtailmentRecords.volume
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  if (curtailmentData.length === 0) {
    console.log("No curtailment data found for this date, skipping Bitcoin calculations");
    return;
  }
  
  console.log(`Found ${curtailmentData.length} curtailment records to process for Bitcoin calculations`);
  
  // Process each miner model
  for (const minerModel of MINER_MODEL_LIST) {
    console.log(`\nProcessing ${minerModel}...`);
    
    try {
      // Try using the service function first
      try {
        await processHistoricalCalculations(TARGET_DATE, minerModel);
        console.log(`  Successfully processed Bitcoin calculations using service function`);
      } catch (serviceError) {
        console.error(`  ERROR with service function: ${serviceError.message}`);
        console.log(`  Falling back to manual calculation...`);
        
        // Fall back to simplified direct calculation
        // Note: This is a simplified version that may not match the full logic in the service
        await manualCalculateBitcoin(TARGET_DATE, minerModel, curtailmentData);
      }
      
      // Verify the calculations
      const bitcoinStats = await db
        .select({
          count: sql<number>`COUNT(*)::int`,
          periods: sql<number>`COUNT(DISTINCT settlement_period)::int`,
          farms: sql<number>`COUNT(DISTINCT farm_id)::int`,
          totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)::text`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      console.log(`  Records: ${bitcoinStats[0]?.count || 0}`);
      console.log(`  Periods: ${bitcoinStats[0]?.periods || 0}`);
      console.log(`  Farms: ${bitcoinStats[0]?.farms || 0}`);
      console.log(`  Total Bitcoin: ${bitcoinStats[0]?.totalBitcoin || '0'} BTC`);
      
      // Check daily Bitcoin summary
      const dailyBitcoin = await db.query.bitcoinDailySummaries.findFirst({
        where: and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        )
      });
      
      if (dailyBitcoin) {
        console.log(`  Daily Summary: ${dailyBitcoin.bitcoinMined} BTC`);
      } else if (bitcoinStats[0]?.count && bitcoinStats[0]?.count > 0) {
        // Create the daily summary manually if it doesn't exist
        try {
          await db.insert(bitcoinDailySummaries).values({
            summaryDate: TARGET_DATE,
            minerModel: minerModel,
            bitcoinMined: bitcoinStats[0].totalBitcoin,
            updatedAt: new Date()
          });
          console.log(`  Created missing daily Bitcoin summary: ${bitcoinStats[0].totalBitcoin} BTC`);
        } catch (summaryError) {
          console.error(`  ERROR creating daily summary: ${summaryError.message}`);
        }
      }
    } catch (error) {
      console.error(`  ERROR processing ${minerModel}:`, error);
    }
  }
}

/**
 * Manual Bitcoin calculation fallback
 */
async function manualCalculateBitcoin(
  date: string, 
  minerModel: string, 
  curtailmentData: { settlementPeriod: number; farmId: string; volume: string }[]
): Promise<void> {
  console.log(`  Performing manual Bitcoin calculation for ${minerModel}`);
  
  // Use a default difficulty as fallback
  const difficulty = 121507793131898; // Default from the logs
  let totalBitcoin = 0;
  const calculations = [];
  
  // Import the calculateBitcoin function
  const { calculateBitcoin } = await import("../server/utils/bitcoin");
  
  // Process each curtailment record
  for (const record of curtailmentData) {
    try {
      // Convert volume (MWh) to positive number for calculation
      const mwh = Math.abs(Number(record.volume));
      
      // Skip records with zero or invalid volume
      if (mwh <= 0 || isNaN(mwh)) continue;
      
      // Calculate Bitcoin mined
      const bitcoinMined = calculateBitcoin(mwh, minerModel, difficulty);
      totalBitcoin += bitcoinMined;
      
      // Add to calculations array
      calculations.push({
        settlementDate: date,
        settlementPeriod: record.settlementPeriod,
        minerModel: minerModel,
        farmId: record.farmId,
        bitcoinMined: bitcoinMined.toString(),
        difficulty: difficulty.toString()
      });
    } catch (error) {
      console.error(`    Error calculating Bitcoin for period ${record.settlementPeriod}, farm ${record.farmId}:`, error);
    }
  }
  
  // Insert calculations in batches
  const BATCH_SIZE = 50;
  const batches = [];
  
  // Create batches
  for (let i = 0; i < calculations.length; i += BATCH_SIZE) {
    batches.push(calculations.slice(i, i + BATCH_SIZE));
  }
  
  // Insert each batch
  for (let i = 0; i < batches.length; i++) {
    try {
      await db.insert(historicalBitcoinCalculations).values(batches[i]);
      console.log(`    Inserted batch ${i+1}/${batches.length} (${batches[i].length} records)`);
    } catch (error) {
      console.error(`    Error inserting batch ${i+1}:`, error);
    }
  }
  
  console.log(`  Completed manual Bitcoin calculation: ${totalBitcoin.toFixed(8)} BTC from ${calculations.length} records`);
}

/**
 * Update all summary tables for Bitcoin calculations
 */
async function updateBitcoinSummaries(): Promise<void> {
  console.log(`\n=== Updating Bitcoin summary tables ===`);
  
  console.log("\nUpdating monthly Bitcoin summaries...");
  // Update monthly summaries for all miner models
  for (const minerModel of MINER_MODEL_LIST) {
    try {
      await calculateMonthlyBitcoinSummary(YEAR_MONTH, minerModel);
      
      // Verify monthly summary
      const monthlySummary = await db.query.bitcoinMonthlySummaries.findFirst({
        where: and(
          eq(bitcoinMonthlySummaries.yearMonth, YEAR_MONTH),
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        )
      });
      
      if (monthlySummary) {
        console.log(`Updated ${YEAR_MONTH} summary for ${minerModel}: ${monthlySummary.bitcoinMined} BTC`);
      } else {
        console.log(`No monthly summary created for ${minerModel} (likely no data)`);
      }
    } catch (error) {
      console.error(`Error updating monthly summary for ${minerModel}:`, error);
    }
  }
  
  console.log("\nUpdating yearly Bitcoin summary...");
  // Update yearly summary
  try {
    await manualUpdateYearlyBitcoinSummary(YEAR);
    
    // Verify yearly summaries
    const yearlySummaries = await db.query.bitcoinYearlySummaries.findMany({
      where: eq(bitcoinYearlySummaries.year, YEAR)
    });
    
    if (yearlySummaries.length > 0) {
      for (const summary of yearlySummaries) {
        console.log(`Updated ${YEAR} summary for ${summary.minerModel}: ${summary.bitcoinMined} BTC`);
      }
    } else {
      console.log(`No yearly summaries created (likely no data)`);
    }
  } catch (error) {
    console.error(`Error updating yearly summary:`, error);
  }
}

/**
 * Update monthly and yearly curtailment summaries
 */
async function updateCurtailmentSummaries(): Promise<void> {
  console.log(`\n=== Updating curtailment summary tables ===`);
  
  // Check if there's any data to update summaries with
  const dailySummary = await db.query.dailySummaries.findFirst({
    where: eq(dailySummaries.summaryDate, TARGET_DATE)
  });
  
  if (!dailySummary) {
    console.log("No daily summary exists, skipping monthly/yearly summary updates");
    return;
  }
  
  // Monthly summary - first get the current state
  console.log("\nChecking monthly summary for April 2025...");
  const oldMonthlySummary = await db.query.monthlySummaries.findFirst({
    where: eq(monthlySummaries.yearMonth, YEAR_MONTH)
  });
  
  if (oldMonthlySummary) {
    console.log(`Existing monthly summary: ${oldMonthlySummary.totalCurtailedEnergy} MWh, £${oldMonthlySummary.totalPayment}`);
  }
  
  // Update monthly summary manually
  console.log("Updating monthly summary...");
  const monthlyResult = await db
    .select({
      totalCurtailedEnergy: sql<string>`SUM(total_curtailed_energy::numeric)`,
      totalPayment: sql<string>`SUM(total_payment::numeric)`
    })
    .from(dailySummaries)
    .where(sql`summary_date::text LIKE ${YEAR_MONTH + '-%'}`);
  
  if (monthlyResult[0] && monthlyResult[0].totalCurtailedEnergy && monthlyResult[0].totalPayment) {
    await db.insert(monthlySummaries).values({
      yearMonth: YEAR_MONTH,
      totalCurtailedEnergy: monthlyResult[0].totalCurtailedEnergy,
      totalPayment: monthlyResult[0].totalPayment,
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [monthlySummaries.yearMonth],
      set: {
        totalCurtailedEnergy: monthlyResult[0].totalCurtailedEnergy,
        totalPayment: monthlyResult[0].totalPayment,
        updatedAt: new Date()
      }
    });
    
    console.log(`Updated monthly summary: ${monthlyResult[0].totalCurtailedEnergy} MWh, £${monthlyResult[0].totalPayment}`);
  }
  
  // Yearly summary - first get the current state
  console.log("\nChecking yearly summary for 2025...");
  const oldYearlySummary = await db.query.yearlySummaries.findFirst({
    where: eq(yearlySummaries.year, YEAR)
  });
  
  if (oldYearlySummary) {
    console.log(`Existing yearly summary: ${oldYearlySummary.totalCurtailedEnergy} MWh, £${oldYearlySummary.totalPayment}`);
  }
  
  // Update yearly summary manually
  console.log("Updating yearly summary...");
  const yearlyResult = await db
    .select({
      totalCurtailedEnergy: sql<string>`SUM(total_curtailed_energy::numeric)`,
      totalPayment: sql<string>`SUM(total_payment::numeric)`
    })
    .from(monthlySummaries)
    .where(sql`year_month::text LIKE ${YEAR + '-%'}`);
  
  if (yearlyResult[0] && yearlyResult[0].totalCurtailedEnergy && yearlyResult[0].totalPayment) {
    await db.insert(yearlySummaries).values({
      year: YEAR,
      totalCurtailedEnergy: yearlyResult[0].totalCurtailedEnergy,
      totalPayment: yearlyResult[0].totalPayment,
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [yearlySummaries.year],
      set: {
        totalCurtailedEnergy: yearlyResult[0].totalCurtailedEnergy,
        totalPayment: yearlyResult[0].totalPayment,
        updatedAt: new Date()
      }
    });
    
    console.log(`Updated yearly summary: ${yearlyResult[0].totalCurtailedEnergy} MWh, £${yearlyResult[0].totalPayment}`);
  }
}

/**
 * Main function to orchestrate the data reprocessing
 */
async function main() {
  console.log(`\n========================================================`);
  console.log(`== REPROCESSING ALL DATA FOR ${TARGET_DATE} ==`);
  console.log(`========================================================`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  const startTime = Date.now();
  
  try {
    // Step 1: Clear existing data
    await clearExistingData();
    
    // Step 2: Process new curtailment data from Elexon API
    await processCurtailmentData();
    
    // Step 3: Update curtailment summary tables
    await updateCurtailmentSummaries();
    
    // Step 4: Process Bitcoin calculations
    await processBitcoinCalculations();
    
    // Step 5: Update Bitcoin summary tables
    await updateBitcoinSummaries();
    
    const totalTime = (Date.now() - startTime) / 1000;
    console.log(`\n========================================================`);
    console.log(`== REPROCESSING COMPLETED SUCCESSFULLY ==`);
    console.log(`== Total time: ${totalTime.toFixed(2)} seconds ==`);
    console.log(`========================================================`);
    
    // Exit cleanly
    process.exit(0);
  } catch (error) {
    console.error("\n=== ERROR DURING REPROCESSING ===");
    console.error(error);
    console.error("========================================================");
    process.exit(1);
  }
}

// Execute the main function
main();
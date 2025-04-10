/**
 * Bitcoin Calculation Processing Script for 2025-04-03
 * 
 * This script handles only Bitcoin calculations without the Elexon API calls
 * for April 3, 2025 (2025-04-03).
 */

import { db } from "../db";
import { 
  curtailmentRecords,
  historicalBitcoinCalculations, 
  bitcoinDailySummaries, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries 
} from "../db/schema";
import { calculateBitcoin } from "../server/utils/bitcoin";
import { eq, and, sql } from "drizzle-orm";
import { performance } from "perf_hooks";

// Constants
const TARGET_DATE = '2025-04-03';
const DEFAULT_DIFFICULTY = 113757508810853; // Use the standard difficulty for 2025
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// For tracking execution time
const startTime = performance.now();

/**
 * Clear existing Bitcoin calculations for the target date
 */
async function clearExistingBitcoinCalculations(): Promise<void> {
  console.log(`\n==== Clearing existing Bitcoin calculations for ${TARGET_DATE} ====\n`);
  
  try {
    // 1. Clear historical_bitcoin_calculations
    for (const minerModel of MINER_MODELS) {
      await db.delete(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      console.log(`Cleared historical Bitcoin calculations for ${TARGET_DATE} and ${minerModel}`);
    }
    
    // 2. Clear bitcoin_daily_summaries
    for (const minerModel of MINER_MODELS) {
      await db.delete(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      console.log(`Cleared Bitcoin daily summaries for ${TARGET_DATE} and ${minerModel}`);
    }
    
    console.log(`\n==== Successfully cleared existing Bitcoin calculations ====\n`);
  } catch (error) {
    console.error(`Error clearing existing Bitcoin calculations:`, error);
    throw error;
  }
}

/**
 * Process Bitcoin calculations for a specific miner model
 */
async function processBitcoinCalculations(minerModel: string): Promise<number> {
  try {
    console.log(`Processing Bitcoin calculations for ${TARGET_DATE} with miner model ${minerModel}`);
    
    // Get all curtailment records for this date
    const records = await db.select({
      settlementPeriod: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      leadPartyName: curtailmentRecords.leadPartyName,
      volume: curtailmentRecords.volume
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (records.length === 0) {
      console.log(`No curtailment records found for ${TARGET_DATE}`);
      return 0;
    }
    
    console.log(`Found ${records.length} curtailment records for ${TARGET_DATE}`);
    
    // Filter and prepare records
    let totalBitcoin = 0;
    let successfulRecords = 0;
    const filteredRecords = [];
    
    // First, filter records to make sure we're only processing valid curtailment records
    for (const record of records) {
      // Convert volume (MWh) to positive number for calculation
      const mwh = Math.abs(Number(record.volume));
      
      // Skip records with zero or invalid volume
      if (mwh <= 0 || isNaN(mwh)) {
        continue;
      }
      
      // Only include valid records
      filteredRecords.push({
        ...record,
        mwh
      });
    }
    
    console.log(`Found ${filteredRecords.length} valid curtailment records for ${TARGET_DATE} with non-zero energy`);
    
    // Process the records in batches to prevent memory issues
    const batchSize = 50;
    const batches = Math.ceil(filteredRecords.length / batchSize);
    
    for (let i = 0; i < batches; i++) {
      const batch = filteredRecords.slice(i * batchSize, (i + 1) * batchSize);
      const insertPromises = [];
      
      for (const record of batch) {
        // Calculate Bitcoin mined
        const bitcoinMined = calculateBitcoin(record.mwh, minerModel, DEFAULT_DIFFICULTY);
        totalBitcoin += bitcoinMined;
        
        try {
          // Insert the calculation record using on conflict do update to handle duplicates
          insertPromises.push(
            db.insert(historicalBitcoinCalculations).values({
              settlementDate: TARGET_DATE,
              settlementPeriod: Number(record.settlementPeriod),
              minerModel: minerModel,
              farmId: record.farmId,
              bitcoinMined: bitcoinMined.toString(),
              difficulty: DEFAULT_DIFFICULTY.toString()
            }).onConflictDoUpdate({
              target: [
                historicalBitcoinCalculations.settlementDate, 
                historicalBitcoinCalculations.settlementPeriod, 
                historicalBitcoinCalculations.farmId, 
                historicalBitcoinCalculations.minerModel
              ],
              set: {
                bitcoinMined: bitcoinMined.toString(),
                difficulty: DEFAULT_DIFFICULTY.toString(),
                calculatedAt: new Date()
              }
            })
          );
        } catch (error) {
          console.error(`Error processing record for ${record.farmId}, period ${record.settlementPeriod}:`, error);
          // Continue with other records
        }
      }
      
      // Execute all inserts for this batch
      try {
        await Promise.all(insertPromises);
        successfulRecords += insertPromises.length;
        console.log(`Batch ${i+1}/${batches}: Processed ${insertPromises.length} records`);
      } catch (error) {
        console.error(`Error processing batch ${i+1}:`, error);
      }
    }
    
    console.log(`Successfully processed ${successfulRecords} Bitcoin calculations for ${TARGET_DATE} and ${minerModel}`);
    console.log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
    
    // After all historical calculations are inserted, calculate the actual total from the database
    const historicalTotal = await db
      .select({ total: sql<string>`SUM(bitcoin_mined::numeric)` })
      .from(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      ));
    
    const dbTotal = historicalTotal[0]?.total ? parseFloat(historicalTotal[0].total) : 0;
    console.log(`Database total for ${minerModel}: ${dbTotal} BTC`);
    
    // Return the actual database total, not the calculated one
    return dbTotal;
  } catch (error) {
    console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
    throw error;
  }
}

/**
 * Update Bitcoin daily summary
 */
async function updateBitcoinDailySummary(minerModel: string, totalBitcoin: number): Promise<void> {
  try {
    await db.insert(bitcoinDailySummaries).values({
      summaryDate: TARGET_DATE,
      minerModel: minerModel,
      bitcoinMined: totalBitcoin.toString(),
      createdAt: new Date(),
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [
        bitcoinDailySummaries.summaryDate,
        bitcoinDailySummaries.minerModel
      ],
      set: {
        bitcoinMined: totalBitcoin.toString(),
        updatedAt: new Date()
      }
    });
    
    console.log(`Updated daily summary for ${TARGET_DATE} and ${minerModel}: ${totalBitcoin} BTC`);
  } catch (error) {
    console.error(`Error updating daily summary for ${minerModel}:`, error);
    throw error;
  }
}

/**
 * Update Bitcoin monthly summary
 */
async function updateBitcoinMonthlySummary(minerModel: string): Promise<void> {
  try {
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    
    // Calculate the total Bitcoin for the month
    const result = await db.execute(sql`
      SELECT
        SUM(bitcoin_mined::NUMERIC) as total_bitcoin
      FROM
        bitcoin_daily_summaries
      WHERE
        TO_CHAR(summary_date, 'YYYY-MM') = ${yearMonth}
        AND miner_model = ${minerModel}
    `);
    
    // SQL results come as an array of records
    const resultArray = result as unknown as Array<Record<string, unknown>>;
    const data = resultArray.length > 0 ? resultArray[0] : null;
    
    if (!data || !data.total_bitcoin) {
      console.log(`No daily Bitcoin data found for ${yearMonth} and ${minerModel}`);
      return;
    }
    
    // Delete existing monthly summary if any
    await db.delete(bitcoinMonthlySummaries)
      .where(and(
        eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
        eq(bitcoinMonthlySummaries.minerModel, minerModel)
      ));
    
    // Insert new monthly summary
    await db.insert(bitcoinMonthlySummaries).values({
      yearMonth: yearMonth,
      minerModel: minerModel,
      bitcoinMined: data.total_bitcoin.toString(),
      updatedAt: new Date()
    });
    
    console.log(`Updated monthly Bitcoin summary for ${yearMonth} and ${minerModel}: ${data.total_bitcoin} BTC`);
  } catch (error) {
    console.error(`Error updating monthly Bitcoin summary for ${minerModel}:`, error);
    throw error;
  }
}

/**
 * Update Bitcoin yearly summary
 */
async function updateBitcoinYearlySummary(minerModel: string): Promise<void> {
  try {
    const year = TARGET_DATE.substring(0, 4); // YYYY
    
    // Calculate the total Bitcoin for the year
    const result = await db.execute(sql`
      SELECT
        SUM(bitcoin_mined::NUMERIC) as total_bitcoin
      FROM
        bitcoin_monthly_summaries
      WHERE
        SUBSTRING(year_month, 1, 4) = ${year}
        AND miner_model = ${minerModel}
    `);
    
    // SQL results come as an array of records
    const resultArray = result as unknown as Array<Record<string, unknown>>;
    const data = resultArray.length > 0 ? resultArray[0] : null;
    
    if (!data || !data.total_bitcoin) {
      console.log(`No monthly Bitcoin data found for ${year} and ${minerModel}`);
      return;
    }
    
    // Delete existing yearly summary if any
    await db.delete(bitcoinYearlySummaries)
      .where(and(
        eq(bitcoinYearlySummaries.year, year),
        eq(bitcoinYearlySummaries.minerModel, minerModel)
      ));
    
    // Insert new yearly summary
    await db.insert(bitcoinYearlySummaries).values({
      year: year,
      minerModel: minerModel,
      bitcoinMined: data.total_bitcoin.toString(),
      updatedAt: new Date()
    });
    
    console.log(`Updated yearly Bitcoin summary for ${year} and ${minerModel}: ${data.total_bitcoin} BTC`);
  } catch (error) {
    console.error(`Error updating yearly Bitcoin summary for ${minerModel}:`, error);
    throw error;
  }
}

/**
 * Verify results after reprocessing
 */
async function verifyResults(): Promise<void> {
  console.log(`\n==== Verifying Bitcoin calculation results ====\n`);
  
  try {
    // Check curtailment records exist
    const curtailmentCount = await db
      .select({ count: sql<number>`COUNT(*)::int` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Curtailment records for ${TARGET_DATE}: ${curtailmentCount[0]?.count || 0} records`);
    
    if (curtailmentCount[0]?.count === 0) {
      console.error(`ERROR: No curtailment records found for ${TARGET_DATE}. Bitcoin calculations cannot proceed.`);
      throw new Error(`No curtailment records found for ${TARGET_DATE}`);
    }
    
    // Check Bitcoin calculations
    for (const minerModel of MINER_MODELS) {
      const historicalCount = await db
        .select({ count: sql<number>`COUNT(*)::int` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      console.log(`Historical Bitcoin calculations for ${minerModel}: ${historicalCount[0]?.count || 0} records`);
      
      const historicalTotal = await db
        .select({ total: sql<string>`SUM(bitcoin_mined::numeric)` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      console.log(`Total Bitcoin calculated for ${minerModel}: ${historicalTotal[0]?.total || '0'} BTC`);
      
      const dailySummary = await db
        .select()
        .from(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      if (dailySummary.length > 0) {
        console.log(`Daily Bitcoin summary for ${minerModel}: ${dailySummary[0].bitcoinMined} BTC`);
      } else {
        console.log(`Warning: No daily Bitcoin summary found for ${minerModel}`);
      }
    }
    
    // Check Bitcoin monthly summaries
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    for (const minerModel of MINER_MODELS) {
      const monthlySummary = await db
        .select()
        .from(bitcoinMonthlySummaries)
        .where(and(
          eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        ));
      
      if (monthlySummary.length > 0) {
        console.log(`Monthly Bitcoin summary for ${yearMonth} and ${minerModel}: ${monthlySummary[0].bitcoinMined} BTC`);
      } else {
        console.log(`Warning: No monthly Bitcoin summary found for ${yearMonth} and ${minerModel}`);
      }
    }
    
    // Compare Bitcoin values between different miner models to ensure they're reasonable
    const bitcoinTotals: Record<string, number> = {};
    
    for (const minerModel of MINER_MODELS) {
      const dailySummary = await db
        .select()
        .from(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      if (dailySummary.length > 0) {
        bitcoinTotals[minerModel] = parseFloat(dailySummary[0].bitcoinMined);
      }
    }
    
    // Check relative values
    if (bitcoinTotals['S19J_PRO'] && bitcoinTotals['M20S'] && bitcoinTotals['S9']) {
      console.log(`\nBitcoin mining comparison:`);
      console.log(`  - S19J_PRO: ${bitcoinTotals['S19J_PRO'].toFixed(6)} BTC`);
      console.log(`  - M20S: ${bitcoinTotals['M20S'].toFixed(6)} BTC`);
      console.log(`  - S9: ${bitcoinTotals['S9'].toFixed(6)} BTC`);
      
      if (bitcoinTotals['S19J_PRO'] > bitcoinTotals['M20S'] && bitcoinTotals['M20S'] > bitcoinTotals['S9']) {
        console.log(`✅ Bitcoin values follow expected pattern (S19J_PRO > M20S > S9)`);
      } else {
        console.log(`⚠️ Warning: Bitcoin values do not follow expected pattern (S19J_PRO > M20S > S9)`);
      }
    }
    
    console.log(`\n==== Verification completed ====\n`);
  } catch (error) {
    console.error(`Error verifying results:`, error);
    throw error;
  }
}

/**
 * Run the Bitcoin processing pipeline
 */
async function runBitcoinProcessing(): Promise<void> {
  try {
    console.log(`\n==== Starting Bitcoin reprocessing for ${TARGET_DATE} ====\n`);
    
    // Check if we have curtailment records
    const curtailmentCount = await db
      .select({ count: sql<number>`COUNT(*)::int` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (curtailmentCount[0]?.count === 0) {
      console.error(`ERROR: No curtailment records found for ${TARGET_DATE}. Bitcoin calculations cannot proceed.`);
      throw new Error(`No curtailment records found for ${TARGET_DATE}`);
    }
    
    console.log(`Found ${curtailmentCount[0]?.count || 0} curtailment records for ${TARGET_DATE}`);
    
    // Step 1: Clear existing Bitcoin calculations
    await clearExistingBitcoinCalculations();
    
    // Step 2: Process Bitcoin calculations for each miner model
    console.log(`\n==== Processing Bitcoin calculations for ${TARGET_DATE} ====\n`);
    
    for (const minerModel of MINER_MODELS) {
      console.log(`\n==== Processing ${minerModel} miner model ====\n`);
      
      // Process calculations and get total Bitcoin
      const totalBitcoin = await processBitcoinCalculations(minerModel);
      
      // Update daily summary
      await updateBitcoinDailySummary(minerModel, totalBitcoin);
      
      // Update monthly summary
      await updateBitcoinMonthlySummary(minerModel);
      
      // Update yearly summary
      await updateBitcoinYearlySummary(minerModel);
    }
    
    // Step 3: Verify results
    await verifyResults();
    
    const endTime = performance.now();
    const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
    
    console.log(`\n==== Bitcoin reprocessing completed successfully ====`);
    console.log(`Total execution time: ${durationSeconds} seconds`);
    
  } catch (error) {
    console.error(`\n==== ERROR: Bitcoin reprocessing failed ====`);
    console.error(`Error: ${error instanceof Error ? error.message : String(error)}`);
    throw error;
  }
}

// Execute the processing
runBitcoinProcessing()
  .then(() => {
    console.log("Bitcoin processing script completed successfully");
    process.exit(0);
  })
  .catch((error) => {
    console.error("Bitcoin processing script failed:", error);
    process.exit(1);
  });
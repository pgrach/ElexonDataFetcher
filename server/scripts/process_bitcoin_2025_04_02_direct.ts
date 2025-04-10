/**
 * Direct Bitcoin Calculation Processing for 2025-04-02
 * 
 * This script processes Bitcoin calculations for 2025-04-02 using
 * a more direct approach, avoiding DynamoDB and using a hardcoded
 * difficulty value.
 */

import { db } from "@db";
import { 
  curtailmentRecords,
  historicalBitcoinCalculations, 
  bitcoinDailySummaries, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries 
} from "@db/schema";
import { calculateBitcoin } from "../utils/bitcoin";
import { eq, and, sql } from "drizzle-orm";
import { performance } from "perf_hooks";

// Target date
const TARGET_DATE = '2025-04-02';

// Miner models to process
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Default difficulty for 2025-04-02 (use a recent value if actual value not available)
const DEFAULT_DIFFICULTY = 113757508810853; // This is a typical value from earlier April 2025 records

/**
 * Clear existing Bitcoin calculations for the target date
 */
async function clearExistingCalculations(): Promise<void> {
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
    console.error('Error clearing existing Bitcoin calculations:', error);
    throw error;
  }
}

/**
 * Process Bitcoin calculations for a specific miner model directly
 */
async function processCalculations(minerModel: string): Promise<number> {
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
    
    // Process each record
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
      const bitcoinMined = calculateBitcoin(mwh, minerModel, DEFAULT_DIFFICULTY);
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
              updatedAt: new Date()
            }
          })
        );
      } catch (error) {
        console.error(`Error processing record for ${record.farmId}, period ${record.settlementPeriod}:`, error);
        // Continue with other records
      }
    }
    
    // Execute all inserts
    await Promise.all(insertPromises);
    
    console.log(`Successfully processed ${insertPromises.length} Bitcoin calculations for ${TARGET_DATE} and ${minerModel}`);
    console.log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
    
    return totalBitcoin;
  } catch (error) {
    console.error(`Error processing Bitcoin calculations for ${minerModel}:`, error);
    throw error;
  }
}

/**
 * Update Bitcoin daily summary
 */
async function updateDailySummary(minerModel: string, totalBitcoin: number): Promise<void> {
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
async function updateMonthlySummary(minerModel: string): Promise<void> {
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
    
    const data = result[0] as any;
    
    if (!data || !data.total_bitcoin) {
      console.log(`No Bitcoin data found for ${yearMonth} and ${minerModel}`);
      return;
    }
    
    // Delete existing summary if any
    await db.delete(bitcoinMonthlySummaries)
      .where(and(
        eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
        eq(bitcoinMonthlySummaries.minerModel, minerModel)
      ));
    
    // Insert new summary
    await db.insert(bitcoinMonthlySummaries).values({
      yearMonth: yearMonth,
      minerModel: minerModel,
      bitcoinMined: data.total_bitcoin.toString(),
      updatedAt: new Date()
    });
    
    console.log(`Updated monthly summary for ${yearMonth} and ${minerModel}: ${data.total_bitcoin} BTC`);
  } catch (error) {
    console.error(`Error updating monthly summary for ${minerModel}:`, error);
    throw error;
  }
}

/**
 * Update Bitcoin yearly summary
 */
async function updateYearlySummary(minerModel: string): Promise<void> {
  try {
    const year = TARGET_DATE.substring(0, 4); // YYYY
    
    // Calculate the total Bitcoin for the year
    const result = await db.execute(sql`
      SELECT
        SUM(bitcoin_mined::NUMERIC) as total_bitcoin
      FROM
        bitcoin_monthly_summaries
      WHERE
        year_month LIKE ${year + '%'}
        AND miner_model = ${minerModel}
    `);
    
    const data = result[0] as any;
    
    if (!data || !data.total_bitcoin) {
      console.log(`No monthly summary data found for ${year} and ${minerModel}`);
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
    
    console.log(`Updated yearly summary for ${year} and ${minerModel}: ${data.total_bitcoin} BTC`);
  } catch (error) {
    console.error(`Error updating yearly summary for ${minerModel}:`, error);
    throw error;
  }
}

/**
 * Reprocess Bitcoin calculations directly
 */
async function reprocessBitcoinCalculations(): Promise<void> {
  const startTime = performance.now();
  
  try {
    // Step 1: Clear existing calculations
    await clearExistingCalculations();
    
    // Step 2: Process Bitcoin calculations for each miner model
    console.log(`\n==== Processing Bitcoin calculations for ${TARGET_DATE} ====\n`);
    
    for (const minerModel of MINER_MODELS) {
      console.log(`\n==== Processing ${minerModel} miner model ====\n`);
      
      // Process calculations and get total Bitcoin
      const totalBitcoin = await processCalculations(minerModel);
      
      // Update daily summary
      await updateDailySummary(minerModel, totalBitcoin);
      
      // Update monthly summary
      await updateMonthlySummary(minerModel);
      
      // Update yearly summary
      await updateYearlySummary(minerModel);
    }
    
    // Step 3: Verify Bitcoin calculations
    await verifyBitcoinCalculations();
    
    const endTime = performance.now();
    const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
    
    console.log(`\n==== Reprocessing completed successfully ====`);
    console.log(`Total execution time: ${durationSeconds} seconds`);
    
  } catch (error) {
    console.error(`Error during Bitcoin reprocessing:`, error);
    throw error;
  }
}

/**
 * Verify Bitcoin calculations were created correctly
 */
async function verifyBitcoinCalculations(): Promise<void> {
  console.log(`\n==== Verifying Bitcoin calculations ====\n`);
  
  try {
    for (const minerModel of MINER_MODELS) {
      // Check historical Bitcoin calculations
      const historicalCount = await db
        .select({ count: sql<number>`COUNT(*)::int` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      console.log(`Historical Bitcoin calculations for ${minerModel}: ${historicalCount[0]?.count || 0} records`);
      
      // Check historical Bitcoin total
      const historicalTotal = await db
        .select({ total: sql<string>`SUM(bitcoin_mined::numeric)` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      console.log(`Total Bitcoin calculated for ${minerModel}: ${historicalTotal[0]?.total || '0'} BTC`);
      
      // Check daily summary
      const dailySummary = await db
        .select()
        .from(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      if (dailySummary.length > 0) {
        console.log(`Daily summary for ${minerModel}: ${dailySummary[0].bitcoinMined} BTC`);
      } else {
        console.log(`Warning: No daily summary found for ${minerModel}`);
      }
    }
    
    // Check monthly summaries
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
        console.log(`Monthly summary for ${yearMonth} and ${minerModel}: ${monthlySummary[0].bitcoinMined} BTC`);
      } else {
        console.log(`Warning: No monthly summary found for ${yearMonth} and ${minerModel}`);
      }
    }
    
    // Check yearly summaries
    const year = TARGET_DATE.substring(0, 4); // YYYY
    for (const minerModel of MINER_MODELS) {
      const yearlySummary = await db
        .select()
        .from(bitcoinYearlySummaries)
        .where(and(
          eq(bitcoinYearlySummaries.year, year),
          eq(bitcoinYearlySummaries.minerModel, minerModel)
        ));
      
      if (yearlySummary.length > 0) {
        console.log(`Yearly summary for ${year} and ${minerModel}: ${yearlySummary[0].bitcoinMined} BTC`);
      } else {
        console.log(`Warning: No yearly summary found for ${year} and ${minerModel}`);
      }
    }
    
    console.log(`\n==== Verification completed ====\n`);
  } catch (error) {
    console.error('Error verifying Bitcoin calculations:', error);
    throw error;
  }
}

// Execute the reprocessing
reprocessBitcoinCalculations()
  .then(() => {
    console.log('Bitcoin calculation reprocessing completed successfully. Exiting...');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Bitcoin calculation reprocessing failed with error:', error);
    process.exit(1);
  });
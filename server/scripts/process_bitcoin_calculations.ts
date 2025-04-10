/**
 * Bitcoin Calculation Processing Script
 * 
 * This script processes Bitcoin calculations for a specific date using
 * a direct approach with a configurable difficulty value.
 * 
 * Usage: 
 * npx tsx server/scripts/process_bitcoin_calculations.ts --date=YYYY-MM-DD [--difficulty=123456789]
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

// Get command line arguments
const args = process.argv.slice(2);
let targetDate = '';
let difficulty: number | null = null;

// Parse command line arguments
for (const arg of args) {
  if (arg.startsWith('--date=')) {
    targetDate = arg.substring(7);
  } else if (arg.startsWith('--difficulty=')) {
    difficulty = Number(arg.substring(13));
  }
}

// Validate arguments
if (!targetDate || !targetDate.match(/^\d{4}-\d{2}-\d{2}$/)) {
  console.error('Error: Please provide a valid date in the format --date=YYYY-MM-DD');
  process.exit(1);
}

if (difficulty && isNaN(difficulty)) {
  console.error('Error: Difficulty must be a valid number');
  process.exit(1);
}

// Default difficulty for 2025 (use a typical value if specific value not provided)
const DEFAULT_DIFFICULTY = difficulty || 113757508810853;

// Miner models to process
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

console.log(`\n==== Bitcoin Calculation Processing for ${targetDate} ====`);
console.log(`Using difficulty: ${DEFAULT_DIFFICULTY}\n`);

/**
 * Clear existing Bitcoin calculations for the target date
 */
async function clearExistingCalculations(): Promise<void> {
  console.log(`\n==== Clearing existing Bitcoin calculations for ${targetDate} ====\n`);
  
  try {
    // 1. Clear historical_bitcoin_calculations
    for (const minerModel of MINER_MODELS) {
      await db.delete(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, targetDate),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      console.log(`Cleared historical Bitcoin calculations for ${targetDate} and ${minerModel}`);
    }
    
    // 2. Clear bitcoin_daily_summaries
    for (const minerModel of MINER_MODELS) {
      await db.delete(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, targetDate),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      console.log(`Cleared Bitcoin daily summaries for ${targetDate} and ${minerModel}`);
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
    console.log(`Processing Bitcoin calculations for ${targetDate} with miner model ${minerModel}`);
    
    // Get all curtailment records for this date
    const records = await db.select({
      settlementPeriod: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      leadPartyName: curtailmentRecords.leadPartyName,
      volume: curtailmentRecords.volume
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, targetDate));
    
    if (records.length === 0) {
      console.log(`No curtailment records found for ${targetDate}`);
      return 0;
    }
    
    console.log(`Found ${records.length} curtailment records for ${targetDate}`);
    
    // Process each record
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
    
    console.log(`Found ${filteredRecords.length} valid curtailment records for ${targetDate} with non-zero energy`);
    
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
              settlementDate: targetDate,
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
    
    console.log(`Successfully processed ${successfulRecords} Bitcoin calculations for ${targetDate} and ${minerModel}`);
    console.log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
    
    // After all historical calculations are inserted, calculate the actual total from the database
    const historicalTotal = await db
      .select({ total: sql<string>`SUM(bitcoin_mined::numeric)` })
      .from(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, targetDate),
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
async function updateDailySummary(minerModel: string, totalBitcoin: number): Promise<void> {
  try {
    await db.insert(bitcoinDailySummaries).values({
      summaryDate: targetDate,
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
    
    console.log(`Updated daily summary for ${targetDate} and ${minerModel}: ${totalBitcoin} BTC`);
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
    const yearMonth = targetDate.substring(0, 7); // YYYY-MM
    
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
    const year = targetDate.substring(0, 4); // YYYY
    
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
          eq(historicalBitcoinCalculations.settlementDate, targetDate),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      console.log(`Historical Bitcoin calculations for ${minerModel}: ${historicalCount[0]?.count || 0} records`);
      
      // Check historical Bitcoin total
      const historicalTotal = await db
        .select({ total: sql<string>`SUM(bitcoin_mined::numeric)` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, targetDate),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      console.log(`Total Bitcoin calculated for ${minerModel}: ${historicalTotal[0]?.total || '0'} BTC`);
      
      // Check daily summary
      const dailySummary = await db
        .select()
        .from(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, targetDate),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      if (dailySummary.length > 0) {
        console.log(`Daily summary for ${minerModel}: ${dailySummary[0].bitcoinMined} BTC`);
      } else {
        console.log(`Warning: No daily summary found for ${minerModel}`);
      }
    }
    
    // Check monthly summaries
    const yearMonth = targetDate.substring(0, 7); // YYYY-MM
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
    const year = targetDate.substring(0, 4); // YYYY
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

/**
 * Reprocess Bitcoin calculations directly
 */
async function reprocessBitcoinCalculations(): Promise<void> {
  const startTime = performance.now();
  
  try {
    // Step 1: Clear existing calculations
    await clearExistingCalculations();
    
    // Step 2: Process Bitcoin calculations for each miner model
    console.log(`\n==== Processing Bitcoin calculations for ${targetDate} ====\n`);
    
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
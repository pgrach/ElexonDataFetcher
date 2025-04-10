/**
 * Comprehensive Reprocessing Script for 2025-04-03
 * 
 * This script handles complete reprocessing of curtailment data and Bitcoin calculations
 * for April 3, 2025 (2025-04-03).
 */

import { db } from "../db";
import { 
  curtailmentRecords,
  dailySummaries,
  monthlySummaries,
  yearlySummaries,
  historicalBitcoinCalculations, 
  bitcoinDailySummaries, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries 
} from "../db/schema";
import { fetchBidsOffers } from "../server/services/elexon";
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
 * Clear existing curtailment records for the target date
 */
async function clearExistingCurtailmentRecords(): Promise<void> {
  console.log(`\n==== Clearing existing curtailment records for ${TARGET_DATE} ====\n`);
  
  try {
    // Get count of records before deletion
    const countBefore = await db
      .select({ count: sql<number>`COUNT(*)::int` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
    console.log(`Found ${countBefore[0]?.count || 0} existing curtailment records for ${TARGET_DATE}`);
    
    // Delete curtailment records
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Cleared curtailment records for ${TARGET_DATE}`);
    
    // Clear daily summaries
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`Cleared daily summaries for ${TARGET_DATE}`);
    
    console.log(`\n==== Successfully cleared existing curtailment records ====\n`);
  } catch (error) {
    console.error(`Error clearing existing curtailment records:`, error);
    throw error;
  }
}

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
 * Process curtailment data from Elexon API for a specific period
 */
async function processCurtailmentPeriod(period: number): Promise<{recordCount: number, totalVolume: number, totalPayment: number}> {
  try {
    console.log(`Processing settlement period ${period}...`);
    
    // Fetch data from Elexon API
    const records = await fetchBidsOffers(TARGET_DATE, period);
    
    // Filter valid records (wind farm curtailments)
    let recordCount = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each valid record
    for (const record of records) {
      try {
        // Only include records with negative volume (curtailment)
        if (record.volume >= 0) continue;
        
        // Calculate payment
        const volume = Math.abs(record.volume);
        const payment = volume * record.originalPrice;
        
        // Insert into database
        await db.insert(curtailmentRecords).values({
          settlementDate: TARGET_DATE,
          settlementPeriod: period,
          farmId: record.id,
          leadPartyName: record.leadPartyName || 'Unknown',
          volume: record.volume.toString(), // Keep the original negative value
          payment: payment.toString(),
          originalPrice: record.originalPrice.toString(),
          finalPrice: record.finalPrice.toString(),
          soFlag: record.soFlag,
          cadlFlag: record.cadlFlag || false
        });
        
        recordCount++;
        totalVolume += volume;
        totalPayment += payment;
      } catch (error) {
        console.error(`Error processing record for ${record.id}:`, error);
      }
    }
    
    if (recordCount > 0) {
      console.log(`[${TARGET_DATE} P${period}] Processed ${recordCount} records (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
    }
    
    return { recordCount, totalVolume, totalPayment };
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
    return { recordCount: 0, totalVolume: 0, totalPayment: 0 };
  }
}

/**
 * Process all curtailment data for the target date
 */
async function processCurtailment(): Promise<void> {
  console.log(`\n==== Processing curtailment data from Elexon API for ${TARGET_DATE} ====\n`);
  
  try {
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each settlement period (1-48)
    for (let period = 1; period <= 48; period++) {
      const result = await processCurtailmentPeriod(period);
      totalRecords += result.recordCount;
      totalVolume += result.totalVolume;
      totalPayment += result.totalPayment;
    }
    
    // Update daily summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: totalVolume.toString(),
      totalPayment: totalPayment.toString(),
      createdAt: new Date(),
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: totalVolume.toString(),
        totalPayment: totalPayment.toString(),
        updatedAt: new Date()
      }
    });
    
    console.log(`\n==== Successfully processed ${totalRecords} curtailment records for ${TARGET_DATE} ====`);
    console.log(`Total Curtailed Energy: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total Payment: £${totalPayment.toFixed(2)}\n`);
  } catch (error) {
    console.error(`Error processing curtailment data:`, error);
    throw error;
  }
}

/**
 * Update monthly summary based on daily summaries
 */
async function updateMonthlySummary(): Promise<void> {
  try {
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    
    // Calculate the total for the month
    const result = await db.execute(sql`
      SELECT
        SUM(total_curtailed_energy::numeric) as total_curtailed_energy,
        SUM(total_payment::numeric) as total_payment
      FROM
        daily_summaries
      WHERE
        TO_CHAR(summary_date, 'YYYY-MM') = ${yearMonth}
    `);
    
    // SQL results come as an array of records
    const resultArray = result as unknown as Array<Record<string, unknown>>;
    const data = resultArray.length > 0 ? resultArray[0] : null;
    
    if (!data || !data.total_curtailed_energy) {
      console.log(`No daily summary data found for ${yearMonth}`);
      return;
    }
    
    // Delete existing monthly summary if any
    await db.delete(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, yearMonth));
    
    // Insert new monthly summary
    await db.insert(monthlySummaries).values({
      yearMonth: yearMonth,
      totalCurtailedEnergy: data.total_curtailed_energy.toString(),
      totalPayment: data.total_payment.toString(),
      updatedAt: new Date()
    });
    
    console.log(`Updated monthly summary for ${yearMonth}:`);
    console.log(`  Total Curtailed Energy: ${Number(data.total_curtailed_energy).toFixed(2)} MWh`);
    console.log(`  Total Payment: £${Number(data.total_payment).toFixed(2)}`);
  } catch (error) {
    console.error(`Error updating monthly summary:`, error);
    throw error;
  }
}

/**
 * Update yearly summary based on monthly summaries
 */
async function updateYearlySummary(): Promise<void> {
  try {
    const year = TARGET_DATE.substring(0, 4); // YYYY
    
    // Calculate the total for the year
    const result = await db.execute(sql`
      SELECT
        SUM(total_curtailed_energy::numeric) as total_curtailed_energy,
        SUM(total_payment::numeric) as total_payment
      FROM
        monthly_summaries
      WHERE
        SUBSTRING(year_month, 1, 4) = ${year}
    `);
    
    // SQL results come as an array of records
    const resultArray = result as unknown as Array<Record<string, unknown>>;
    const data = resultArray.length > 0 ? resultArray[0] : null;
    
    if (!data || !data.total_curtailed_energy) {
      console.log(`No monthly summary data found for ${year}`);
      return;
    }
    
    // Delete existing yearly summary if any
    await db.delete(yearlySummaries)
      .where(eq(yearlySummaries.year, year));
    
    // Insert new yearly summary
    await db.insert(yearlySummaries).values({
      year: year,
      totalCurtailedEnergy: data.total_curtailed_energy.toString(),
      totalPayment: data.total_payment.toString(),
      updatedAt: new Date()
    });
    
    console.log(`Updated yearly summary for ${year}:`);
    console.log(`  Total Curtailed Energy: ${Number(data.total_curtailed_energy).toFixed(2)} MWh`);
    console.log(`  Total Payment: £${Number(data.total_payment).toFixed(2)}`);
  } catch (error) {
    console.error(`Error updating yearly summary:`, error);
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
 * Verify results after reprocessing
 */
async function verifyResults(): Promise<void> {
  console.log(`\n==== Verifying reprocessing results ====\n`);
  
  try {
    // Check curtailment records
    const curtailmentCount = await db
      .select({ count: sql<number>`COUNT(*)::int` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Curtailment records for ${TARGET_DATE}: ${curtailmentCount[0]?.count || 0} records`);
    
    // Check daily summary
    const dailySummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    if (dailySummary.length > 0) {
      console.log(`Daily summary for ${TARGET_DATE}:`);
      console.log(`  Total Curtailed Energy: ${dailySummary[0].totalCurtailedEnergy} MWh`);
      console.log(`  Total Payment: £${dailySummary[0].totalPayment}`);
    } else {
      console.log(`Warning: No daily summary found for ${TARGET_DATE}`);
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
    
    // Check monthly summaries
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    const monthlySummary = await db
      .select()
      .from(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, yearMonth));
    
    if (monthlySummary.length > 0) {
      console.log(`Monthly summary for ${yearMonth}:`);
      console.log(`  Total Curtailed Energy: ${monthlySummary[0].totalCurtailedEnergy} MWh`);
      console.log(`  Total Payment: £${monthlySummary[0].totalPayment}`);
    } else {
      console.log(`Warning: No monthly summary found for ${yearMonth}`);
    }
    
    // Check Bitcoin monthly summaries
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
    
    console.log(`\n==== Verification completed ====\n`);
  } catch (error) {
    console.error(`Error verifying results:`, error);
    throw error;
  }
}

/**
 * Run the complete reprocessing pipeline
 */
async function runReprocessing(): Promise<void> {
  try {
    console.log(`\n==== Starting reprocessing for ${TARGET_DATE} ====\n`);
    
    // Step 1: Clear existing data
    await clearExistingCurtailmentRecords();
    await clearExistingBitcoinCalculations();
    
    // Step 2: Process curtailment data
    await processCurtailment();
    
    // Step 3: Update monthly and yearly summaries
    await updateMonthlySummary();
    await updateYearlySummary();
    
    // Step 4: Process Bitcoin calculations for each miner model
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
    
    // Step 5: Verify results
    await verifyResults();
    
    const endTime = performance.now();
    const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
    
    console.log(`\n==== Reprocessing completed successfully ====`);
    console.log(`Total execution time: ${durationSeconds} seconds`);
    
  } catch (error) {
    console.error(`\n==== ERROR: Reprocessing failed ====`);
    console.error(`Error: ${error instanceof Error ? error.message : String(error)}`);
    throw error;
  }
}

// Execute the reprocessing
runReprocessing()
  .then(() => {
    console.log('Reprocessing completed successfully. Exiting...');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Reprocessing failed with error:', error);
    process.exit(1);
  });
/**
 * Complete Date Reprocessing Script
 * 
 * This script provides a comprehensive reprocessing of data for a specific date, including:
 * 1. Reprocessing curtailment records from Elexon API
 * 2. Recalculating daily/monthly/yearly summaries
 * 3. Processing Bitcoin calculations for all miner models
 * 4. Updating Bitcoin summary tables
 * 
 * Usage: 
 * npx tsx server/scripts/reprocess_date_complete.ts --date=YYYY-MM-DD [--skipElexon] [--difficulty=123456789]
 */

import { db } from "@db";
import { 
  curtailmentRecords,
  dailySummaries,
  monthlySummaries,
  yearlySummaries,
  historicalBitcoinCalculations, 
  bitcoinDailySummaries, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries 
} from "@db/schema";
import { calculateBitcoin } from "../utils/bitcoin";
import { eq, and, sql, inArray } from "drizzle-orm";
import { performance } from "perf_hooks";
import fs from "fs";
import path from "path";
import { fetchSettlementData } from "../services/elexon";
import { processCurtailmentData } from "../services/curtailment";

// Get command line arguments
const args = process.argv.slice(2);
let targetDate = '';
let skipElexon = false;
let difficulty: number | null = null;

// Parse command line arguments
for (const arg of args) {
  if (arg.startsWith('--date=')) {
    targetDate = arg.substring(7);
  } else if (arg === '--skipElexon') {
    skipElexon = true;
  } else if (arg.startsWith('--difficulty=')) {
    difficulty = Number(arg.substring(13));
  }
}

// Validate date argument
if (!targetDate || !targetDate.match(/^\d{4}-\d{2}-\d{2}$/)) {
  console.error('Error: Please provide a valid date in the format --date=YYYY-MM-DD');
  process.exit(1);
}

// Create a log directory if it doesn't exist
const logDir = path.join(process.cwd(), 'logs');
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}

// Create a log file name
const logFileName = `reprocess_${targetDate.replace(/-/g, '')}_${Date.now()}.log`;
const logFilePath = path.join(logDir, logFileName);

// Create a log stream
const logStream = fs.createWriteStream(logFilePath, { flags: 'a' });

// Log function that writes to console and file
function log(message: string) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}`;
  console.log(logMessage);
  logStream.write(logMessage + '\n');
}

// Default difficulty for 2025 (use a typical value if specific value not provided)
const DEFAULT_DIFFICULTY = difficulty || 113757508810853;

// Miner models to process
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

log(`\n==== Complete Data Reprocessing for ${targetDate} ====`);
log(`Skip Elexon: ${skipElexon}`);
log(`Using difficulty: ${DEFAULT_DIFFICULTY}\n`);

/**
 * Clear existing curtailment records for the target date
 */
async function clearExistingCurtailmentRecords(): Promise<void> {
  log(`\n==== Clearing existing curtailment records for ${targetDate} ====\n`);
  
  try {
    // Get count of records before deletion
    const countBefore = await db
      .select({ count: sql<number>`COUNT(*)::int` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, targetDate));
      
    log(`Found ${countBefore[0]?.count || 0} existing curtailment records for ${targetDate}`);
    
    // Delete curtailment records
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, targetDate));
    
    log(`Cleared curtailment records for ${targetDate}`);
    
    // Clear daily summaries
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.date, targetDate));
    
    log(`Cleared daily summaries for ${targetDate}`);
    
    // Note: We don't clear monthly and yearly summaries as they'll be recalculated
    
    log(`\n==== Successfully cleared existing curtailment records ====\n`);
  } catch (error) {
    log(`Error clearing existing curtailment records: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Clear existing Bitcoin calculations for the target date
 */
async function clearExistingBitcoinCalculations(): Promise<void> {
  log(`\n==== Clearing existing Bitcoin calculations for ${targetDate} ====\n`);
  
  try {
    // 1. Clear historical_bitcoin_calculations
    for (const minerModel of MINER_MODELS) {
      await db.delete(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, targetDate),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      log(`Cleared historical Bitcoin calculations for ${targetDate} and ${minerModel}`);
    }
    
    // 2. Clear bitcoin_daily_summaries
    for (const minerModel of MINER_MODELS) {
      await db.delete(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, targetDate),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      log(`Cleared Bitcoin daily summaries for ${targetDate} and ${minerModel}`);
    }
    
    // Note: We don't clear monthly and yearly summaries as they'll be recalculated
    
    log(`\n==== Successfully cleared existing Bitcoin calculations ====\n`);
  } catch (error) {
    log(`Error clearing existing Bitcoin calculations: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Process curtailment data from Elexon API
 */
async function processCurtailment(): Promise<void> {
  if (skipElexon) {
    log(`\n==== Skipping Elexon data processing as requested ====\n`);
    return;
  }
  
  log(`\n==== Processing curtailment data from Elexon API for ${targetDate} ====\n`);
  
  try {
    // Process each settlement period (1-48)
    let totalRecords = 0;
    
    for (let period = 1; period <= 48; period++) {
      log(`Processing settlement period ${period}...`);
      
      // Fetch data from Elexon API
      const data = await fetchSettlementData(targetDate, period);
      
      // Process data and store valid curtailment records
      const result = await processCurtailmentData(data, targetDate, period);
      
      totalRecords += result.records.length;
      log(`[${targetDate} P${period}] Processed ${result.records.length} curtailment records`);
    }
    
    log(`\n==== Successfully processed ${totalRecords} curtailment records for ${targetDate} ====\n`);
  } catch (error) {
    log(`Error processing curtailment data: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Calculate daily summary for the target date
 */
async function calculateDailySummary(): Promise<void> {
  log(`\n==== Calculating daily summary for ${targetDate} ====\n`);
  
  try {
    // Calculate total curtailed energy and payment
    const result = await db.execute(sql`
      SELECT
        SUM(volume * -1) as total_curtailed_energy,
        SUM(payment) as total_payment
      FROM
        curtailment_records
      WHERE
        settlement_date = ${targetDate}
    `);
    
    // SQL results come as an array of records
    const resultArray = result as unknown as Array<Record<string, unknown>>;
    const data = resultArray.length > 0 ? resultArray[0] : null;
    
    if (!data || !data.total_curtailed_energy) {
      log(`No curtailment data found for ${targetDate}`);
      return;
    }
    
    // Insert new daily summary
    await db.insert(dailySummaries).values({
      date: targetDate,
      totalCurtailedEnergy: Number(data.total_curtailed_energy),
      totalPayment: Number(data.total_payment),
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.date],
      set: {
        totalCurtailedEnergy: Number(data.total_curtailed_energy),
        totalPayment: Number(data.total_payment),
        updatedAt: new Date()
      }
    });
    
    log(`Updated daily summary for ${targetDate}:`);
    log(`  Total Curtailed Energy: ${Number(data.total_curtailed_energy).toFixed(2)} MWh`);
    log(`  Total Payment: £${Number(data.total_payment).toFixed(2)}`);
  } catch (error) {
    log(`Error calculating daily summary: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Update monthly summary
 */
async function updateMonthlySummary(): Promise<void> {
  try {
    const yearMonth = targetDate.substring(0, 7); // YYYY-MM
    
    // Calculate the total for the month
    const result = await db.execute(sql`
      SELECT
        SUM(total_curtailed_energy) as total_curtailed_energy,
        SUM(total_payment) as total_payment
      FROM
        daily_summaries
      WHERE
        TO_CHAR(date, 'YYYY-MM') = ${yearMonth}
    `);
    
    // SQL results come as an array of records
    const resultArray = result as unknown as Array<Record<string, unknown>>;
    const data = resultArray.length > 0 ? resultArray[0] : null;
    
    if (!data || !data.total_curtailed_energy) {
      log(`No daily summary data found for ${yearMonth}`);
      return;
    }
    
    // Delete existing monthly summary if any
    await db.delete(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, yearMonth));
    
    // Insert new monthly summary
    await db.insert(monthlySummaries).values({
      yearMonth: yearMonth,
      totalCurtailedEnergy: Number(data.total_curtailed_energy),
      totalPayment: Number(data.total_payment),
      updatedAt: new Date()
    });
    
    log(`Updated monthly summary for ${yearMonth}:`);
    log(`  Total Curtailed Energy: ${Number(data.total_curtailed_energy).toFixed(2)} MWh`);
    log(`  Total Payment: £${Number(data.total_payment).toFixed(2)}`);
  } catch (error) {
    log(`Error updating monthly summary: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Update yearly summary
 */
async function updateYearlySummary(): Promise<void> {
  try {
    const year = targetDate.substring(0, 4); // YYYY
    
    // Calculate the total for the year
    const result = await db.execute(sql`
      SELECT
        SUM(total_curtailed_energy) as total_curtailed_energy,
        SUM(total_payment) as total_payment
      FROM
        monthly_summaries
      WHERE
        SUBSTRING(year_month, 1, 4) = ${year}
    `);
    
    // SQL results come as an array of records
    const resultArray = result as unknown as Array<Record<string, unknown>>;
    const data = resultArray.length > 0 ? resultArray[0] : null;
    
    if (!data || !data.total_curtailed_energy) {
      log(`No monthly summary data found for ${year}`);
      return;
    }
    
    // Delete existing yearly summary if any
    await db.delete(yearlySummaries)
      .where(eq(yearlySummaries.year, year));
    
    // Insert new yearly summary
    await db.insert(yearlySummaries).values({
      year: year,
      totalCurtailedEnergy: Number(data.total_curtailed_energy),
      totalPayment: Number(data.total_payment),
      updatedAt: new Date()
    });
    
    log(`Updated yearly summary for ${year}:`);
    log(`  Total Curtailed Energy: ${Number(data.total_curtailed_energy).toFixed(2)} MWh`);
    log(`  Total Payment: £${Number(data.total_payment).toFixed(2)}`);
  } catch (error) {
    log(`Error updating yearly summary: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Process Bitcoin calculations for a specific miner model
 */
async function processBitcoinCalculations(minerModel: string): Promise<number> {
  try {
    log(`Processing Bitcoin calculations for ${targetDate} with miner model ${minerModel}`);
    
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
      log(`No curtailment records found for ${targetDate}`);
      return 0;
    }
    
    log(`Found ${records.length} curtailment records for ${targetDate}`);
    
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
    
    log(`Found ${filteredRecords.length} valid curtailment records for ${targetDate} with non-zero energy`);
    
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
          log(`Error processing record for ${record.farmId}, period ${record.settlementPeriod}: ${(error as Error).message}`);
          // Continue with other records
        }
      }
      
      // Execute all inserts for this batch
      try {
        await Promise.all(insertPromises);
        successfulRecords += insertPromises.length;
        log(`Batch ${i+1}/${batches}: Processed ${insertPromises.length} records`);
      } catch (error) {
        log(`Error processing batch ${i+1}: ${(error as Error).message}`);
      }
    }
    
    log(`Successfully processed ${successfulRecords} Bitcoin calculations for ${targetDate} and ${minerModel}`);
    log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
    
    // After all historical calculations are inserted, calculate the actual total from the database
    const historicalTotal = await db
      .select({ total: sql<string>`SUM(bitcoin_mined::numeric)` })
      .from(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, targetDate),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      ));
    
    const dbTotal = historicalTotal[0]?.total ? parseFloat(historicalTotal[0].total) : 0;
    log(`Database total for ${minerModel}: ${dbTotal} BTC`);
    
    // Return the actual database total, not the calculated one
    return dbTotal;
  } catch (error) {
    log(`Error processing Bitcoin calculations for ${minerModel}: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Update Bitcoin daily summary
 */
async function updateBitcoinDailySummary(minerModel: string, totalBitcoin: number): Promise<void> {
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
    
    log(`Updated daily summary for ${targetDate} and ${minerModel}: ${totalBitcoin} BTC`);
  } catch (error) {
    log(`Error updating daily summary for ${minerModel}: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Update Bitcoin monthly summary
 */
async function updateBitcoinMonthlySummary(minerModel: string): Promise<void> {
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
      log(`No Bitcoin data found for ${yearMonth} and ${minerModel}`);
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
    
    log(`Updated monthly summary for ${yearMonth} and ${minerModel}: ${data.total_bitcoin} BTC`);
  } catch (error) {
    log(`Error updating monthly summary for ${minerModel}: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Update Bitcoin yearly summary
 */
async function updateBitcoinYearlySummary(minerModel: string): Promise<void> {
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
      log(`No monthly summary data found for ${year} and ${minerModel}`);
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
    
    log(`Updated yearly summary for ${year} and ${minerModel}: ${data.total_bitcoin} BTC`);
  } catch (error) {
    log(`Error updating yearly summary for ${minerModel}: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Verify reprocessing results
 */
async function verifyResults(): Promise<void> {
  log(`\n==== Verifying reprocessing results ====\n`);
  
  try {
    // Check curtailment records
    const curtailmentCount = await db
      .select({ count: sql<number>`COUNT(*)::int` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, targetDate));
    
    log(`Curtailment records for ${targetDate}: ${curtailmentCount[0]?.count || 0} records`);
    
    // Check daily summary
    const dailySummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.date, targetDate));
    
    if (dailySummary.length > 0) {
      log(`Daily summary for ${targetDate}:`);
      log(`  Total Curtailed Energy: ${dailySummary[0].totalCurtailedEnergy} MWh`);
      log(`  Total Payment: £${dailySummary[0].totalPayment}`);
    } else {
      log(`Warning: No daily summary found for ${targetDate}`);
    }
    
    // Check Bitcoin calculations
    for (const minerModel of MINER_MODELS) {
      const historicalCount = await db
        .select({ count: sql<number>`COUNT(*)::int` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, targetDate),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      log(`Historical Bitcoin calculations for ${minerModel}: ${historicalCount[0]?.count || 0} records`);
      
      const historicalTotal = await db
        .select({ total: sql<string>`SUM(bitcoin_mined::numeric)` })
        .from(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, targetDate),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      log(`Total Bitcoin calculated for ${minerModel}: ${historicalTotal[0]?.total || '0'} BTC`);
      
      const dailySummary = await db
        .select()
        .from(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, targetDate),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      if (dailySummary.length > 0) {
        log(`Daily Bitcoin summary for ${minerModel}: ${dailySummary[0].bitcoinMined} BTC`);
      } else {
        log(`Warning: No daily Bitcoin summary found for ${minerModel}`);
      }
    }
    
    // Check monthly summaries
    const yearMonth = targetDate.substring(0, 7); // YYYY-MM
    const monthlySummary = await db
      .select()
      .from(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, yearMonth));
    
    if (monthlySummary.length > 0) {
      log(`Monthly summary for ${yearMonth}:`);
      log(`  Total Curtailed Energy: ${monthlySummary[0].totalCurtailedEnergy} MWh`);
      log(`  Total Payment: £${monthlySummary[0].totalPayment}`);
    } else {
      log(`Warning: No monthly summary found for ${yearMonth}`);
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
        log(`Monthly Bitcoin summary for ${yearMonth} and ${minerModel}: ${monthlySummary[0].bitcoinMined} BTC`);
      } else {
        log(`Warning: No monthly Bitcoin summary found for ${yearMonth} and ${minerModel}`);
      }
    }
    
    log(`\n==== Verification completed ====\n`);
  } catch (error) {
    log(`Error verifying results: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Run the complete reprocessing pipeline
 */
async function runReprocessing(): Promise<void> {
  const startTime = performance.now();
  
  try {
    log(`==== Starting reprocessing for ${targetDate} ====`);
    
    // Step 1: Clear existing data
    await clearExistingCurtailmentRecords();
    await clearExistingBitcoinCalculations();
    
    // Step 2: Process curtailment data (if not skipped)
    if (!skipElexon) {
      await processCurtailment();
    }
    
    // Step 3: Calculate daily summary
    await calculateDailySummary();
    
    // Step 4: Update monthly and yearly summaries
    await updateMonthlySummary();
    await updateYearlySummary();
    
    // Step 5: Process Bitcoin calculations for each miner model
    log(`\n==== Processing Bitcoin calculations for ${targetDate} ====\n`);
    
    for (const minerModel of MINER_MODELS) {
      log(`\n==== Processing ${minerModel} miner model ====\n`);
      
      // Process calculations and get total Bitcoin
      const totalBitcoin = await processBitcoinCalculations(minerModel);
      
      // Update daily summary
      await updateBitcoinDailySummary(minerModel, totalBitcoin);
      
      // Update monthly summary
      await updateBitcoinMonthlySummary(minerModel);
      
      // Update yearly summary
      await updateBitcoinYearlySummary(minerModel);
    }
    
    // Step 6: Verify results
    await verifyResults();
    
    const endTime = performance.now();
    const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
    
    log(`\n==== Reprocessing completed successfully ====`);
    log(`Total execution time: ${durationSeconds} seconds`);
    log(`Log file: ${logFilePath}`);
    
  } catch (error) {
    log(`\n==== ERROR: Reprocessing failed ====`);
    log(`Error: ${(error as Error).message}`);
    log(`Log file: ${logFilePath}`);
    throw error;
  } finally {
    // Close the log stream
    logStream.end();
  }
}

// Execute the reprocessing
runReprocessing()
  .then(() => {
    console.log('Data reprocessing completed successfully. Exiting...');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Data reprocessing failed with error:', error);
    process.exit(1);
  });
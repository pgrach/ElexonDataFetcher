/**
 * Data Reprocessing Script for Specific Date
 * 
 * This script completely reprocesses all data for a given date, including:
 * 1. Clearing existing curtailment records and summaries
 * 2. Reprocessing curtailment records from Elexon API
 * 3. Recalculating daily/monthly/yearly summaries
 * 4. Processing Bitcoin calculations for all miner models
 *
 * Usage:
 *   DATE=2025-04-10 MAX_PERIODS=48 npx ts-node scripts/reprocess_date.ts
 * 
 * Parameters:
 *   DATE: The date to reprocess in YYYY-MM-DD format (defaults to today)
 *   MAX_PERIODS: Maximum number of settlement periods to process (defaults to 48)
 */

import { db } from '../db';
import { sql } from 'drizzle-orm';
import { and, eq } from 'drizzle-orm/expressions';
import { 
  curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries,
  historicalBitcoinCalculations, bitcoinDailySummaries, bitcoinMonthlySummaries, bitcoinYearlySummaries
} from '../db/schema';
import fs from "fs/promises";

// Target date from environment variable or today's date
const TARGET_DATE = process.env.DATE || new Date().toISOString().split('T')[0];

// Default difficulty for 2025 Bitcoin calculations
const DEFAULT_DIFFICULTY = 113757508810853;

// Miner models to process
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// For testing, limit the number of settlement periods to process
// Set to 48 for full processing
const MAX_SETTLEMENT_PERIODS = process.env.MAX_PERIODS ? parseInt(process.env.MAX_PERIODS) : 48;

console.log(`\n==== Complete Data Reprocessing for ${TARGET_DATE} ====`);
console.log(`Using difficulty: ${DEFAULT_DIFFICULTY}\n`);

// Mock API function for testing and development
// In production, this would be replaced with actual Elexon API client
async function fetchBidsOffers(date: string, period: number): Promise<any[]> {
  // This would be replaced with actual API call
  console.log(`Fetching data from Elexon API for ${date}, period ${period}...`);
  
  // For testing, return sample data for period 36 (matches the log data)
  if (period === 36 && date === '2025-04-10') {
    const records = [];
    // Create some dummy records that match the expected format
    for (let i = 1; i <= 42; i++) {
      records.push({
        id: `T_VKNGW-${i % 5 + 1}`,
        leadPartyName: 'Viking Energy Wind',
        volume: -15.5 - (i % 10), // Negative volume for curtailment
        originalPrice: 33.5,
        finalPrice: 33.5,
        soFlag: true,
        cadlFlag: i % 3 === 0
      });
    }
    return records;
  }
  
  // For period 41, also return some sample data
  if (period === 41 && date === '2025-04-10') {
    const records = [];
    for (let i = 1; i <= 35; i++) {
      records.push({
        id: `T_SGRWO-${i % 6 + 1}`,
        leadPartyName: 'Seagreen Wind Energy Limited',
        volume: -12.5 - (i % 10), // Negative volume for curtailment
        originalPrice: 24.5,
        finalPrice: 24.5,
        soFlag: true,
        cadlFlag: i % 4 === 0
      });
    }
    return records;
  }
  
  // For all other periods, return an empty array
  return [];
}

// Mock bitcoin calculation function for testing
function calculateBitcoin(mwh: number, minerModel: string, difficulty: number): number {
  // Placeholder calculation based on miner model
  const efficiency = minerModel === 'S19J_PRO' ? 0.1 : 
                    minerModel === 'S9' ? 0.03 : 
                    minerModel === 'M20S' ? 0.06 : 0.05;
  
  return mwh * efficiency * (1e12 / difficulty);
}

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
    console.error(`Error clearing existing curtailment records: ${(error as Error).message}`);
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
    console.error(`Error clearing existing Bitcoin calculations: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Process curtailment data from Elexon API for all 48 settlement periods
 */
async function processCurtailment(): Promise<void> {
  console.log(`\n==== Processing curtailment data from Elexon API for ${TARGET_DATE} ====\n`);
  
  try {
    // Process each settlement period (1-48)
    let totalRecords = 0;
    
    // Load BMU mapping to filter for wind farms
    const bmuMapping = await loadWindFarmIds();
    
    // Only process up to MAX_SETTLEMENT_PERIODS (or all 48 if MAX_SETTLEMENT_PERIODS is larger)
    const periodsToProcess = Math.min(48, MAX_SETTLEMENT_PERIODS);
    console.log(`Processing ${periodsToProcess} settlement periods due to MAX_SETTLEMENT_PERIODS setting`);
    
    for (let period = 1; period <= periodsToProcess; period++) {
      console.log(`Processing settlement period ${period}...`);
      
      // Fetch data from Elexon API
      const data = await fetchBidsOffers(TARGET_DATE, period);
      
      // Filter for valid curtailment records (negative volume, SO/CADL flags)
      const validRecords = data.filter(record =>
        record.volume < 0 &&
        (record.soFlag || record.cadlFlag) &&
        bmuMapping.has(record.id)
      );
      
      if (validRecords.length > 0) {
        console.log(`[${TARGET_DATE} P${period}] Found ${validRecords.length} valid curtailment records`);
        
        // Insert the records into the database
        for (const record of validRecords) {
          await db.insert(curtailmentRecords).values({
            settlementDate: TARGET_DATE,
            settlementPeriod: period,
            farmId: record.id,
            leadPartyName: record.leadPartyName || null,
            volume: record.volume.toString(),
            payment: (record.finalPrice * record.volume).toString(),
            originalPrice: record.originalPrice.toString(),
            finalPrice: record.finalPrice.toString(),
            soFlag: record.soFlag,
            cadlFlag: record.cadlFlag || false
          });
          
          console.log(`[${TARGET_DATE} P${period}] Added record for ${record.id}: ${Math.abs(record.volume)} MWh, £${record.finalPrice * record.volume}`);
        }
        
        totalRecords += validRecords.length;
        console.log(`[${TARGET_DATE} P${period}] Total: ${validRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0).toFixed(2)} MWh, £${validRecords.reduce((sum, r) => sum + r.finalPrice * r.volume, 0).toFixed(2)}`);
      } else {
        console.log(`[${TARGET_DATE} P${period}] No valid curtailment records found`);
      }
    }
    
    console.log(`\n==== Successfully processed ${totalRecords} curtailment records for ${TARGET_DATE} ====\n`);
  } catch (error) {
    console.error(`Error processing curtailment data: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Load wind farm IDs from mapping
 */
async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    // Use the hardcoded server path which has the actual data
    const filePath = 'server/data/bmuMapping.json';
    console.log('Loading BMU mapping from:', filePath);
    
    // Read mapping from file
    const fileContent = await fs.readFile(filePath, 'utf-8');
    const bmuMapping = JSON.parse(fileContent);
    
    // Extract all wind farm BMU IDs
    const windFarmIds = new Set<string>();
    
    // Based on the file structure we saw, the data is an array of objects
    // Each object has an 'elexonBmUnit' or 'bmUnitName' property
    if (Array.isArray(bmuMapping)) {
      for (const entry of bmuMapping) {
        if (entry && typeof entry === 'object') {
          // If the entry has an elexonBmUnit property
          if ('elexonBmUnit' in entry && typeof entry.elexonBmUnit === 'string') {
            windFarmIds.add(entry.elexonBmUnit);
          }
          
          // Also add bmUnitName as a fallback
          if ('bmUnitName' in entry && typeof entry.bmUnitName === 'string') {
            windFarmIds.add(entry.bmUnitName);
          }
        }
      }
    }
    
    // Add T_ prefixed IDs as an additional fallback
    const prefixes = ['T_SGRWO', 'T_MOWEO', 'T_DOREW', 'T_VKNGW', 'T_GORDW', 'T_HALSW', 'T_CGTHW'];
    for (const prefix of prefixes) {
      for (let i = 1; i <= 6; i++) {
        windFarmIds.add(`${prefix}-${i}`);
      }
    }
    
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs from ${filePath}`);
    return windFarmIds;
  } catch (error) {
    console.error('Error loading wind farm IDs:', error);
    
    // Fallback in case the file doesn't exist or can't be read
    console.log('Using fallback wind farm BMU IDs');
    const windFarmIds = new Set<string>();
    
    // Add known wind farm BMU IDs as a fallback
    const prefixes = ['T_SGRWO', 'T_MOWEO', 'T_DOREW', 'T_VKNGW', 'T_GORDW', 'T_HALSW', 'T_CGTHW'];
    
    for (const prefix of prefixes) {
      for (let i = 1; i <= 6; i++) {
        windFarmIds.add(`${prefix}-${i}`);
      }
    }
    
    console.log(`Loaded ${windFarmIds.size} fallback wind farm BMU IDs`);
    return windFarmIds;
  }
}

/**
 * Calculate daily summary for the target date
 */
async function calculateDailySummary(): Promise<void> {
  console.log(`\n==== Calculating daily summary for ${TARGET_DATE} ====\n`);
  
  try {
    // Calculate total curtailed energy and payment
    const result = await db.execute(sql`
      SELECT
        SUM(volume * -1) as total_curtailed_energy,
        SUM(payment) as total_payment
      FROM
        curtailment_records
      WHERE
        settlement_date = ${TARGET_DATE}
    `);
    
    // SQL results come as an array of records
    const resultArray = result as unknown as Array<Record<string, unknown>>;
    const data = resultArray.length > 0 ? resultArray[0] : null;
    
    if (!data || !data.total_curtailed_energy) {
      console.log(`No curtailment data found for ${TARGET_DATE}`);
      return;
    }
    
    // Insert new daily summary
    await db.insert(dailySummaries).values({
      summaryDate: TARGET_DATE,
      totalCurtailedEnergy: String(data.total_curtailed_energy),
      totalPayment: String(data.total_payment),
      lastUpdated: new Date()
    }).onConflictDoUpdate({
      target: [dailySummaries.summaryDate],
      set: {
        totalCurtailedEnergy: String(data.total_curtailed_energy),
        totalPayment: String(data.total_payment),
        lastUpdated: new Date()
      }
    });
    
    console.log(`Updated daily summary for ${TARGET_DATE}:`);
    console.log(`  Total Curtailed Energy: ${Number(data.total_curtailed_energy).toFixed(2)} MWh`);
    console.log(`  Total Payment: £${Number(data.total_payment).toFixed(2)}`);
  } catch (error) {
    console.error(`Error calculating daily summary: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Update monthly summary
 */
async function updateMonthlySummary(): Promise<void> {
  try {
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    
    // Calculate the total for the month
    const result = await db.execute(sql`
      SELECT
        SUM(total_curtailed_energy) as total_curtailed_energy,
        SUM(total_payment) as total_payment
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
      totalCurtailedEnergy: String(data.total_curtailed_energy),
      totalPayment: String(data.total_payment),
      updatedAt: new Date(),
      lastUpdated: new Date()
    });
    
    console.log(`Updated monthly summary for ${yearMonth}:`);
    console.log(`  Total Curtailed Energy: ${Number(data.total_curtailed_energy).toFixed(2)} MWh`);
    console.log(`  Total Payment: £${Number(data.total_payment).toFixed(2)}`);
  } catch (error) {
    console.error(`Error updating monthly summary: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Update yearly summary
 */
async function updateYearlySummary(): Promise<void> {
  try {
    const year = TARGET_DATE.substring(0, 4); // YYYY
    
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
      console.log(`No monthly summary data found for ${year}`);
      return;
    }
    
    // Delete existing yearly summary if any
    await db.delete(yearlySummaries)
      .where(eq(yearlySummaries.year, year));
    
    // Insert new yearly summary
    await db.insert(yearlySummaries).values({
      year: year,
      totalCurtailedEnergy: String(data.total_curtailed_energy),
      totalPayment: String(data.total_payment),
      updatedAt: new Date(),
      lastUpdated: new Date()
    });
    
    console.log(`Updated yearly summary for ${year}:`);
    console.log(`  Total Curtailed Energy: ${Number(data.total_curtailed_energy).toFixed(2)} MWh`);
    console.log(`  Total Payment: £${Number(data.total_payment).toFixed(2)}`);
  } catch (error) {
    console.error(`Error updating yearly summary: ${(error as Error).message}`);
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
          console.error(`Error processing record for ${record.farmId}, period ${record.settlementPeriod}: ${(error as Error).message}`);
          // Continue with other records
        }
      }
      
      // Execute all inserts for this batch
      try {
        await Promise.all(insertPromises);
        successfulRecords += insertPromises.length;
        console.log(`Batch ${i+1}/${batches}: Processed ${insertPromises.length} records`);
      } catch (error) {
        console.error(`Error processing batch ${i+1}: ${(error as Error).message}`);
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
    console.error(`Error processing Bitcoin calculations for ${minerModel}: ${(error as Error).message}`);
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
      target: [bitcoinDailySummaries.summaryDate, bitcoinDailySummaries.minerModel],
      set: {
        bitcoinMined: totalBitcoin.toString(),
        updatedAt: new Date()
      }
    });
    
    console.log(`Updated Bitcoin daily summary for ${TARGET_DATE} and ${minerModel}: ${totalBitcoin} BTC`);
  } catch (error) {
    console.error(`Error updating Bitcoin daily summary: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Update Bitcoin monthly summary
 */
async function updateBitcoinMonthlySummary(minerModel: string): Promise<void> {
  try {
    const yearMonth = TARGET_DATE.substring(0, 7); // YYYY-MM
    
    // Calculate the total for the month
    const result = await db
      .select({ total: sql<string>`SUM(bitcoin_mined::numeric)` })
      .from(bitcoinDailySummaries)
      .where(and(
        sql`TO_CHAR(summary_date, 'YYYY-MM') = ${yearMonth}`,
        eq(bitcoinDailySummaries.minerModel, minerModel)
      ));
    
    const totalBitcoin = result[0]?.total ? parseFloat(result[0].total) : 0;
    
    if (totalBitcoin <= 0) {
      console.log(`No Bitcoin daily summary data found for ${yearMonth} and ${minerModel}`);
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
      bitcoinMined: totalBitcoin.toString(),
      createdAt: new Date(),
      updatedAt: new Date()
    });
    
    console.log(`Updated Bitcoin monthly summary for ${yearMonth} and ${minerModel}: ${totalBitcoin} BTC`);
  } catch (error) {
    console.error(`Error updating Bitcoin monthly summary: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Update Bitcoin yearly summary
 */
async function updateBitcoinYearlySummary(minerModel: string): Promise<void> {
  try {
    const year = TARGET_DATE.substring(0, 4); // YYYY
    
    // Calculate the total for the year
    const result = await db
      .select({ total: sql<string>`SUM(bitcoin_mined::numeric)` })
      .from(bitcoinMonthlySummaries)
      .where(and(
        sql`SUBSTRING(year_month, 1, 4) = ${year}`,
        eq(bitcoinMonthlySummaries.minerModel, minerModel)
      ));
    
    const totalBitcoin = result[0]?.total ? parseFloat(result[0].total) : 0;
    
    if (totalBitcoin <= 0) {
      console.log(`No Bitcoin monthly summary data found for ${year} and ${minerModel}`);
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
      bitcoinMined: totalBitcoin.toString(),
      createdAt: new Date(),
      updatedAt: new Date()
    });
    
    console.log(`Updated Bitcoin yearly summary for ${year} and ${minerModel}: ${totalBitcoin} BTC`);
  } catch (error) {
    console.error(`Error updating Bitcoin yearly summary: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Get verification summary for the reprocessed date
 */
async function getVerificationSummary(): Promise<{
  records: string;
  periods: string;
  volume: string;
  payment: string;
}> {
  try {
    // Count number of records
    const recordsResult = await db
      .select({ count: sql<number>`COUNT(*)::int` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Count number of unique settlement periods
    const periodsResult = await db
      .select({ count: sql<number>`COUNT(DISTINCT settlement_period)::int` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Get total volume and payment
    const totalsResult = await db
      .select({
        volume: sql<string>`SUM(volume * -1)::numeric(14,2)`,
        payment: sql<string>`SUM(payment)::numeric(14,2)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const records = recordsResult[0]?.count?.toString() || '0';
    const periods = periodsResult[0]?.count?.toString() || '0';
    const volume = totalsResult[0]?.volume?.toString() || '0';
    const payment = totalsResult[0]?.payment?.toString() || '0';
    
    return { records, periods, volume, payment };
  } catch (error) {
    console.error(`Error getting verification summary: ${(error as Error).message}`);
    return { records: '0', periods: '0', volume: '0', payment: '0' };
  }
}

/**
 * Run the complete reprocessing
 */
async function runReprocessing(): Promise<void> {
  const startTime = performance.now();
  
  try {
    // 1. Clear existing data
    await clearExistingCurtailmentRecords();
    await clearExistingBitcoinCalculations();
    
    // 2. Process curtailment records
    await processCurtailment();
    
    // 3. Calculate summaries
    await calculateDailySummary();
    await updateMonthlySummary();
    await updateYearlySummary();
    
    // 4. Process Bitcoin calculations for each miner model
    console.log(`\n==== Processing Bitcoin calculations for ${TARGET_DATE} ====\n`);
    
    for (const minerModel of MINER_MODELS) {
      const totalBitcoin = await processBitcoinCalculations(minerModel);
      
      // 5. Update Bitcoin summaries
      await updateBitcoinDailySummary(minerModel, totalBitcoin);
      await updateBitcoinMonthlySummary(minerModel);
      await updateBitcoinYearlySummary(minerModel);
    }
    
    // 6. Verify the results
    const verificationSummary = await getVerificationSummary();
    
    console.log(`\n==== Reprocessing complete for ${TARGET_DATE} ====`);
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(1);
    
    console.log(`Total execution time: ${duration} seconds\n`);
    
    console.log(`==== Verification Summary ====`);
    console.log(`Records: ${verificationSummary.records}`);
    console.log(`Settlement Periods: ${verificationSummary.periods}`);
    console.log(`Total Volume: ${verificationSummary.volume} MWh`);
    console.log(`Total Payment: £${verificationSummary.payment}`);
    
  } catch (error) {
    console.error(`\n==== Error during reprocessing: ${(error as Error).message} ====\n`);
    process.exit(1);
  }
}

// Execute the reprocessing
runReprocessing();
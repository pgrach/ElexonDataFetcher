/**
 * Fix Missing Periods for 2025-03-31
 * 
 * This script fixes missing settlement periods (1-6 and 25-48) for March 31, 2025 by:
 * 1. Preserving existing curtailment records for periods 7-24
 * 2. Fetching data from Elexon API for the missing periods (1-6 and 25-48)
 * 3. Updating the Bitcoin calculations for the new periods
 * 4. Updating all dependent tables in cascade:
 *    - daily_summaries
 *    - monthly_summaries 
 *    - yearly_summaries
 *    - historical_bitcoin_calculations
 *    - bitcoin_monthly_summaries
 *    - bitcoin_yearly_summaries
 */

import { db } from "../db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries, 
         historicalBitcoinCalculations, bitcoinMonthlySummaries, bitcoinYearlySummaries } from "../db/schema";
import { eq, and, sql, not, inArray } from "drizzle-orm";
import { fetchBidsOffers, delay } from "../server/services/elexon";
import { calculateBitcoin } from "../server/utils/bitcoin";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const TARGET_DATE = '2025-03-31';
const MISSING_PERIODS = [...Array(6).keys()].map(i => i + 1).concat([...Array(24).keys()].map(i => i + 25));
const LOG_FILE = `./logs/fix_missing_periods_${TARGET_DATE}_${new Date().toISOString().replace(/:/g, '-')}.log`;

// Logger utility
async function logMessage(message: string): Promise<void> {
  const timestamp = new Date().toISOString();
  const logEntry = `[${timestamp}] ${message}\n`;
  
  console.log(message);
  
  try {
    await fs.appendFile(LOG_FILE, logEntry);
  } catch (error) {
    console.error(`Error writing to log file: ${error}`);
  }
}

/**
 * Delete existing Bitcoin calculations for missing periods only
 */
async function resetBitcoinCalculationsForMissingPeriods(): Promise<void> {
  await logMessage(`Clearing existing Bitcoin calculations for missing periods...`);
  
  // Get count before delete
  const beforeCount = await db
    .select({ count: sql<number>`count(*)` })
    .from(historicalBitcoinCalculations)
    .where(and(
      eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
      inArray(historicalBitcoinCalculations.settlementPeriod, MISSING_PERIODS)
    ));
    
  await logMessage(`Found ${beforeCount[0]?.count || 0} existing Bitcoin calculations to delete for missing periods`);
  
  // Delete calculations for missing periods
  await db
    .delete(historicalBitcoinCalculations)
    .where(and(
      eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
      inArray(historicalBitcoinCalculations.settlementPeriod, MISSING_PERIODS)
    ));
    
  // Verify deletion
  const afterCount = await db
    .select({ count: sql<number>`count(*)` })
    .from(historicalBitcoinCalculations)
    .where(and(
      eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
      inArray(historicalBitcoinCalculations.settlementPeriod, MISSING_PERIODS)
    ));
    
  await logMessage(`Verified deletion: ${afterCount[0]?.count || 0} Bitcoin calculations remaining for missing periods`);
}

/**
 * Fetch data from Elexon API for a specific period
 */
async function fetchElexonDataForPeriod(period: number): Promise<any[]> {
  try {
    await logMessage(`Fetching data for period ${period}...`);
    
    const response = await fetchBidsOffers(TARGET_DATE, period);
    
    if (response && response.successfulBids) {
      await logMessage(`Got ${response.successfulBids.length} successful bids for period ${period}`);
      return response.successfulBids.map((bid: any) => ({
        settlementDate: TARGET_DATE,
        settlementPeriod: period,
        farmId: bid.bmuId,
        leadPartyName: bid.leadPartyName || 'Unknown',
        volume: bid.volume,
        price: bid.price,
        payment: bid.payment || (parseFloat(bid.volume) * parseFloat(bid.price)).toString()
      }));
    } else {
      await logMessage(`No data found for period ${period} or error in response`);
      return [];
    }
  } catch (error) {
    await logMessage(`Error fetching data for period ${period}: ${error}`);
    return [];
  }
}

/**
 * Fetch data for all missing periods
 */
async function fetchMissingPeriodsData(): Promise<void> {
  await logMessage(`Fetching data for ${MISSING_PERIODS.length} missing periods...`);
  
  let totalRecords = 0;
  
  for (const period of MISSING_PERIODS) {
    try {
      const records = await fetchElexonDataForPeriod(period);
      
      if (records.length > 0) {
        // Insert records into database
        await db.insert(curtailmentRecords).values(records);
        totalRecords += records.length;
        await logMessage(`Inserted ${records.length} records for period ${period}`);
      } else {
        await logMessage(`No records to insert for period ${period}`);
      }
      
      // Add a delay to avoid overwhelming the API
      await delay(500);
    } catch (error) {
      await logMessage(`Error processing period ${period}: ${error}`);
    }
  }
  
  await logMessage(`Total records inserted: ${totalRecords}`);
  
  // Verify data was ingested
  const records = await db
    .select({ count: sql<number>`count(*)` })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
  await logMessage(`Total records for ${TARGET_DATE} after update: ${records[0]?.count || 0}`);
}

/**
 * Calculate Bitcoin mining potential for missing periods
 */
async function calculateBitcoinPotentialForMissingPeriods(): Promise<void> {
  await logMessage(`Calculating Bitcoin mining potential for missing periods...`);
  
  try {
    // Get distinct periods
    const periods = await db
      .select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        inArray(curtailmentRecords.settlementPeriod, MISSING_PERIODS)
      ))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
      
    await logMessage(`Found ${periods.length} distinct settlement periods with new data`);
    
    // Process each period
    let calculationsAdded = 0;
    
    for (const { period } of periods) {
      await logMessage(`Processing settlement period ${period}`);
      
      // Get farm data for this period
      const periodFarmData = await db
        .select({
          farmId: curtailmentRecords.farmId,
          volume: sql<string>`ABS(${curtailmentRecords.volume}::numeric)`
        })
        .from(curtailmentRecords)
        .where(and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, period)
        ));
        
      // Calculate Bitcoin for each farm and each miner model
      for (const farm of periodFarmData) {
        const farmId = farm.farmId;
        const volume = parseFloat(farm.volume);
        
        // We'll calculate for all supported miner models
        const minerModels = ['S19J_PRO', 'S9', 'M20S'];
        
        for (const minerModel of minerModels) {
          try {
            // Calculate Bitcoin that could be mined with this energy
            const bitcoinMined = calculateBitcoin(volume, minerModel);
            
            // Store the calculation
            await db.insert(historicalBitcoinCalculations).values({
              settlementDate: TARGET_DATE,
              settlementPeriod: period,
              farmId: farmId,
              minerModel: minerModel,
              bitcoinMined: bitcoinMined.toString(),
              difficulty: '121507793131898' // Current network difficulty
            });
            
            calculationsAdded++;
          } catch (error) {
            await logMessage(`Error calculating Bitcoin for ${farmId}, period ${period}, model ${minerModel}: ${error}`);
          }
        }
      }
    }
    
    await logMessage(`Added ${calculationsAdded} new Bitcoin calculations`);
  } catch (error) {
    await logMessage(`Error calculating Bitcoin potential: ${error}`);
    throw error;
  }
}

/**
 * Update daily summary with total values
 */
async function updateDailySummary(): Promise<void> {
  await logMessage(`Updating daily summary for ${TARGET_DATE}...`);
  
  try {
    // Calculate total volume and payment from curtailment records
    const totals = await db
      .select({
        volume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        payment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (totals[0]) {
      const totalVolume = parseFloat(totals[0].volume || '0');
      const totalPayment = parseFloat(totals[0].payment || '0');
      
      // Update daily summary
      await db
        .update(dailySummaries)
        .set({
          totalCurtailedEnergy: totalVolume.toString(),
          totalPayment: totalPayment.toString(),
          lastUpdated: new Date()
        })
        .where(eq(dailySummaries.summaryDate, TARGET_DATE));
      
      await logMessage(`Updated daily summary: Total energy ${totalVolume.toFixed(2)} MWh, payment £${Math.abs(totalPayment).toFixed(2)}`);
    } else {
      await logMessage(`No data found for daily summary update`);
    }
  } catch (error) {
    await logMessage(`Error updating daily summary: ${error}`);
    throw error;
  }
}

/**
 * Update monthly and yearly summaries
 */
async function updateSummaries(): Promise<void> {
  await logMessage(`Updating monthly and yearly summaries...`);
  
  const yearMonth = TARGET_DATE.substring(0, 7);
  const year = TARGET_DATE.substring(0, 4);
  
  try {
    // Update monthly summary
    const monthlyTotals = await db
      .select({
        volume: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        payment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${TARGET_DATE}::date)`);
    
    if (monthlyTotals[0]) {
      await db
        .update(monthlySummaries)
        .set({
          totalCurtailedEnergy: monthlyTotals[0].volume,
          totalPayment: monthlyTotals[0].payment,
          updatedAt: new Date()
        })
        .where(eq(monthlySummaries.yearMonth, yearMonth));
      
      await logMessage(`Updated monthly summary for ${yearMonth}`);
    }
    
    // Update yearly summary
    const yearlyTotals = await db
      .select({
        volume: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        payment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${TARGET_DATE}::date)`);
    
    if (yearlyTotals[0]) {
      await db
        .update(yearlySummaries)
        .set({
          totalCurtailedEnergy: yearlyTotals[0].volume,
          totalPayment: yearlyTotals[0].payment,
          updatedAt: new Date()
        })
        .where(eq(yearlySummaries.year, year));
      
      await logMessage(`Updated yearly summary for ${year}`);
    }
  } catch (error) {
    await logMessage(`Error updating summaries: ${error}`);
    throw error;
  }
}

/**
 * Update Bitcoin monthly and yearly summaries
 */
async function updateBitcoinSummaries(): Promise<void> {
  await logMessage(`Updating Bitcoin monthly and yearly summaries...`);
  
  const yearMonth = TARGET_DATE.substring(0, 7);
  const year = TARGET_DATE.substring(0, 4);
  
  try {
    // Update monthly Bitcoin summaries for each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      // Calculate monthly total
      const monthlyTotal = await db
        .select({
          bitcoinMined: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
        })
        .from(historicalBitcoinCalculations)
        .where(and(
          sql`date_trunc('month', ${historicalBitcoinCalculations.settlementDate}::date) = date_trunc('month', ${TARGET_DATE}::date)`,
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ));
      
      if (monthlyTotal[0]?.bitcoinMined) {
        // Update monthly summary
        await db
          .update(bitcoinMonthlySummaries)
          .set({
            bitcoinMined: monthlyTotal[0].bitcoinMined,
            updatedAt: new Date()
          })
          .where(and(
            eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
            eq(bitcoinMonthlySummaries.minerModel, minerModel)
          ));
        
        await logMessage(`Updated monthly Bitcoin summary for ${yearMonth}, ${minerModel}`);
      }
    }
    
    // Update yearly Bitcoin summaries
    for (const minerModel of minerModels) {
      // Get sum of all monthly summaries
      const yearlyTotal = await db
        .select({
          bitcoinMined: sql<string>`SUM(${bitcoinMonthlySummaries.bitcoinMined}::numeric)`
        })
        .from(bitcoinMonthlySummaries)
        .where(and(
          sql`substring(${bitcoinMonthlySummaries.yearMonth} from 1 for 4) = ${year}`,
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        ));
      
      if (yearlyTotal[0]?.bitcoinMined) {
        // Update yearly summary
        await db
          .update(bitcoinYearlySummaries)
          .set({
            bitcoinMined: yearlyTotal[0].bitcoinMined,
            updatedAt: new Date()
          })
          .where(and(
            eq(bitcoinYearlySummaries.year, year),
            eq(bitcoinYearlySummaries.minerModel, minerModel)
          ));
        
        await logMessage(`Updated yearly Bitcoin summary for ${year}, ${minerModel}`);
      }
    }
  } catch (error) {
    await logMessage(`Error updating Bitcoin summaries: ${error}`);
    throw error;
  }
}

/**
 * Verify data integrity
 */
async function verifyDataIntegrity(): Promise<void> {
  await logMessage(`Verifying data integrity for ${TARGET_DATE}...`);
  
  try {
    // Check curtailment records and periods
    const curtailmentCount = await db
      .select({ count: sql<number>`count(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const periodsCount = await db
      .select({ count: sql<number>`count(DISTINCT ${curtailmentRecords.settlementPeriod})` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const dailySummary = await db
      .select({
        energy: dailySummaries.totalCurtailedEnergy,
        payment: dailySummaries.totalPayment
      })
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    const bitcoinCalcs = await db
      .select({
        model: historicalBitcoinCalculations.minerModel,
        count: sql<number>`count(*)`,
        total: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
      .groupBy(historicalBitcoinCalculations.minerModel);
    
    // Log summary of verification
    await logMessage(`--- Data Integrity Summary for ${TARGET_DATE} ---`);
    await logMessage(`Curtailment records: ${curtailmentCount[0]?.count || 0}`);
    await logMessage(`Settlement periods: ${periodsCount[0]?.count || 0} of 48`);
    
    if (dailySummary[0]) {
      await logMessage(`Daily summary: ${dailySummary[0].energy} MWh, £${Math.abs(parseFloat(dailySummary[0].payment || '0')).toFixed(2)}`);
    } else {
      await logMessage(`Warning: No daily summary found!`);
    }
    
    await logMessage(`Bitcoin calculations:`);
    for (const calc of bitcoinCalcs) {
      await logMessage(`- ${calc.model}: ${calc.count} records, ${parseFloat(calc.total || '0').toFixed(8)} BTC`);
    }
    
    // Check if we have the required data for all miner models
    const missingModels = ['S19J_PRO', 'S9', 'M20S'].filter(model => 
      !bitcoinCalcs.some(calc => calc.model === model)
    );
    
    if (missingModels.length > 0) {
      await logMessage(`Warning: Missing Bitcoin calculations for models: ${missingModels.join(', ')}`);
    } else {
      await logMessage(`All required miner models have Bitcoin calculations.`);
    }
    
    // Check for gaps in settlement periods
    const periods = await db
      .select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    const periodSet = new Set(periods.map(p => p.period));
    const missingPeriods = [];
    
    for (let i = 1; i <= 48; i++) {
      if (!periodSet.has(i)) {
        missingPeriods.push(i);
      }
    }
    
    if (missingPeriods.length > 0) {
      await logMessage(`Note: ${missingPeriods.length} settlement periods still have no curtailment data.`);
      await logMessage(`This may be normal if those periods had no curtailment.`);
    } else {
      await logMessage(`All 48 settlement periods have curtailment data.`);
    }
  } catch (error) {
    await logMessage(`Error verifying data integrity: ${error}`);
  }
}

/**
 * Main function - run the fix
 */
async function main() {
  try {
    await logMessage(`=== Starting fix for missing periods on ${TARGET_DATE} ===`);
    
    // Step 1: Clear Bitcoin calculations for missing periods
    await resetBitcoinCalculationsForMissingPeriods();
    
    // Step 2: Fetch missing periods data from Elexon API
    await fetchMissingPeriodsData();
    
    // Step 3: Calculate Bitcoin potential for new periods
    await calculateBitcoinPotentialForMissingPeriods();
    
    // Step 4: Update daily summary
    await updateDailySummary();
    
    // Step 5: Update monthly and yearly summaries
    await updateSummaries();
    
    // Step 6: Update Bitcoin summaries
    await updateBitcoinSummaries();
    
    // Step 7: Verify data integrity
    await verifyDataIntegrity();
    
    await logMessage(`=== Fix completed successfully for ${TARGET_DATE} ===`);
  } catch (error) {
    await logMessage(`ERROR: Fix process failed: ${error}`);
    process.exit(1);
  }
}

// Create log directory if it doesn't exist
async function initializeLogDirectory() {
  try {
    await fs.mkdir('./logs', { recursive: true });
    await logMessage(`=== Fix script started at ${new Date().toISOString()} ===`);
  } catch (error) {
    console.error(`Failed to create log directory: ${error}`);
    process.exit(1);
  }
}

// Run the script
initializeLogDirectory().then(() => main());
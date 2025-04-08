/**
 * Fix Specific Missing Periods for 2025-03-31
 * 
 * This script uses the enhanced curtailment service to process 
 * a single date and then recalculates Bitcoin potential and summaries.
 */

import { db } from "../db";
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries, 
         historicalBitcoinCalculations, bitcoinMonthlySummaries, bitcoinYearlySummaries } from "../db/schema";
import { eq, and, sql } from "drizzle-orm";
import { processDailyCurtailment } from "../server/services/curtailment_enhanced";
import { calculateBitcoin } from "../server/utils/bitcoin";
import fs from "fs/promises";

const TARGET_DATE = '2025-03-31';
const LOG_FILE = `./logs/fix_specific_periods_${TARGET_DATE}_${new Date().toISOString().replace(/:/g, '-')}.log`;

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
 * Reset Bitcoin calculations for the target date
 */
async function resetBitcoinCalculations(): Promise<void> {
  await logMessage(`Clearing existing Bitcoin calculations for ${TARGET_DATE}...`);
  
  // Get count before delete
  const beforeCount = await db
    .select({ count: sql<number>`count(*)` })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
  await logMessage(`Found ${beforeCount[0]?.count || 0} existing Bitcoin calculations to delete`);
  
  // Delete all calculations for the target date
  await db
    .delete(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
  // Verify deletion
  const afterCount = await db
    .select({ count: sql<number>`count(*)` })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
  await logMessage(`Verified deletion: ${afterCount[0]?.count || 0} Bitcoin calculations remaining`);
}

/**
 * Reingest data using the enhanced curtailment service
 */
async function reingestUsingEnhancedService(): Promise<void> {
  await logMessage(`Re-ingesting data for ${TARGET_DATE} using enhanced curtailment service...`);
  
  try {
    // Use the enhanced curtailment service to process all 48 periods
    await processDailyCurtailment(TARGET_DATE);
    
    // Verify data was ingested
    const curtailmentCount = await db
      .select({ count: sql<number>`count(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
    const periodsCount = await db
      .select({ count: sql<number>`count(DISTINCT ${curtailmentRecords.settlementPeriod})` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
    await logMessage(`Successfully reingested data: ${curtailmentCount[0]?.count || 0} curtailment records across ${periodsCount[0]?.count || 0} periods.`);
  } catch (error) {
    await logMessage(`Error reingesting data: ${error}`);
    throw error;
  }
}

/**
 * Get aggregated curtailment data by farm for a given date
 */
async function getFarmDataForDate(date: string): Promise<any[]> {
  try {
    const farmData = await db
      .select({
        farmId: curtailmentRecords.farmId,
        leadPartyName: curtailmentRecords.leadPartyName,
        volume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        payment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.farmId, curtailmentRecords.leadPartyName);
      
    return farmData.map(farm => ({
      farmId: farm.farmId,
      leadPartyName: farm.leadPartyName,
      volume: parseFloat(farm.volume),
      payment: parseFloat(farm.payment)
    }));
  } catch (error) {
    await logMessage(`Error getting farm data: ${error}`);
    throw error;
  }
}

/**
 * Calculate Bitcoin mining potential and store results
 */
async function calculateBitcoinPotential(): Promise<void> {
  await logMessage(`Calculating Bitcoin mining potential for ${TARGET_DATE}...`);
  
  try {
    // Get distinct periods for the date
    const periods = await db
      .select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
      
    await logMessage(`Found ${periods.length} distinct settlement periods`);
    
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
    
    await logMessage(`Added ${calculationsAdded} Bitcoin calculations for ${TARGET_DATE}`);
    
    // Verify calculations
    const verifyCount = await db
      .select({ count: sql<number>`count(*)` })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
      
    await logMessage(`Verification: ${verifyCount[0]?.count || 0} Bitcoin calculations exist for ${TARGET_DATE}`);
  } catch (error) {
    await logMessage(`Error calculating Bitcoin potential: ${error}`);
    throw error;
  }
}

/**
 * Update monthly Bitcoin summaries
 */
async function updateMonthlySummaries(): Promise<void> {
  const yearMonth = TARGET_DATE.substring(0, 7);
  await logMessage(`Updating monthly Bitcoin summaries for ${yearMonth}...`);
  
  try {
    // Calculate monthly totals for each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
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
        const total = parseFloat(monthlyTotal[0].bitcoinMined);
        
        // Update or insert monthly summary
        await db.insert(bitcoinMonthlySummaries).values({
          yearMonth: yearMonth,
          minerModel: minerModel,
          bitcoinMined: total.toString(),
          valueAtMining: (total * 63237.30).toString() // Using a fixed price for demo purposes
        }).onConflictDoUpdate({
          target: [bitcoinMonthlySummaries.yearMonth, bitcoinMonthlySummaries.minerModel],
          set: {
            bitcoinMined: total.toString(),
            valueAtMining: (total * 63237.30).toString(),
            updatedAt: new Date()
          }
        });
        
        await logMessage(`Updated monthly summary for ${yearMonth}, ${minerModel}: ${total.toFixed(8)} BTC`);
      } else {
        await logMessage(`No Bitcoin data found for ${yearMonth}, ${minerModel}`);
      }
    }
  } catch (error) {
    await logMessage(`Error updating monthly summaries: ${error}`);
    throw error;
  }
}

/**
 * Update yearly Bitcoin summaries
 */
async function updateYearlySummaries(): Promise<void> {
  const year = TARGET_DATE.substring(0, 4);
  await logMessage(`Updating yearly Bitcoin summaries for ${year}...`);
  
  try {
    // Calculate yearly totals for each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      // Get sum of all monthly summaries for this year and miner model
      const yearlyTotal = await db
        .select({
          bitcoinMined: sql<string>`SUM(${bitcoinMonthlySummaries.bitcoinMined}::numeric)`,
          monthsCount: sql<number>`count(*)`
        })
        .from(bitcoinMonthlySummaries)
        .where(and(
          sql`substring(${bitcoinMonthlySummaries.yearMonth} from 1 for 4) = ${year}`,
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        ));
      
      if (yearlyTotal[0]?.bitcoinMined) {
        const total = parseFloat(yearlyTotal[0].bitcoinMined);
        const monthsCount = yearlyTotal[0].monthsCount;
        
        // Update or insert yearly summary
        await db.insert(bitcoinYearlySummaries).values({
          year: year,
          minerModel: minerModel,
          bitcoinMined: total.toString(),
          valueAtMining: (total * 63237.30).toString() // Using a fixed price for demo purposes
        }).onConflictDoUpdate({
          target: [bitcoinYearlySummaries.year, bitcoinYearlySummaries.minerModel],
          set: {
            bitcoinMined: total.toString(),
            valueAtMining: (total * 63237.30).toString(),
            updatedAt: new Date()
          }
        });
        
        await logMessage(`Updated yearly summary for ${year}, ${minerModel}: ${total.toFixed(8)} BTC (${monthsCount} months)`);
      } else {
        await logMessage(`No Bitcoin data found for ${year}, ${minerModel}`);
      }
    }
  } catch (error) {
    await logMessage(`Error updating yearly summaries: ${error}`);
    throw error;
  }
}

/**
 * Verify data integrity
 */
async function verifyDataIntegrity(): Promise<void> {
  await logMessage(`Verifying data integrity for ${TARGET_DATE}...`);
  
  try {
    // Check curtailment records
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
  } catch (error) {
    await logMessage(`Error verifying data integrity: ${error}`);
  }
}

/**
 * Main function - run the fix
 */
async function main() {
  try {
    await logMessage(`=== Starting fix for ${TARGET_DATE} ===`);
    
    // Step 1: Clear existing Bitcoin calculations
    await resetBitcoinCalculations();
    
    // Step 2: Reingest data from Elexon API using enhanced service
    await reingestUsingEnhancedService();
    
    // Step 3: Calculate Bitcoin potential
    await calculateBitcoinPotential();
    
    // Step 4: Update monthly and yearly summaries
    await updateMonthlySummaries();
    await updateYearlySummaries();
    
    // Step 5: Verify data integrity
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
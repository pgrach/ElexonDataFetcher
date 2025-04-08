/**
 * Update Bitcoin Calculations for Missing Periods 2025-03-31
 * 
 * This script handles only periods that don't already have Bitcoin calculations.
 */

import { db } from "../db";
import { curtailmentRecords, historicalBitcoinCalculations, 
         bitcoinMonthlySummaries, bitcoinYearlySummaries } from "../db/schema";
import { eq, and, sql, not, inArray } from "drizzle-orm";
import { calculateBitcoin } from "../server/utils/bitcoin";
import fs from "fs/promises";

const TARGET_DATE = '2025-03-31';
const LOG_FILE = `./logs/update_bitcoin_missing_periods_${TARGET_DATE}_${new Date().toISOString().replace(/:/g, '-')}.log`;

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
 * Calculate Bitcoin mining potential for periods that don't have calculations yet
 */
async function calculateBitcoinPotentialForMissingPeriods(): Promise<void> {
  await logMessage(`Calculating Bitcoin mining potential for missing periods on ${TARGET_DATE}...`);
  
  try {
    // Find periods that already have Bitcoin calculations
    const existingPeriods = await db
      .select({ period: historicalBitcoinCalculations.settlementPeriod })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
      .groupBy(historicalBitcoinCalculations.settlementPeriod);
      
    const existingPeriodNumbers = existingPeriods.map(p => p.period);
    await logMessage(`Found ${existingPeriodNumbers.length} periods with existing Bitcoin calculations: ${existingPeriodNumbers.join(', ')}`);
    
    // Get periods from curtailment records that don't have Bitcoin calculations
    const missingPeriods = await db
      .select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        not(inArray(curtailmentRecords.settlementPeriod, existingPeriodNumbers))
      ))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
      
    await logMessage(`Found ${missingPeriods.length} periods that need Bitcoin calculations: ${missingPeriods.map(p => p.period).join(', ')}`);
    
    if (missingPeriods.length === 0) {
      await logMessage(`No missing periods to process. All done!`);
      return;
    }
    
    // Process each missing period - in batches to avoid memory issues
    const BATCH_SIZE = 5;
    let calculationsAdded = 0;
    
    for (let i = 0; i < missingPeriods.length; i += BATCH_SIZE) {
      const batchPeriods = missingPeriods.slice(i, i + BATCH_SIZE);
      await logMessage(`Processing batch of ${batchPeriods.length} periods (${i+1} to ${Math.min(i + BATCH_SIZE, missingPeriods.length)} of ${missingPeriods.length})`);
      
      for (const { period } of batchPeriods) {
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
              
              // Store the calculation with an upsert to avoid unique constraint violations
              await db.insert(historicalBitcoinCalculations).values({
                settlementDate: TARGET_DATE,
                settlementPeriod: period,
                farmId: farmId,
                minerModel: minerModel,
                bitcoinMined: bitcoinMined.toString(),
                difficulty: '121507793131898' // Current network difficulty
              }).onConflictDoUpdate({
                target: [
                  historicalBitcoinCalculations.settlementDate,
                  historicalBitcoinCalculations.settlementPeriod,
                  historicalBitcoinCalculations.farmId,
                  historicalBitcoinCalculations.minerModel
                ],
                set: {
                  bitcoinMined: bitcoinMined.toString()
                }
              });
              
              calculationsAdded++;
            } catch (error) {
              await logMessage(`Error calculating Bitcoin for ${farmId}, period ${period}, model ${minerModel}: ${error}`);
            }
          }
        }
      }
      
      // Log progress after each batch
      const currentCount = await db
        .select({ count: sql<number>`count(*)` })
        .from(historicalBitcoinCalculations)
        .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
        
      await logMessage(`Progress: ${currentCount[0]?.count || 0} Bitcoin calculations added so far`);
    }
    
    await logMessage(`Added ${calculationsAdded} new Bitcoin calculations for ${TARGET_DATE}`);
    
    // Verify final calculations
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
 * Main function - run the update
 */
async function main() {
  try {
    await logMessage(`=== Starting Bitcoin calculations update for missing periods on ${TARGET_DATE} ===`);
    
    // Step 1: Calculate Bitcoin potential for missing periods
    await calculateBitcoinPotentialForMissingPeriods();
    
    // Step 2: Update monthly and yearly summaries
    await updateMonthlySummaries();
    await updateYearlySummaries();
    
    await logMessage(`=== Bitcoin update completed successfully for ${TARGET_DATE} ===`);
  } catch (error) {
    await logMessage(`ERROR: Bitcoin update process failed: ${error}`);
    process.exit(1);
  }
}

// Create log directory if it doesn't exist
async function initializeLogDirectory() {
  try {
    await fs.mkdir('./logs', { recursive: true });
    await logMessage(`=== Bitcoin update script started at ${new Date().toISOString()} ===`);
  } catch (error) {
    console.error(`Failed to create log directory: ${error}`);
    process.exit(1);
  }
}

// Run the script
initializeLogDirectory().then(() => main());
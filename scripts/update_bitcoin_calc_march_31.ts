/**
 * Update Bitcoin Calculations for 2025-03-31
 * 
 * This script updates the Bitcoin calculations for March 31, 2025 by:
 * 1. Deleting existing Bitcoin calculations for 2025-03-31
 * 2. Calculating Bitcoin mining potential for all miner models
 * 3. Updating all dependent Bitcoin-related tables:
 *    - historical_bitcoin_calculations
 *    - bitcoin_monthly_summaries
 *    - bitcoin_yearly_summaries
 */

import { db } from "../db";
import { historicalBitcoinCalculations, bitcoinMonthlySummaries, bitcoinYearlySummaries, curtailmentRecords } from "../db/schema";
import { eq, and, sql, inArray } from "drizzle-orm";
import { calculateBitcoin } from "../server/utils/bitcoin";
import fs from "fs/promises";

const TARGET_DATE = '2025-03-31';
const LOG_FILE = `./logs/bitcoin_update_${TARGET_DATE}_${new Date().toISOString().replace(/:/g, '-')}.log`;

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
 * Calculate Bitcoin mining potential and store results
 */
async function calculateBitcoinPotential(): Promise<void> {
  await logMessage(`Calculating Bitcoin mining potential for ${TARGET_DATE}...`);
  
  try {
    // Get all curtailment records for the date
    const curtailments = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        farmId: curtailmentRecords.farmId,
        volume: sql<string>`ABS(${curtailmentRecords.volume}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .orderBy(curtailmentRecords.settlementPeriod, curtailmentRecords.farmId);
    
    await logMessage(`Processing ${curtailments.length} curtailment records`);
    
    // Get distinct periods for the date
    const periods = await db
      .select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
      
    await logMessage(`Found ${periods.length} distinct settlement periods`);
    
    // Process each record
    let calculationsAdded = 0;
    
    // We'll calculate for all supported miner models to ensure consistency
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const curtailment of curtailments) {
      const farmId = curtailment.farmId;
      const period = curtailment.period;
      const volume = parseFloat(curtailment.volume);
      
      for (const minerModel of minerModels) {
        try {
          // Calculate Bitcoin that could be mined with this energy
          const bitcoinMined = calculateBitcoin(volume, minerModel);
          
          // Store the calculation using upsert to handle duplicates
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
              bitcoinMined: bitcoinMined.toString(),
              difficulty: '121507793131898',
              calculatedAt: new Date()
            }
          });
          
          calculationsAdded++;
          
          if (calculationsAdded % 100 === 0) {
            await logMessage(`Added ${calculationsAdded} Bitcoin calculations so far...`);
          }
        } catch (error) {
          await logMessage(`Error calculating Bitcoin for ${farmId}, period ${period}, model ${minerModel}: ${error}`);
        }
      }
    }
    
    await logMessage(`Added ${calculationsAdded} Bitcoin calculations for ${TARGET_DATE}`);
    
    // Verify calculations
    const verifyCount = await db
      .select({ 
        model: historicalBitcoinCalculations.minerModel,
        count: sql<number>`count(*)` 
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
      .groupBy(historicalBitcoinCalculations.minerModel);
      
    await logMessage(`Verification counts by model:`);
    for (const model of verifyCount) {
      await logMessage(`${model.model}: ${model.count} calculations`);
    }
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
  await logMessage(`Verifying Bitcoin calculation data integrity for ${TARGET_DATE}...`);
  
  try {
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
    await logMessage(`--- Bitcoin Data Integrity Summary for ${TARGET_DATE} ---`);
    
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
    
    // Check monthly and yearly summaries
    const yearMonth = TARGET_DATE.substring(0, 7);
    const year = TARGET_DATE.substring(0, 4);
    
    const monthlySummaries = await db
      .select()
      .from(bitcoinMonthlySummaries)
      .where(and(
        eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
        inArray(bitcoinMonthlySummaries.minerModel, ['S19J_PRO', 'S9', 'M20S'])
      ));
    
    await logMessage(`Monthly summaries for ${yearMonth}:`);
    for (const summary of monthlySummaries) {
      await logMessage(`- ${summary.minerModel}: ${parseFloat(summary.bitcoinMined || '0').toFixed(8)} BTC`);
    }
    
    const yearlySummaries = await db
      .select()
      .from(bitcoinYearlySummaries)
      .where(and(
        eq(bitcoinYearlySummaries.year, year),
        inArray(bitcoinYearlySummaries.minerModel, ['S19J_PRO', 'S9', 'M20S'])
      ));
    
    await logMessage(`Yearly summaries for ${year}:`);
    for (const summary of yearlySummaries) {
      await logMessage(`- ${summary.minerModel}: ${parseFloat(summary.bitcoinMined || '0').toFixed(8)} BTC`);
    }
  } catch (error) {
    await logMessage(`Error verifying data integrity: ${error}`);
  }
}

/**
 * Main function - run the update
 */
async function main() {
  try {
    await logMessage(`=== Starting Bitcoin calculation update for ${TARGET_DATE} ===`);
    
    // Step 1: Reset existing Bitcoin calculations
    await resetBitcoinCalculations();
    
    // Step 2: Calculate Bitcoin potential
    await calculateBitcoinPotential();
    
    // Step 3: Update monthly and yearly summaries
    await updateMonthlySummaries();
    await updateYearlySummaries();
    
    // Step 4: Verify data integrity
    await verifyDataIntegrity();
    
    await logMessage(`=== Bitcoin calculation update completed successfully for ${TARGET_DATE} ===`);
  } catch (error) {
    await logMessage(`ERROR: Update process failed: ${error}`);
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
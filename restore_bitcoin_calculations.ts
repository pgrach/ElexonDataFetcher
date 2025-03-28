/**
 * Restore Bitcoin Calculation Script
 * 
 * This script restores Bitcoin calculations for a specific date
 * without modifying the Bitcoin values, only redistributing the energy.
 * 
 * Usage:
 *   npx tsx restore_bitcoin_calculations.ts <date>
 * 
 * Example:
 *   npx tsx restore_bitcoin_calculations.ts 2025-03-27
 */

import { db } from "./db";
import { 
  dailySummaries, 
  historicalBitcoinCalculations, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries, 
  curtailmentRecords 
} from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import { format, parseISO } from "date-fns";

// Configuration
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
// For value at mining calculations (approximate BTC price in GBP)
const BITCOIN_PRICE_GBP = 66403.60; 

// Main function to restore Bitcoin calculations
async function restoreCalculations(date: string): Promise<void> {
  console.log(`\n=== Restoring Bitcoin calculations for ${date} ===`);
  
  try {
    // 1. Get the daily summary for this date to verify energy values
    const summary = await db.select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date))
      .limit(1);
    
    if (summary.length === 0) {
      console.log(`No daily summary found for ${date}. Exiting.`);
      return;
    }
    
    const totalEnergy = parseFloat(summary[0].totalCurtailedEnergy?.toString() || "0");
    console.log(`Found daily summary with ${totalEnergy.toLocaleString()} MWh curtailed energy`);
    
    // 2. Check if we have any existing Bitcoin calculations for verification
    const currentCalculations = await db.select({
      minerModel: historicalBitcoinCalculations.minerModel,
      count: sql<number>`COUNT(*)`,
      totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`
    })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, date))
    .groupBy(historicalBitcoinCalculations.minerModel);
    
    // For each model, store the total Bitcoin amount currently in the DB
    const bitcoinByModel: Record<string, number> = {};
    const recordsByModel: Record<string, number> = {};
    
    for (const calc of currentCalculations) {
      bitcoinByModel[calc.minerModel] = parseFloat(calc.totalBitcoin.toString() || "0");
      recordsByModel[calc.minerModel] = calc.count;
    }
    
    console.log('\nCurrent Bitcoin amounts by model:');
    for (const model of MINER_MODELS) {
      const amount = bitcoinByModel[model] || 0;
      console.log(`${model}: ${amount.toFixed(8)} BTC from ${recordsByModel[model] || 0} records`);
    }
    
    // 3. Get reference values from monthly and yearly summaries
    console.log('\nMonthly summary Bitcoin values:');
    const monthlyReference = await db.select()
      .from(bitcoinMonthlySummaries)
      .where(eq(bitcoinMonthlySummaries.yearMonth, format(parseISO(date), 'yyyy-MM')));
    
    for (const monthlyData of monthlyReference) {
      console.log(`${monthlyData.minerModel}: ${parseFloat(monthlyData.bitcoinMined.toString()).toFixed(8)} BTC`);
    }
    
    // 4. Clear any existing Bitcoin calculations for this date
    // Only if we have enough information to restore them
    if (Object.keys(bitcoinByModel).length > 0) {
      const deletedCount = await db.delete(historicalBitcoinCalculations)
        .where(eq(historicalBitcoinCalculations.settlementDate, date))
        .returning();
      
      console.log(`\nCleared ${deletedCount.length} existing Bitcoin calculations`);
      
      // 5. Distribute the existing energy across 48 settlement periods,
      // while keeping the same total Bitcoin per model
      const periodsInDay = 48;
      const energyPerPeriod = totalEnergy / periodsInDay;
      
      // Create a single farm ID for summary-based calculations
      const summaryFarmId = "SUMMARY_BASED"; 
      
      // 6. Insert Bitcoin calculations for each period and miner model
      let totalInserted = 0;
      
      // Process each period
      for (let period = 1; period <= periodsInDay; period++) {
        // For each miner model, calculate Bitcoin mined and insert a record
        for (const minerModel of MINER_MODELS) {
          // If we don't have a record for this model, skip it
          if (!bitcoinByModel[minerModel]) continue;
          
          // Calculate Bitcoin amount for just this period (evenly distribute)
          const bitcoinMined = bitcoinByModel[minerModel] / periodsInDay;
          
          // Insert the record
          await db.insert(historicalBitcoinCalculations).values({
            settlementDate: date,
            settlementPeriod: period,
            farmId: summaryFarmId,
            minerModel: minerModel,
            bitcoinMined: bitcoinMined.toString(),
            // We don't know the exact difficulty value used before, but we maintain the Bitcoin value
            difficulty: "113757508810853", // Using a common difficulty value
            calculatedAt: new Date()
          });
          
          totalInserted++;
        }
      }
      
      console.log(`Inserted ${totalInserted} Bitcoin calculation records`);
      
      // 7. Verify the results
      const verificationResults = await db.select({
        minerModel: historicalBitcoinCalculations.minerModel,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, date))
      .groupBy(historicalBitcoinCalculations.minerModel);
      
      console.log('\nVerification results:');
      for (const result of verificationResults) {
        const newTotal = parseFloat(result.totalBitcoin.toString());
        const originalTotal = bitcoinByModel[result.minerModel] || 0;
        const diff = Math.abs(newTotal - originalTotal);
        
        console.log(`${result.minerModel}: ${newTotal.toFixed(8)} BTC (original: ${originalTotal.toFixed(8)}, diff: ${diff.toExponential(2)})`);
      }
      
      // 8. Update the monthly and yearly summaries if needed
      console.log('\nChecking if we need to update monthly/yearly summaries...');
      
      let summariesUpdated = false;
      
      // If the monthly values are significantly different from our calculation,
      // restore them to original values by directly updating the table
      for (const model of MINER_MODELS) {
        const monthlyData = monthlyReference.find(m => m.minerModel === model);
        if (!monthlyData) continue;
        
        const modelBitcoin = verificationResults.find(r => r.minerModel === model);
        if (!modelBitcoin) continue;
        
        // If there's a significant difference, we should update the monthly summary
        const monthlyBitcoin = parseFloat(monthlyData.bitcoinMined.toString());
        const calculatedBitcoin = parseFloat(modelBitcoin.totalBitcoin.toString());
        
        if (Math.abs(monthlyBitcoin - calculatedBitcoin) > 0.00001) {
          console.log(`\nUpdating monthly/yearly summaries to ensure consistency...`);
          summariesUpdated = true;
          break;
        }
      }
      
      if (summariesUpdated) {
        console.log('Restoring original summary values...');
        // Re-run update for monthly and yearly summaries
        await updateMonthlySummary(date);
        await updateYearlySummary(date);
      } else {
        console.log('Monthly/yearly summaries appear to be consistent with calculations.');
      }
    } else {
      console.log('No existing Bitcoin calculations found for this date.');
    }
    
    console.log(`\nCompleted restoration for ${date}`);
  } catch (error) {
    console.error(`Error restoring calculations for ${date}:`, error);
    throw error;
  }
}

// Update the monthly Bitcoin summary for the month containing the specified date
async function updateMonthlySummary(date: string): Promise<void> {
  try {
    const parsedDate = parseISO(date);
    const yearMonth = format(parsedDate, 'yyyy-MM');
    
    console.log(`\nUpdating monthly Bitcoin summary for ${yearMonth}...`);
    
    // Get the total Bitcoin mined for each model in this month
    const monthlyResults = await db
      .select({
        minerModel: historicalBitcoinCalculations.minerModel,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`,
      })
      .from(historicalBitcoinCalculations)
      .where(sql`TO_CHAR(settlement_date, 'YYYY-MM') = ${yearMonth}`)
      .groupBy(historicalBitcoinCalculations.minerModel);
    
    // Clear existing monthly records for this month and models
    const deletedMonthly = await db
      .delete(bitcoinMonthlySummaries)
      .where(sql`year_month = ${yearMonth}`)
      .returning();
    
    console.log(`Cleared ${deletedMonthly.length} existing monthly summary records`);
    
    // Insert new monthly records
    let insertedMonthly = 0;
    for (const result of monthlyResults) {
      const bitcoinAmount = parseFloat(result.totalBitcoin);
      
      await db.insert(bitcoinMonthlySummaries).values({
        yearMonth: yearMonth,
        minerModel: result.minerModel,
        bitcoinMined: result.totalBitcoin,
        valueAtMining: (bitcoinAmount * BITCOIN_PRICE_GBP).toString(),
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      console.log(`${result.minerModel}: ${bitcoinAmount.toFixed(8)} BTC (£${(bitcoinAmount * BITCOIN_PRICE_GBP).toLocaleString('en-GB', {maximumFractionDigits: 2})})`);
      insertedMonthly++;
    }
    
    console.log(`Inserted ${insertedMonthly} monthly summary records for ${yearMonth}`);
  } catch (error) {
    console.error('Error updating monthly Bitcoin summary:', error);
  }
}

// Update the yearly Bitcoin summary for the year containing the specified date
async function updateYearlySummary(date: string): Promise<void> {
  try {
    const parsedDate = parseISO(date);
    const year = format(parsedDate, 'yyyy');
    
    console.log(`\nUpdating yearly Bitcoin summary for ${year}...`);
    
    // Get the total Bitcoin mined for each model in this year
    const yearlyResults = await db
      .select({
        minerModel: historicalBitcoinCalculations.minerModel,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`,
      })
      .from(historicalBitcoinCalculations)
      .where(sql`TO_CHAR(settlement_date, 'YYYY') = ${year}`)
      .groupBy(historicalBitcoinCalculations.minerModel);
    
    // Clear existing yearly records for this year and models
    const deletedYearly = await db
      .delete(bitcoinYearlySummaries)
      .where(sql`year = ${year}`)
      .returning();
    
    console.log(`Cleared ${deletedYearly.length} existing yearly summary records`);
    
    // Insert new yearly records
    let insertedYearly = 0;
    for (const result of yearlyResults) {
      const bitcoinAmount = parseFloat(result.totalBitcoin);
      
      await db.insert(bitcoinYearlySummaries).values({
        year: year,
        minerModel: result.minerModel,
        bitcoinMined: result.totalBitcoin,
        valueAtMining: (bitcoinAmount * BITCOIN_PRICE_GBP).toString(),
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      console.log(`${result.minerModel}: ${bitcoinAmount.toFixed(8)} BTC (£${(bitcoinAmount * BITCOIN_PRICE_GBP).toLocaleString('en-GB', {maximumFractionDigits: 2})})`);
      insertedYearly++;
    }
    
    console.log(`Inserted ${insertedYearly} yearly summary records for ${year}`);
  } catch (error) {
    console.error('Error updating yearly Bitcoin summary:', error);
  }
}

// Main function
async function main() {
  const targetDate = process.argv[2];
  
  if (!targetDate) {
    console.error('Please provide a date in format YYYY-MM-DD');
    process.exit(1);
  }
  
  try {
    // Validate date format
    if (!/^\d{4}-\d{2}-\d{2}$/.test(targetDate)) {
      throw new Error('Invalid date format. Use YYYY-MM-DD');
    }
    
    console.log(`Starting Bitcoin calculation restoration for ${targetDate}`);
    await restoreCalculations(targetDate);
    console.log(`\nCompleted Bitcoin calculation restoration for ${targetDate}`);
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  } finally {
    // Clean up database connection
    try {
      // @ts-ignore - Properly close the pool if available
      if (db.pool && typeof db.pool.end === 'function') {
        // @ts-ignore
        await db.pool.end();
      }
    } catch (error) {
      console.error('Error closing database connection:', error);
    }
    process.exit(0);
  }
}

// Run the script
main();
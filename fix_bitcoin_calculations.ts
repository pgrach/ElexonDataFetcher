/**
 * Fix Bitcoin Calculation Updater
 * 
 * This script corrects the Bitcoin calculations for a specific date
 * and updates the monthly and yearly summaries accordingly.
 * 
 * Usage:
 *   npx tsx fix_bitcoin_calculations.ts <date>
 * 
 * Example:
 *   npx tsx fix_bitcoin_calculations.ts 2025-03-27
 */

import { db } from "./db";
import { dailySummaries, historicalBitcoinCalculations, bitcoinMonthlySummaries, bitcoinYearlySummaries } from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import { format, parseISO } from "date-fns";

// Configuration
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
// Use more realistic Bitcoin difficulty value from minerstat API
const DEFAULT_DIFFICULTY = 113757508810853; 
// For value at mining calculations (approximate BTC price in GBP)
const BITCOIN_PRICE_GBP = 66403.60; 

// Helper to calculate Bitcoin mined for a specific miner model and energy amount
function calculateBitcoinMined(energyMWh: number, minerModel: string, difficulty: number): number {
  // Conversion constants
  const J_TO_KWH = 2.77778e-7;
  
  // Miner efficiency in J/TH (joules per terahash)
  const minerEfficiency: Record<string, number> = {
    "S19J_PRO": 29.5, // Antminer S19J Pro
    "S9": 98,       // Antminer S9
    "M20S": 48      // Whatsminer M20S
  };

  // Get miner efficiency or default to S19J Pro
  const efficiency = minerEfficiency[minerModel] || minerEfficiency["S19J_PRO"];
  
  // Convert MWh to kWh
  const energyKWh = energyMWh * 1000;
  
  // Calculate petahashes (PH) based on miner efficiency
  const petahashesGenerated = energyKWh / (efficiency * J_TO_KWH * 1000);
  
  // Calculate expected Bitcoin mined
  // Formula: (petahashes * 1e15 / difficulty) * 6.25 BTC per block * number of seconds in 30 minutes
  const bitcoinMined = (petahashesGenerated * 1e15 / difficulty) * 6.25 * 1800;
  
  return bitcoinMined;
}

// Process a single date
async function processDate(date: string): Promise<void> {
  console.log(`\n=== Processing Bitcoin calculations for ${date} ===`);
  
  try {
    // 1. Get the daily summary for this date
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
    
    if (totalEnergy <= 0) {
      console.log(`No curtailed energy for ${date}. Exiting.`);
      return;
    }
    
    // 2. Clear any existing Bitcoin calculations for this date
    const deletedCount = await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, date))
      .returning();
    
    console.log(`Cleared ${deletedCount.length} existing Bitcoin calculations`);
    
    // 3. Distribute the total energy evenly across 48 settlement periods
    const periodsInDay = 48;
    const energyPerPeriod = totalEnergy / periodsInDay;
    
    // Create a single farm ID for summary-based calculations
    const summaryFarmId = "SUMMARY_BASED"; 
    
    // 4. Insert Bitcoin calculations for each period and miner model
    let totalInserted = 0;
    const bitcoinByModel: Record<string, number> = {};
    
    // Initialize bitcoinByModel with each miner model set to 0
    for (const model of MINER_MODELS) {
      bitcoinByModel[model] = 0;
    }
    
    // Process each period
    for (let period = 1; period <= periodsInDay; period++) {
      // For each miner model, calculate Bitcoin mined and insert a record
      for (const minerModel of MINER_MODELS) {
        // Calculate Bitcoin amount for just this period
        const bitcoinMined = calculateBitcoinMined(energyPerPeriod, minerModel, DEFAULT_DIFFICULTY);
        
        // Keep track of total Bitcoin mined for each model
        bitcoinByModel[minerModel] += bitcoinMined;
        
        // Insert the record
        await db.insert(historicalBitcoinCalculations).values({
          settlementDate: date,
          settlementPeriod: period,
          farmId: summaryFarmId,
          minerModel: minerModel,
          bitcoinMined: bitcoinMined.toString(),
          difficulty: DEFAULT_DIFFICULTY.toString(),
          calculatedAt: new Date()
        });
        
        totalInserted++;
      }
    }
    
    console.log(`Inserted ${totalInserted} Bitcoin calculation records (${periodsInDay} periods x ${MINER_MODELS.length} miners)`);
    
    // 5. Log the total Bitcoin mined for each model
    console.log(`\nTotal Bitcoin mined by model:`);
    for (const model in bitcoinByModel) {
      console.log(`${model}: ${bitcoinByModel[model].toFixed(8)} BTC`);
    }
    
    // 6. Update the monthly and yearly summaries
    await updateMonthlySummary(date);
    await updateYearlySummary(date);
    
    console.log(`\nCompleted processing for ${date}`);
  } catch (error) {
    console.error(`Error processing ${date}:`, error);
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
    
    console.log(`Starting Bitcoin calculation update for ${targetDate}`);
    await processDate(targetDate);
    console.log(`\nCompleted Bitcoin calculation update for ${targetDate}`);
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
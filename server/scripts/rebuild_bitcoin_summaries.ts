/**
 * Rebuild Bitcoin Summaries
 * 
 * This script completely rebuilds the bitcoin_daily_summaries, bitcoin_monthly_summaries,
 * and bitcoin_yearly_summaries tables based on data in historical_bitcoin_calculations.
 */

import { db } from "@db";
import { sql } from "drizzle-orm";
import { 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries, 
  bitcoinMonthlySummaries, 
  bitcoinYearlySummaries 
} from "@db/schema";
import { calculateMonthlyBitcoinSummary, manualUpdateYearlyBitcoinSummary } from "../services/bitcoinService";

/**
 * Update all Bitcoin Daily Summaries 
 * 
 * Regenerates the bitcoin_daily_summaries table based on historical_bitcoin_calculations
 */
async function updateAllDailyBitcoinSummaries(): Promise<void> {
  try {
    console.log(`\n=== Updating All Bitcoin Daily Summaries ===\n`);
    
    // Get all unique date and miner model combinations from historical calculations
    const result = await db.execute(sql`
      SELECT DISTINCT settlement_date, miner_model 
      FROM historical_bitcoin_calculations
      ORDER BY settlement_date ASC, miner_model ASC
    `);
    
    const distinctPairs = result.rows || [];
    console.log(`Found ${distinctPairs.length} distinct date-miner combinations`);
    
    // First, clear the table
    console.log(`Clearing bitcoin_daily_summaries table...`);
    await db.execute(sql`TRUNCATE TABLE bitcoin_daily_summaries`);
    
    // Process each date-miner pair
    let counter = 0;
    for (const pair of distinctPairs) {
      const date = pair.settlement_date as string;
      const minerModel = pair.miner_model as string;
      
      // Calculate total Bitcoin mined for the day
      const result = await db.execute(sql`
        SELECT
          SUM(CAST(bitcoin_mined AS NUMERIC)) as total_bitcoin
        FROM
          historical_bitcoin_calculations
        WHERE
          settlement_date = ${date}
          AND miner_model = ${minerModel}
      `);
      
      if (!result.rows || !result.rows.length || !result.rows[0].total_bitcoin) {
        console.log(`No Bitcoin data found for ${date} and ${minerModel}`);
        continue;
      }
      
      const data = result.rows[0];
      
      // Insert new summary (without average_difficulty which is redundant)
      await db.insert(bitcoinDailySummaries).values({
        summaryDate: date,
        minerModel: minerModel,
        bitcoinMined: data.total_bitcoin.toString(),
        updatedAt: new Date(),
        createdAt: new Date()
      });
      
      counter++;
      if (counter % 100 === 0) {
        console.log(`Processed ${counter}/${distinctPairs.length} daily summaries`);
      }
    }
    
    console.log(`\n=== Successfully updated ${counter} Bitcoin daily summaries ===\n`);
  } catch (error) {
    console.error(`Error updating all Bitcoin daily summaries:`, error);
    throw error;
  }
}

/**
 * Update all Bitcoin Monthly Summaries
 */
async function updateAllMonthlyBitcoinSummaries(): Promise<void> {
  try {
    console.log(`\n=== Updating All Bitcoin Monthly Summaries ===\n`);
    
    // Get all unique year-month values from historical calculations
    const yearMonthsResult = await db.execute(sql`
      SELECT DISTINCT 
        SUBSTRING(settlement_date, 1, 7) as year_month
      FROM 
        historical_bitcoin_calculations
      ORDER BY 
        year_month ASC
    `);
    
    const distinctYearMonths = yearMonthsResult.rows || [];
    console.log(`Found ${distinctYearMonths.length} distinct year-months`);
    
    // Get all unique miner models
    const minerModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model 
      FROM historical_bitcoin_calculations
      ORDER BY miner_model ASC
    `);
    
    const distinctMinerModels = minerModelsResult.rows || [];
    console.log(`Found ${distinctMinerModels.length} distinct miner models`);
    
    // First, clear the table
    console.log(`Clearing bitcoin_monthly_summaries table...`);
    await db.execute(sql`TRUNCATE TABLE bitcoin_monthly_summaries`);
    
    let counter = 0;
    const totalCombinations = distinctYearMonths.length * distinctMinerModels.length;
    
    // Process each year-month and miner model combination
    for (const ym of distinctYearMonths) {
      const yearMonth = ym.year_month as string;
      
      for (const mm of distinctMinerModels) {
        const minerModel = mm.miner_model as string;
        
        // Call the existing service function to update the monthly summary
        try {
          await calculateMonthlyBitcoinSummary(yearMonth, minerModel);
          counter++;
          
          if (counter % 20 === 0 || counter === totalCombinations) {
            console.log(`Processed ${counter}/${totalCombinations} monthly summaries`);
          }
        } catch (error) {
          console.error(`Error processing monthly summary for ${yearMonth} and ${minerModel}:`, error);
          // Continue with other combinations even if one fails
        }
      }
    }
    
    console.log(`\n=== Successfully updated ${counter} Bitcoin monthly summaries ===\n`);
  } catch (error) {
    console.error(`Error updating all Bitcoin monthly summaries:`, error);
    throw error;
  }
}

/**
 * Update all Bitcoin Yearly Summaries
 */
async function updateAllYearlyBitcoinSummaries(): Promise<void> {
  try {
    console.log(`\n=== Updating All Bitcoin Yearly Summaries ===\n`);
    
    // Get all unique years from historical calculations
    const yearsResult = await db.execute(sql`
      SELECT DISTINCT 
        SUBSTRING(settlement_date, 1, 4) as year
      FROM 
        historical_bitcoin_calculations
      ORDER BY 
        year ASC
    `);
    
    const distinctYears = yearsResult.rows || [];
    console.log(`Found ${distinctYears.length} distinct years`);
    
    // First, clear the table
    console.log(`Clearing bitcoin_yearly_summaries table...`);
    await db.execute(sql`TRUNCATE TABLE bitcoin_yearly_summaries`);
    
    let counter = 0;
    
    // Process each year
    for (const y of distinctYears) {
      const year = y.year as string;
      
      // Call the existing service function to update the yearly summary
      try {
        await manualUpdateYearlyBitcoinSummary(year);
        counter++;
        console.log(`Processed yearly summary for ${year}`);
      } catch (error) {
        console.error(`Error processing yearly summary for ${year}:`, error);
        // Continue with other years even if one fails
      }
    }
    
    console.log(`\n=== Successfully updated ${counter} Bitcoin yearly summaries ===\n`);
  } catch (error) {
    console.error(`Error updating all Bitcoin yearly summaries:`, error);
    throw error;
  }
}

/**
 * Main function to run the full rebuild
 */
async function main() {
  try {
    console.log(`\n=== Starting Bitcoin Summary Tables Rebuild ===\n`);
    const startTime = Date.now();
    
    // Rebuild all three summary tables in sequence
    await updateAllDailyBitcoinSummaries();
    await updateAllMonthlyBitcoinSummaries();
    await updateAllYearlyBitcoinSummaries();
    
    const endTime = Date.now();
    const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
    
    console.log(`\n=== Bitcoin Summary Tables Rebuild Completed ===`);
    console.log(`Total Duration: ${durationSeconds} seconds`);
    
    process.exit(0);
  } catch (error) {
    console.error('Error during Bitcoin summary tables rebuild:', error);
    process.exit(1);
  }
}

// Execute the main function
main();
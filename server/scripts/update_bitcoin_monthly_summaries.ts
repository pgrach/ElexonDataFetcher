/**
 * Update Bitcoin Monthly Summaries for 2025-04
 * 
 * This script updates the bitcoin_monthly_summaries table based on 
 * the data in bitcoin_daily_summaries.
 */

import { db } from "@db";
import { bitcoinMonthlySummaries } from "@db/schema";
import { sql } from "drizzle-orm";

// Constants
const TARGET_MONTH = "2025-04";
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

async function calculateMonthlyBitcoinSummary(yearMonth: string, minerModel: string): Promise<void> {
  try {
    console.log(`Calculating monthly Bitcoin summary for ${yearMonth} and ${minerModel}...`);
    
    // Calculate total Bitcoin mined for the month
    const result = await db.execute(sql`
      SELECT
        SUM(CAST(bitcoin_mined AS NUMERIC)) as total_bitcoin
      FROM
        bitcoin_daily_summaries
      WHERE
        TO_CHAR(summary_date, 'YYYY-MM') = ${yearMonth}
        AND miner_model = ${minerModel}
    `);
    
    // Check if we have data
    if (!result.rows || result.rows.length === 0 || !result.rows[0].total_bitcoin) {
      console.log(`No Bitcoin data found for ${yearMonth} and ${minerModel}`);
      return;
    }
    
    const totalBitcoin = result.rows[0].total_bitcoin;
    
    // Delete existing monthly summary if any
    await db.execute(sql`
      DELETE FROM bitcoin_monthly_summaries
      WHERE year_month = ${yearMonth}
      AND miner_model = ${minerModel}
    `);
    
    // Insert new summary
    await db.insert(bitcoinMonthlySummaries).values({
      yearMonth: yearMonth,
      minerModel: minerModel,
      bitcoinMined: totalBitcoin.toString(),
      updatedAt: new Date(),
      createdAt: new Date()
    });
    
    console.log(`Monthly Bitcoin summary updated for ${yearMonth} and ${minerModel}: ${totalBitcoin} BTC`);
  } catch (error) {
    console.error(`Error updating monthly Bitcoin summary for ${yearMonth} and ${minerModel}:`, error);
    throw error;
  }
}

async function updateYearlyBitcoinSummary(year: string, minerModel: string): Promise<void> {
  try {
    console.log(`Calculating yearly Bitcoin summary for ${year} and ${minerModel}...`);
    
    // Calculate total Bitcoin mined for the year
    const result = await db.execute(sql`
      SELECT
        SUM(CAST(bitcoin_mined AS NUMERIC)) as total_bitcoin
      FROM
        bitcoin_monthly_summaries
      WHERE
        SUBSTRING(year_month, 1, 4) = ${year}
        AND miner_model = ${minerModel}
    `);
    
    // Check if we have data
    if (!result.rows || result.rows.length === 0 || !result.rows[0].total_bitcoin) {
      console.log(`No Bitcoin data found for ${year} and ${minerModel}`);
      return;
    }
    
    const totalBitcoin = result.rows[0].total_bitcoin;
    
    // Delete existing yearly summary if any
    await db.execute(sql`
      DELETE FROM bitcoin_yearly_summaries
      WHERE year = ${year}
      AND miner_model = ${minerModel}
    `);
    
    // Insert new summary
    await db.execute(sql`
      INSERT INTO bitcoin_yearly_summaries 
      (year, miner_model, bitcoin_mined, created_at, updated_at)
      VALUES (
        ${year},
        ${minerModel},
        ${totalBitcoin.toString()},
        NOW(),
        NOW()
      )
    `);
    
    console.log(`Yearly Bitcoin summary updated for ${year} and ${minerModel}: ${totalBitcoin} BTC`);
  } catch (error) {
    console.error(`Error updating yearly Bitcoin summary for ${year} and ${minerModel}:`, error);
    throw error;
  }
}

async function main() {
  try {
    console.log(`\n=== Starting Bitcoin Monthly Summaries Update for ${TARGET_MONTH} ===\n`);
    
    const startTime = Date.now();
    
    // Update monthly summaries for each miner model
    for (const minerModel of MINER_MODELS) {
      try {
        await calculateMonthlyBitcoinSummary(TARGET_MONTH, minerModel);
      } catch (error) {
        console.error(`Error processing monthly summary for ${minerModel}:`, error);
        // Continue with other models even if one fails
      }
    }
    
    // Update yearly summaries
    const year = TARGET_MONTH.substring(0, 4);
    for (const minerModel of MINER_MODELS) {
      try {
        await updateYearlyBitcoinSummary(year, minerModel);
      } catch (error) {
        console.error(`Error processing yearly summary for ${minerModel}:`, error);
        // Continue with other models even if one fails
      }
    }
    
    const endTime = Date.now();
    
    console.log(`\n=== Bitcoin Summaries Update Completed ===`);
    console.log(`Duration: ${((endTime - startTime) / 1000).toFixed(2)} seconds`);
    
    process.exit(0);
  } catch (error) {
    console.error('Error during Bitcoin summaries update:', error);
    process.exit(1);
  }
}

main();
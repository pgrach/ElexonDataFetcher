/**
 * Update Bitcoin Daily Summaries for 2025-03-24
 * 
 * This script updates the bitcoin_daily_summaries table based on 
 * the data in historical_bitcoin_calculations.
 */

import { db } from "@db";
import { sql } from "drizzle-orm";
import { bitcoinDailySummaries } from "@db/schema";

const TARGET_DATE = '2025-03-24';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function updateDailyBitcoinSummary(date: string, minerModel: string): Promise<void> {
  try {
    console.log(`Updating daily Bitcoin summary for ${date} and ${minerModel}...`);
    
    // Calculate total Bitcoin mined for the day
    console.log(`Executing query for ${date} and ${minerModel}...`);
    const result = await db.execute(sql`
      SELECT
        SUM(CAST(bitcoin_mined AS NUMERIC)) as total_bitcoin
      FROM
        historical_bitcoin_calculations
      WHERE
        settlement_date = ${date}
        AND miner_model = ${minerModel}
    `);
    console.log(`Query result:`, result);
    
    // Access the rows array from the result
    if (!result.rows || result.rows.length === 0 || !result.rows[0].total_bitcoin) {
      console.log(`No Bitcoin data found for ${date} and ${minerModel}`);
      return;
    }
    
    const data = result.rows[0];
    
    // Delete existing summary if any
    await db.execute(sql`
      DELETE FROM bitcoin_daily_summaries
      WHERE summary_date = ${date}
      AND miner_model = ${minerModel}
    `);
    
    // Insert new summary using the schema (without average_difficulty)
    await db.insert(bitcoinDailySummaries).values({
      summaryDate: date,
      minerModel: minerModel,
      bitcoinMined: data.total_bitcoin.toString(),
      updatedAt: new Date(),
      createdAt: new Date()
    });
    
    console.log(`Daily Bitcoin summary updated for ${date} and ${minerModel}: ${data.total_bitcoin} BTC`);
  } catch (error) {
    console.error(`Error updating daily Bitcoin summary for ${date} and ${minerModel}:`, error);
    throw error;
  }
}

async function main() {
  try {
    console.log(`\n=== Starting Bitcoin Daily Summaries Update for ${TARGET_DATE} ===\n`);
    
    const startTime = Date.now();
    
    // Update daily summaries for each miner model
    for (const minerModel of MINER_MODELS) {
      try {
        await updateDailyBitcoinSummary(TARGET_DATE, minerModel);
      } catch (error) {
        console.error(`Error processing ${minerModel}:`, error);
        // Continue with other models even if one fails
      }
    }
    
    const endTime = Date.now();
    
    console.log(`\n=== Bitcoin Daily Summaries Update Completed ===`);
    console.log(`Duration: ${((endTime - startTime) / 1000).toFixed(2)} seconds`);
    
    process.exit(0);
  } catch (error) {
    console.error('Error during Bitcoin daily summaries update:', error);
    process.exit(1);
  }
}

main();
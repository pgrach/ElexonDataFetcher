/**
 * Update Bitcoin Daily Summary Table
 * 
 * This script aggregates the bitcoin_mined values from historical_bitcoin_calculations
 * and creates/updates a record in the bitcoin_daily_summaries table.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

// Default date is 2025-03-28
const DATE_ARG = process.argv[2] || '2025-03-28';

/**
 * Update the bitcoin_daily_summaries table for a specific date
 * @param date The date to update in YYYY-MM-DD format
 */
export async function updateBitcoinDailySummary(date: string = DATE_ARG): Promise<void> {
  console.log(`=== Updating Bitcoin Daily Summary for ${date} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);

  try {
    // Get the difficulty for this date
    const difficultyResult = await db.execute(
      sql`SELECT difficulty FROM historical_bitcoin_calculations 
          WHERE settlement_date = ${date} 
          LIMIT 1`
    );

    if (!difficultyResult.rows || difficultyResult.rows.length === 0) {
      console.error(`No difficulty information found for ${date}`);
      return;
    }

    const difficulty = difficultyResult.rows[0].difficulty;

    // Aggregate bitcoin_mined by miner_model
    const aggregateResult = await db.execute(
      sql`SELECT 
            miner_model,
            SUM(bitcoin_mined) as total_bitcoin_mined
          FROM historical_bitcoin_calculations
          WHERE settlement_date = ${date}
          GROUP BY miner_model`
    );

    if (!aggregateResult.rows || aggregateResult.rows.length === 0) {
      console.error(`No bitcoin mining calculations found for ${date}`);
      return;
    }

    // For each miner model, insert or update the summary record
    for (const row of aggregateResult.rows) {
      const minerModel = row.miner_model;
      const bitcoinMined = parseFloat(row.total_bitcoin_mined);
      
      // Assuming a BTC price of $66,000 for calculation
      const valueAtMining = bitcoinMined * 66000;

      console.log(`Miner model ${minerModel}: ${bitcoinMined.toFixed(8)} BTC (${valueAtMining.toFixed(2)} USD)`);

      // Check if summary already exists
      const existingRecord = await db.execute(
        sql`SELECT id FROM bitcoin_daily_summaries 
            WHERE summary_date = ${date} AND miner_model = ${minerModel}`
      );

      if (existingRecord.rows && existingRecord.rows.length > 0) {
        // Update existing record
        await db.execute(
          sql`UPDATE bitcoin_daily_summaries
              SET bitcoin_mined = ${bitcoinMined},
                  value_at_mining = ${valueAtMining},
                  average_difficulty = ${difficulty},
                  updated_at = NOW()
              WHERE summary_date = ${date} AND miner_model = ${minerModel}`
        );
        console.log(`Updated bitcoin daily summary for ${date}, miner ${minerModel}`);
      } else {
        // Insert new record
        await db.execute(
          sql`INSERT INTO bitcoin_daily_summaries
              (summary_date, miner_model, bitcoin_mined, value_at_mining, average_difficulty, created_at, updated_at)
              VALUES
              (${date}, ${minerModel}, ${bitcoinMined}, ${valueAtMining}, ${difficulty}, NOW(), NOW())`
        );
        console.log(`Created bitcoin daily summary for ${date}, miner ${minerModel}`);
      }
    }

    console.log(`=== Bitcoin Daily Summary Update Completed ===`);
  } catch (error) {
    console.error(`Error updating Bitcoin daily summary for ${date}:`, error);
  }
}

// Main function to run the script
async function main(): Promise<void> {
  try {
    await updateBitcoinDailySummary();
    process.exit(0);
  } catch (error) {
    console.error('Error in main execution:', error);
    process.exit(1);
  }
}

// Execute the main function
main();
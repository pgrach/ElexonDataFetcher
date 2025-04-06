/**
 * Clear All Data for March 22, 2025
 * 
 * This script completely removes all settlement period data for March 22, 2025
 * from both curtailment_records, daily_summaries, and historical_bitcoin_calculations.
 */

import { db } from "./db";

// Create a namespace for this script
console.log("Starting clear_march_22 script");
const TARGET_DATE = "2025-03-22";

async function main(): Promise<void> {
  try {
    console.log(`Starting clearing process for ${TARGET_DATE}`);

    // Clear records from curtailment_records table
    const deleteResult = await db.query(
      `DELETE FROM curtailment_records 
       WHERE settlement_date = $1`,
      [TARGET_DATE]
    );
    console.log(`Deleted ${deleteResult.rowCount} records from curtailment_records`);

    // Clear records from historical_bitcoin_calculations table
    const deleteBitcoinResult = await db.query(
      `DELETE FROM historical_bitcoin_calculations 
       WHERE settlement_date = $1`,
      [TARGET_DATE]
    );
    console.log(`Deleted ${deleteBitcoinResult.rowCount} records from historical_bitcoin_calculations`);

    // Clear daily summary
    const deleteDailySummaryResult = await db.query(
      `DELETE FROM daily_summaries 
       WHERE date = $1`,
      [TARGET_DATE]
    );
    console.log(`Deleted ${deleteDailySummaryResult.rowCount} records from daily_summaries`);

    console.log(`Successfully cleared all data for ${TARGET_DATE}`);
  } catch (error) {
    console.error(`Failed to clear data for ${TARGET_DATE}: ${error}`);
    process.exit(1);
  } finally {
    // Clean up resources
    process.exit(0);
  }
}

main();
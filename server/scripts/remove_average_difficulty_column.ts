/**
 * Remove Average Difficulty Column From Bitcoin Daily Summaries
 * 
 * This script removes the average_difficulty column from the bitcoin_daily_summaries table
 * to follow the DRY principle, as this data is already available in historical_bitcoin_calculations
 */

import { db } from "@db";
import { sql } from "drizzle-orm";

async function removeDifficultyColumn(): Promise<void> {
  try {
    console.log("\n=== Starting Removal of Average Difficulty Column ===\n");
    
    console.log("Checking if column exists...");
    const columnCheckResult = await db.execute(sql`
      SELECT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'bitcoin_daily_summaries' 
        AND column_name = 'average_difficulty'
      ) as column_exists;
    `);
    
    const columnExists = columnCheckResult.rows && 
                         columnCheckResult.rows.length > 0 && 
                         columnCheckResult.rows[0].column_exists === true;
    
    if (!columnExists) {
      console.log("Column 'average_difficulty' doesn't exist in bitcoin_daily_summaries table. Nothing to do.");
      return;
    }
    
    console.log("Column 'average_difficulty' exists. Proceeding with removal...");
    
    // Alter the table to remove the column
    await db.execute(sql`
      ALTER TABLE bitcoin_daily_summaries 
      DROP COLUMN average_difficulty;
    `);
    
    console.log("\n=== Successfully removed average_difficulty column from bitcoin_daily_summaries ===\n");
  } catch (error) {
    console.error("Error removing average_difficulty column:", error);
    throw error;
  }
}

async function main() {
  try {
    const startTime = Date.now();
    
    // Remove the column
    await removeDifficultyColumn();
    
    const endTime = Date.now();
    console.log(`Operation completed in ${((endTime - startTime) / 1000).toFixed(2)} seconds.`);
    
    process.exit(0);
  } catch (error) {
    console.error("Script failed:", error);
    process.exit(1);
  }
}

// Run the script
main();
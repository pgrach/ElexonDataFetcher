/**
 * Clear All Data for March 22, 2025
 * 
 * This script completely removes all settlement period data for March 22, 2025
 * from both curtailment_records, daily_summaries, and historical_bitcoin_calculations.
 */

import { db } from "./db";
import { Logger } from "./server/utils/logger";

const logger = new Logger("clear_march_22");
const TARGET_DATE = "2025-03-22";

async function main(): Promise<void> {
  try {
    logger.info(`Starting clearing process for ${TARGET_DATE}`);

    // Clear records from curtailment_records table
    const deleteResult = await db.query(
      `DELETE FROM curtailment_records 
       WHERE settlement_date = $1`,
      [TARGET_DATE]
    );
    logger.success(`Deleted ${deleteResult.rowCount} records from curtailment_records`);

    // Clear records from historical_bitcoin_calculations table
    const deleteBitcoinResult = await db.query(
      `DELETE FROM historical_bitcoin_calculations 
       WHERE settlement_date = $1`,
      [TARGET_DATE]
    );
    logger.success(`Deleted ${deleteBitcoinResult.rowCount} records from historical_bitcoin_calculations`);

    // Clear daily summary
    const deleteDailySummaryResult = await db.query(
      `DELETE FROM daily_summaries 
       WHERE date = $1`,
      [TARGET_DATE]
    );
    logger.success(`Deleted ${deleteDailySummaryResult.rowCount} records from daily_summaries`);

    logger.success(`Successfully cleared all data for ${TARGET_DATE}`);
  } catch (error) {
    logger.error(`Failed to clear data for ${TARGET_DATE}: ${error}`);
    process.exit(1);
  } finally {
    await db.end();
    process.exit(0);
  }
}

main();
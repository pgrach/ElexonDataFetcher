/**
 * Clear All Data for March 21, 2025
 * 
 * This script completely removes all settlement period data for March 21, 2025
 * from both curtailment_records, daily_summaries, and historical_bitcoin_calculations.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-21';

async function main(): Promise<void> {
  console.log(`=== Clearing All Data for ${TARGET_DATE} ===`);
  console.log(`Started at: ${new Date().toISOString()}`);
  
  try {
    console.log(`Clearing all existing data for ${TARGET_DATE}...`);
    
    // First, delete from historical_bitcoin_calculations
    const bitcoinDeleteResult = await db.execute(
      sql`DELETE FROM historical_bitcoin_calculations WHERE settlement_date = ${TARGET_DATE}`
    );
    console.log(`Deleted Bitcoin calculation records for ${TARGET_DATE}`);
    
    // Next, delete from curtailment_records
    const deleteResult = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Deleted ${deleteResult.rowCount} curtailment records for ${TARGET_DATE}`);
    
    // Then delete from daily_summaries
    const summaryDeleteResult = await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`Deleted ${summaryDeleteResult.rowCount} daily summary records for ${TARGET_DATE}`);
    
    console.log(`\nAll data for ${TARGET_DATE} has been successfully cleared at ${new Date().toISOString()}`);
  } catch (error) {
    console.error('Error during data clearing process:', error);
    process.exit(1);
  }
}

// Run the script
main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
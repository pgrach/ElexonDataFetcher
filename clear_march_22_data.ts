/**
 * Clear All Data for March 22, 2025
 * 
 * This script completely removes all settlement period data for March 22, 2025
 * from both curtailment_records, daily_summaries, and historical_bitcoin_calculations.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

// Target date
const TARGET_DATE = '2025-03-22';

async function main(): Promise<void> {
  console.log(`Clearing ALL data for ${TARGET_DATE}...`);
  
  try {
    // Delete from curtailment_records
    const curtailmentResult = await db.execute(sql`
      DELETE FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    console.log(`Deleted ${curtailmentResult.rowCount} records from curtailment_records table`);
    
    // Delete from historical_bitcoin_calculations
    const bitcoinResult = await db.execute(sql`
      DELETE FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    console.log(`Deleted ${bitcoinResult.rowCount} records from historical_bitcoin_calculations table`);
    
    // Delete from daily_summaries
    const summaryResult = await db.execute(sql`
      DELETE FROM daily_summaries
      WHERE date = ${TARGET_DATE}
    `);
    
    console.log(`Deleted ${summaryResult.rowCount} records from daily_summaries table`);
    
    console.log('----------------------------------------');
    console.log(`All data for ${TARGET_DATE} has been successfully cleared`);
    console.log('----------------------------------------');
    
  } catch (error) {
    console.error('Error clearing data:', error);
    process.exit(1);
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
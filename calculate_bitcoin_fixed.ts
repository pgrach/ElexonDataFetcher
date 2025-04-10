/**
 * Simplified Bitcoin Mining Potential Calculator for 2025-04-03
 * 
 * This script uses a default difficulty value instead of trying to fetch from DynamoDB.
 */

import { db } from './db';
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations,
  bitcoinDailySummaries
} from './db/schema';
import { and, eq, sql } from 'drizzle-orm';
import { calculateBitcoin } from './server/utils/bitcoin';

// Configuration
const TARGET_DATE = '2025-04-03';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const DEFAULT_DIFFICULTY = 113757508810853; // Using a known difficulty value for consistency

/**
 * Simple logging utility with timestamps
 */
function log(message: string): void {
  const timestamp = new Date().toISOString().substring(11, 19);
  console.log(`[${timestamp}] ${message}`);
}

/**
 * Process Bitcoin calculations for all miner models with improved error handling
 */
async function processBitcoinCalculations(): Promise<void> {
  log(`Processing Bitcoin calculations for ${TARGET_DATE}...`);
  
  try {
    log(`Using default difficulty: ${DEFAULT_DIFFICULTY}`);
    
    // Get all curtailment records for the target date
    const records = await db
      .select({
        id: curtailmentRecords.id,
        settlementPeriod: curtailmentRecords.settlementPeriod,
        farmId: curtailmentRecords.farmId,
        volume: curtailmentRecords.volume
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    log(`Found ${records.length} curtailment records for ${TARGET_DATE}`);
    
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      log(`Processing ${minerModel} miner model...`);
      
      // Delete all existing records for this date and model
      await db.execute(sql`
        DELETE FROM historical_bitcoin_calculations 
        WHERE settlement_date = ${TARGET_DATE} 
        AND miner_model = ${minerModel}
      `);
      
      log(`Cleared existing Bitcoin calculations for ${minerModel}`);
      
      // Calculate total Bitcoin
      let totalBitcoin = 0;
      
      // Insert Bitcoin calculations using UPSERT (INSERT ... ON CONFLICT) 
      const insertPromises = [];
      
      for (const record of records) {
        const mwh = Math.abs(parseFloat(record.volume.toString()));
        const bitcoinMined = calculateBitcoin(mwh, minerModel, DEFAULT_DIFFICULTY);
        totalBitcoin += bitcoinMined;
        
        // Use SQL query with ON CONFLICT DO UPDATE to handle duplicates
        insertPromises.push(
          db.execute(sql`
            INSERT INTO historical_bitcoin_calculations 
            (settlement_date, settlement_period, miner_model, farm_id, bitcoin_mined, difficulty, calculated_at)
            VALUES 
            (${TARGET_DATE}, ${record.settlementPeriod}, ${minerModel}, ${record.farmId}, ${bitcoinMined.toString()}, ${DEFAULT_DIFFICULTY.toString()}, NOW())
            ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
            DO UPDATE SET
              bitcoin_mined = ${bitcoinMined.toString()},
              difficulty = ${DEFAULT_DIFFICULTY.toString()},
              calculated_at = NOW()
          `)
        );
      }
      
      try {
        await Promise.all(insertPromises);
        log(`Successfully inserted ${insertPromises.length} Bitcoin calculations for ${minerModel}`);
      } catch (error) {
        log(`Error inserting Bitcoin calculations: ${(error as Error).message}`);
      }
      
      log(`Calculated ${totalBitcoin.toFixed(8)} BTC for ${minerModel}`);
      
      // Update Bitcoin daily summary
      await db.execute(sql`
        INSERT INTO bitcoin_daily_summaries 
        (summary_date, miner_model, bitcoin_mined)
        VALUES 
        (${TARGET_DATE}, ${minerModel}, ${totalBitcoin.toString()})
        ON CONFLICT (summary_date, miner_model) 
        DO UPDATE SET
          bitcoin_mined = ${totalBitcoin.toString()}
      `);
      
      log(`Updated Bitcoin daily summary for ${minerModel}`);
    }
    
    // Verify calculations
    for (const minerModel of MINER_MODELS) {
      const summary = await db
        .select({ bitcoinMined: bitcoinDailySummaries.bitcoinMined })
        .from(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      if (summary.length > 0) {
        log(`${minerModel} daily summary: ${parseFloat(summary[0].bitcoinMined?.toString() || '0').toFixed(8)} BTC`);
      }
    }
  } catch (error) {
    log(`Error processing Bitcoin calculations: ${(error as Error).message}`);
    throw error;
  }
}

// Execute the calculations
processBitcoinCalculations()
  .then(() => {
    console.log('\nBitcoin calculations completed successfully.');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\nBitcoin calculations failed with error:', error);
    process.exit(1);
  });
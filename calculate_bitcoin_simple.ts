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
 * Process Bitcoin calculations for all miner models
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
      
      // Clear existing Bitcoin calculations for this date and model
      const deleteResult = await db.delete(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        ))
        .returning({ id: historicalBitcoinCalculations.id });
      
      log(`Cleared ${deleteResult.length} existing Bitcoin calculations for ${minerModel}`);
      
      // Calculate Bitcoin mining potential for each curtailment record
      let totalBitcoin = 0;
      
      for (const record of records) {
        const mwh = Math.abs(parseFloat(record.volume.toString()));
        const bitcoinMined = calculateBitcoin(mwh, minerModel, DEFAULT_DIFFICULTY);
        totalBitcoin += bitcoinMined;
        
        try {
          await db.insert(historicalBitcoinCalculations).values({
            settlementDate: TARGET_DATE,
            settlementPeriod: Number(record.settlementPeriod),
            minerModel: minerModel,
            farmId: record.farmId,
            bitcoinMined: bitcoinMined.toString(),
            difficulty: DEFAULT_DIFFICULTY.toString(),
            calculatedAt: new Date()
          });
        } catch (error) {
          log(`Error inserting Bitcoin calculation for record ${record.id}: ${(error as Error).message}`);
        }
      }
      
      log(`Calculated ${totalBitcoin.toFixed(8)} BTC for ${minerModel}`);
      
      // Clear existing Bitcoin daily summary
      await db.delete(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      // Insert new Bitcoin daily summary
      await db.insert(bitcoinDailySummaries).values({
        summaryDate: TARGET_DATE,
        minerModel: minerModel,
        bitcoinMined: totalBitcoin.toString()
      });
      
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
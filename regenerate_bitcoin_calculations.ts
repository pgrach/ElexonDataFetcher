/**
 * Regenerate Bitcoin Calculations for a Specific Date
 * 
 * This script focuses only on regenerating Bitcoin calculations
 * after the curtailment data has been cleaned up.
 * 
 * Usage:
 *   npx tsx regenerate_bitcoin_calculations.ts
 */

import { processSingleDay } from "./server/services/bitcoinService";
import { db } from "./db";
import { historicalBitcoinCalculations } from "./db/schema";
import { eq, sql } from "drizzle-orm";

// Explicitly import the processSingleDay function by fixing its signature
declare module "./server/services/bitcoinService" {
  export function processSingleDay(date: string, minerModel: string): Promise<void>;
}

const TARGET_DATE = '2025-03-05';

async function regenerateBitcoinCalculations() {
  console.log(`\n=== Regenerating Bitcoin Calculations for ${TARGET_DATE} ===\n`);
  
  try {
    // Step 1: Delete any existing Bitcoin calculations for this date
    console.log(`Removing existing Bitcoin calculations for ${TARGET_DATE}...`);
    const deletedCount = await db
      .delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    console.log(`Deleted ${deletedCount} existing Bitcoin calculations`);
    
    // Step 2: Check curtailment records status
    const curtailmentCheck = await db.execute(sql`
      SELECT 
        COUNT(*) AS record_count,
        COUNT(DISTINCT settlement_period) AS period_count,
        SUM(ABS(volume::numeric)) AS total_volume,
        SUM(payment::numeric) AS total_payment
      FROM curtailment_records 
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    console.log(`Found ${curtailmentCheck[0].recordCount} curtailment records across ${curtailmentCheck[0].periodCount} periods`);
    console.log(`Total volume: ${Number(curtailmentCheck[0].totalVolume).toFixed(2)} MWh`);
    console.log(`Total payment: £${Number(curtailmentCheck[0].totalPayment).toFixed(2)}`);
    
    // Process for all miner models
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    console.log(`\nProcessing Bitcoin calculations for ${minerModels.length} miner models...`);
    
    for (const model of minerModels) {
      console.log(`Processing model: ${model}`);
      await processSingleDay(TARGET_DATE, model);
    }
    
    // Verify Bitcoin calculations
    const bitcoinCheck = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
        modelCount: sql<number>`COUNT(DISTINCT miner_model)`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
      
    console.log(`\nBitcoin calculation summary: ${bitcoinCheck[0].recordCount} records`);
    console.log(`Periods: ${bitcoinCheck[0].periodCount}, Farms: ${bitcoinCheck[0].farmCount}, Models: ${bitcoinCheck[0].modelCount}`);
    
    console.log(`\n=== Processing Complete for ${TARGET_DATE} ===`);
  } catch (error) {
    console.error(`Error regenerating Bitcoin calculations for ${TARGET_DATE}:`, error);
  }
}

// Execute the processing
regenerateBitcoinCalculations();
/**
 * Generate M20S Bitcoin Calculations for 2025-04-02 using direct SQL
 * 
 * This script uses direct SQL queries to generate historical Bitcoin calculations
 * for the M20S miner model specifically for 2025-04-02, then updates the daily summary.
 */

import { db } from "@db";
import { performance } from "perf_hooks";
import { sql } from "drizzle-orm";
import { getDifficultyData } from "../../server/services/dynamodbService";

// Target date and miner model
const TARGET_DATE = '2025-04-02';
const MINER_MODEL = 'M20S';

// M20S specs (J/TH)
const M20S_EFFICIENCY = 50; // J/TH - energy consumption per terahash

/**
 * Calculate Bitcoin mining potential for M20S miners
 */
function calculateBitcoinMiningPotential(
  volume: string | number, 
  difficulty: number
): number {
  // Energy in MWh
  const volumeValue = typeof volume === 'string' 
    ? parseFloat(volume) 
    : volume;
  
  const energy = Math.abs(volumeValue);
  
  // Convert MWh to Joules
  const joules = energy * 3600000000; // 1 MWh = 3.6 billion joules
  
  // Calculate hashing power (TH) based on miner efficiency
  const hashingPower = joules / M20S_EFFICIENCY;
  
  // Calculate Bitcoin mined
  // Formula: (hashingPower * 100000000 * 3600) / (difficulty * 2^32)
  const bitcoinMined = (hashingPower * 100000000 * 3600) / (difficulty * Math.pow(2, 32));
  
  return bitcoinMined;
}

/**
 * Process and store Bitcoin calculations for the M20S model
 */
async function processM20SBitcoinCalculations(): Promise<void> {
  console.log(`\n==== Processing M20S Bitcoin Calculations for ${TARGET_DATE} ====\n`);
  
  try {
    // First, delete any existing calculations for this date and miner model
    console.log('Deleting any existing calculations...');
    
    await db.execute(sql`
      DELETE FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE}
      AND miner_model = ${MINER_MODEL}
    `);
    
    console.log('Deletion completed.');
    
    // Get Bitcoin difficulty for the target date
    const difficulty = await getDifficultyData(TARGET_DATE);
    console.log(`Using Bitcoin difficulty: ${difficulty}`);
    
    // Get negative volume curtailment records for the target date
    const curtailmentResult = await db.execute(sql`
      SELECT 
        id, 
        settlement_date, 
        settlement_period, 
        farm_id, 
        volume
      FROM 
        curtailment_records
      WHERE 
        settlement_date = ${TARGET_DATE}
        AND volume < 0
    `);
    
    const curtailmentRecords = curtailmentResult.rows;
    
    if (!curtailmentRecords || curtailmentRecords.length === 0) {
      throw new Error(`No curtailment records found for ${TARGET_DATE}`);
    }
    
    console.log(`Found ${curtailmentRecords.length} curtailment records for ${TARGET_DATE}`);
    
    // Process each curtailment record
    let calculationsCount = 0;
    for (const record of curtailmentRecords) {
      // Calculate Bitcoin mining potential
      const bitcoinMined = calculateBitcoinMiningPotential(record.volume, difficulty);
      
      // Insert the calculation record directly with SQL
      await db.execute(sql`
        INSERT INTO historical_bitcoin_calculations (
          settlement_date, 
          settlement_period, 
          farm_id, 
          miner_model, 
          bitcoin_mined, 
          difficulty
        ) VALUES (
          ${record.settlement_date}, 
          ${record.settlement_period}, 
          ${record.farm_id}, 
          ${MINER_MODEL}, 
          ${bitcoinMined.toString()}, 
          ${difficulty.toString()}
        )
        ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
        DO UPDATE SET 
          bitcoin_mined = ${bitcoinMined.toString()}, 
          difficulty = ${difficulty.toString()}
      `);
      
      calculationsCount++;
      
      // Log progress every 100 records
      if (calculationsCount % 100 === 0) {
        console.log(`Processed ${calculationsCount} records...`);
      }
    }
    
    console.log(`\nProcessed ${calculationsCount} M20S Bitcoin calculations for ${TARGET_DATE}`);
    
    // Update the daily summary
    await updateDailySummary();
    
  } catch (error) {
    console.error(`Error processing M20S Bitcoin calculations:`, error);
    throw error;
  }
}

/**
 * Update the Bitcoin daily summary for M20S
 */
async function updateDailySummary(): Promise<void> {
  console.log(`\n==== Updating M20S Bitcoin Daily Summary for ${TARGET_DATE} ====\n`);
  
  try {
    // Calculate total Bitcoin mined for the day
    const bitcoinTotal = await db.execute(sql`
      SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE}
      AND miner_model = ${MINER_MODEL}
    `);
    
    const totalBitcoin = bitcoinTotal.rows?.[0]?.total_bitcoin;
    
    if (!totalBitcoin) {
      console.log(`No Bitcoin total could be calculated for ${MINER_MODEL}`);
      return;
    }
    
    // Delete existing summary if any
    await db.execute(sql`
      DELETE FROM bitcoin_daily_summaries
      WHERE summary_date = ${TARGET_DATE}
      AND miner_model = ${MINER_MODEL}
    `);
    
    // Insert new summary
    await db.execute(sql`
      INSERT INTO bitcoin_daily_summaries (
        summary_date, 
        miner_model, 
        bitcoin_mined, 
        created_at, 
        updated_at
      ) VALUES (
        ${TARGET_DATE}, 
        ${MINER_MODEL}, 
        ${totalBitcoin.toString()}, 
        NOW(), 
        NOW()
      )
    `);
    
    console.log(`Updated Bitcoin daily summary for ${MINER_MODEL}: ${totalBitcoin} BTC`);
    
  } catch (error) {
    console.error(`Error updating M20S Bitcoin daily summary:`, error);
    throw error;
  }
}

/**
 * Verify Bitcoin calculations have been created
 */
async function verifyBitcoinCalculations(): Promise<void> {
  console.log(`\n==== Verifying M20S Bitcoin Calculations for ${TARGET_DATE} ====\n`);
  
  // Check historical calculations
  const calculationsResult = await db.execute(sql`
    SELECT COUNT(*) as count
    FROM historical_bitcoin_calculations
    WHERE settlement_date = ${TARGET_DATE}
    AND miner_model = ${MINER_MODEL}
  `);
  
  const calculationsCount = calculationsResult.rows?.[0]?.count || 0;
  console.log(`Found ${calculationsCount} ${MINER_MODEL} historical Bitcoin calculations for ${TARGET_DATE}`);
  
  // Check daily summary
  const dailySummaryResult = await db.execute(sql`
    SELECT *
    FROM bitcoin_daily_summaries
    WHERE summary_date = ${TARGET_DATE}
    AND miner_model = ${MINER_MODEL}
  `);
  
  if (dailySummaryResult.rows?.length > 0) {
    console.log(`Bitcoin daily summary for ${MINER_MODEL}: ${dailySummaryResult.rows[0].bitcoin_mined} BTC`);
  } else {
    console.log(`No Bitcoin daily summary found for ${MINER_MODEL}`);
  }
}

/**
 * Main function to generate M20S Bitcoin calculations
 */
async function main(): Promise<void> {
  const startTime = performance.now();
  
  try {
    console.log(`\n==== Starting M20S Bitcoin Calculations Generation for ${TARGET_DATE} ====\n`);
    
    // Process and store calculations
    await processM20SBitcoinCalculations();
    
    // Verify calculations have been created
    await verifyBitcoinCalculations();
    
    const endTime = performance.now();
    const durationSeconds = ((endTime - startTime) / 1000).toFixed(2);
    
    console.log(`\n==== M20S Bitcoin Calculations Generation Completed ====`);
    console.log(`Total execution time: ${durationSeconds} seconds`);
    
  } catch (error) {
    console.error(`Error during M20S Bitcoin calculations generation:`, error);
    throw error;
  }
}

// Execute the generation process
main()
  .then(() => {
    console.log('M20S Bitcoin calculations generation completed successfully.');
    process.exit(0);
  })
  .catch((error) => {
    console.error('M20S Bitcoin calculations generation failed with error:', error);
    process.exit(1);
  });
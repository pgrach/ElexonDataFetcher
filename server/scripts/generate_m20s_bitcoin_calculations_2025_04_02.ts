/**
 * Generate M20S Bitcoin Calculations for 2025-04-02
 * 
 * This script generates historical Bitcoin calculations for the M20S miner model
 * specifically for 2025-04-02, then updates the daily Bitcoin summary.
 */

import { db } from "@db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries,
  insertHistoricalBitcoinCalculationSchema,
  insertBitcoinDailySummarySchema 
} from "@db/schema";
import { and, eq, sql } from "drizzle-orm";
import { performance } from "perf_hooks";
import { getDifficultyData } from "../../server/services/dynamodbService";

// Target date
const TARGET_DATE = '2025-04-02';
const MINER_MODEL = 'M20S';

// M20S specs (J/TH)
const M20S_EFFICIENCY = 50; // J/TH - energy consumption per terahash

interface CurtailmentData {
  id: number;
  settlementDate: string;
  settlementPeriod: number;
  volume: string | number;
  farmId: string;
  leadParty?: string;
}

/**
 * Calculate Bitcoin mining potential for M20S miners
 */
async function calculateBitcoinMiningPotential(
  curtailmentData: CurtailmentData, 
  difficulty: number
): Promise<number> {
  // Energy in MWh
  const volumeValue = typeof curtailmentData.volume === 'string' 
    ? parseFloat(curtailmentData.volume) 
    : curtailmentData.volume;
  
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
    // First, check and delete any existing calculations for this date and miner model
    console.log('Checking for existing calculations...');
    const existingCalculations = await db.select({ count: sql<number>`COUNT(*)::int` })
      .from(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
        eq(historicalBitcoinCalculations.minerModel, MINER_MODEL)
      ));
      
    const existingCount = existingCalculations[0]?.count || 0;
    if (existingCount > 0) {
      console.log(`Found ${existingCount} existing M20S calculations for ${TARGET_DATE}. Deleting...`);
      await db.delete(historicalBitcoinCalculations)
        .where(and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, MINER_MODEL)
        ));
      console.log('Deleted existing calculations successfully.');
    } else {
      console.log('No existing calculations found.');
    }
    
    // Get Bitcoin difficulty for the target date
    const difficulty = await getDifficultyData(TARGET_DATE);
    const difficultyData = { difficulty };
    
    console.log(`Using Bitcoin difficulty: ${difficulty}`);
    
    // Get curtailment records for the target date
    const curtailmentData = await db.select({
      id: curtailmentRecords.id,
      settlementDate: curtailmentRecords.settlementDate,
      settlementPeriod: curtailmentRecords.settlementPeriod,
      volume: curtailmentRecords.volume,
      farmId: curtailmentRecords.farmId
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (!curtailmentData.length) {
      throw new Error(`No curtailment records found for ${TARGET_DATE}`);
    }
    
    console.log(`Found ${curtailmentData.length} curtailment records for ${TARGET_DATE}`);
    
    // Process each curtailment record
    let calculationsCount = 0;
    const batchSize = 100;
    let batch: any[] = [];
    
    for (const record of curtailmentData) {
      // Skip positive volumes (we're only interested in curtailment - negative volumes)
      const volumeValue = typeof record.volume === 'string' 
        ? parseFloat(record.volume) 
        : record.volume;
        
      if (volumeValue >= 0) {
        continue;
      }
      
      // Calculate Bitcoin mining potential
      const bitcoinMined = await calculateBitcoinMiningPotential(record, difficultyData.difficulty);
      
      // Create historical Bitcoin calculation record
      const calculationRecord = insertHistoricalBitcoinCalculationSchema.parse({
        settlementDate: record.settlementDate,
        settlementPeriod: record.settlementPeriod,
        minerModel: MINER_MODEL,
        farmId: record.farmId,
        bitcoinMined: bitcoinMined.toString(),
        difficulty: difficultyData.difficulty.toString(),
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      batch.push(calculationRecord);
      calculationsCount++;
      
      // Insert in batches for better performance
      if (batch.length >= batchSize) {
        await db.insert(historicalBitcoinCalculations).values(batch);
        console.log(`Inserted ${batch.length} calculation records...`);
        batch = [];
      }
    }
    
    // Insert any remaining records
    if (batch.length > 0) {
      await db.insert(historicalBitcoinCalculations).values(batch);
      console.log(`Inserted final ${batch.length} calculation records...`);
    }
    
    console.log(`Processed ${calculationsCount} M20S Bitcoin calculations for ${TARGET_DATE}`);
    
    // Update the daily summary
    await updateDailySummary(calculationsCount);
    
  } catch (error) {
    console.error(`Error processing M20S Bitcoin calculations:`, error);
    throw error;
  }
}

/**
 * Update the Bitcoin daily summary for M20S
 */
async function updateDailySummary(calculationsCount: number): Promise<void> {
  console.log(`\n==== Updating M20S Bitcoin Daily Summary for ${TARGET_DATE} ====\n`);
  
  try {
    if (calculationsCount === 0) {
      console.log(`No calculations to summarize for ${TARGET_DATE}`);
      return;
    }
    
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
    await db.delete(bitcoinDailySummaries)
      .where(and(
        eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
        eq(bitcoinDailySummaries.minerModel, MINER_MODEL)
      ));
    
    // Insert new summary
    await db.insert(bitcoinDailySummaries).values({
      summaryDate: TARGET_DATE,
      minerModel: MINER_MODEL,
      bitcoinMined: totalBitcoin.toString(),
      updatedAt: new Date(),
      createdAt: new Date()
    });
    
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
  const calculationsCount = await db.select({ count: sql<number>`COUNT(*)::int` })
    .from(historicalBitcoinCalculations)
    .where(and(
      eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
      eq(historicalBitcoinCalculations.minerModel, MINER_MODEL)
    ));
  
  console.log(`Found ${calculationsCount[0]?.count || 0} ${MINER_MODEL} historical Bitcoin calculations for ${TARGET_DATE}`);
  
  // Check daily summary
  const dailySummary = await db.select()
    .from(bitcoinDailySummaries)
    .where(and(
      eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
      eq(bitcoinDailySummaries.minerModel, MINER_MODEL)
    ));
  
  if (dailySummary.length > 0) {
    console.log(`Bitcoin daily summary for ${MINER_MODEL}: ${dailySummary[0].bitcoinMined} BTC`);
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
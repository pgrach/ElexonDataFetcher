/**
 * Recalculate Bitcoin Mining Potential for 2025-04-03
 * 
 * This script calculates the Bitcoin mining potential for all miner models
 * based on the curtailment records we just created.
 */

import { db } from './db';
import { curtailmentRecords, historicalBitcoinCalculations, bitcoinDailySummaries } from './db/schema';
import { eq, and, sql } from 'drizzle-orm';
import * as fs from 'fs';

// Configuration
const TARGET_DATE = '2025-04-03';
const LOG_FILE_PATH = `./logs/recalculate_april3_bitcoin_${new Date().toISOString().replace(/:/g, '-')}.log`;
const DEFAULT_DIFFICULTY = 113757508810853; // Default difficulty value
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Mining efficiency by miner model (TH/s per MW)
const MINING_EFFICIENCIES = {
  'S19J_PRO': 100.0,
  'S9': 13.5,
  'M20S': 68.0
};

// Terahashes per Bitcoin based on difficulty
function terahashesPerBitcoin(difficulty: number): number {
  return difficulty / 100000000;
}

/**
 * Simple logging utility with timestamps
 */
function log(message: string): void {
  const timestamp = new Date().toISOString().substring(11, 19);
  const logMessage = `[${timestamp}] ${message}`;
  console.log(logMessage);
  
  // Append to log file
  fs.appendFileSync(LOG_FILE_PATH, logMessage + '\n');
}

/**
 * Clear existing Bitcoin calculations for the target date
 */
async function clearExistingBitcoinCalculations(): Promise<void> {
  log(`Clearing existing Bitcoin calculations for ${TARGET_DATE}...`);
  
  for (const minerModel of MINER_MODELS) {
    // Clear historical calculations
    const histResult = await db.delete(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      ))
      .returning({
        id: historicalBitcoinCalculations.id
      });
    
    log(`Cleared ${histResult.length} historical Bitcoin calculations for ${minerModel}`);
    
    // Clear daily summaries
    const summaryResult = await db.delete(bitcoinDailySummaries)
      .where(and(
        eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
        eq(bitcoinDailySummaries.minerModel, minerModel)
      ))
      .returning({
        id: bitcoinDailySummaries.id
      });
    
    log(`Cleared ${summaryResult.length} Bitcoin daily summaries for ${minerModel}`);
  }
}

/**
 * Calculate Bitcoin mining potential for a specific miner model
 */
async function calculateBitcoinForModel(minerModel: string): Promise<void> {
  log(`Calculating Bitcoin potential for ${TARGET_DATE} with model ${minerModel}...`);
  
  // Get all curtailment records for the target date
  const records = await db
    .select()
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  log(`Found ${records.length} curtailment records for Bitcoin calculations`);
  
  if (records.length === 0) {
    throw new Error(`No curtailment records found for ${TARGET_DATE}`);
  }
  
  const miningEfficiency = MINING_EFFICIENCIES[minerModel as keyof typeof MINING_EFFICIENCIES] || 100.0;
  const thPerBitcoin = terahashesPerBitcoin(DEFAULT_DIFFICULTY);
  
  let totalBitcoin = 0;
  let recordsProcessed = 0;
  
  // Process in batches of 50
  const batchSize = 50;
  const batches = Math.ceil(records.length / batchSize);
  
  for (let batchIndex = 0; batchIndex < batches; batchIndex++) {
    const startIdx = batchIndex * batchSize;
    const endIdx = Math.min(startIdx + batchSize, records.length);
    const batchRecords = records.slice(startIdx, endIdx);
    
    for (const record of batchRecords) {
      const energy = parseFloat(record.volume?.toString() || '0');
      
      if (energy <= 0) continue;
      
      // Calculate how many terahashes this energy could produce
      // Energy is in MWh, so we multiply by 0.5 to get the effective hours (assuming 30min periods)
      const terahashes = energy * miningEfficiency * 0.5;
      
      // Calculate Bitcoin that could be mined
      const bitcoin = terahashes / thPerBitcoin;
      
      // Insert historical calculation
      await db.execute(sql`
        INSERT INTO historical_bitcoin_calculations (
          settlement_date, settlement_period, farm_id, miner_model,
          bitcoin_mined, difficulty, calculated_at
        ) VALUES (
          ${TARGET_DATE}, ${record.settlementPeriod}, ${record.farmId}, 
          ${minerModel}, ${bitcoin.toString()}, ${DEFAULT_DIFFICULTY.toString()}, NOW()
        )
      `);
      
      totalBitcoin += bitcoin;
      recordsProcessed++;
    }
    
    log(`Batch ${batchIndex + 1}/${batches}: Processed ${batchRecords.length} records`);
  }
  
  log(`Processed ${recordsProcessed} Bitcoin calculations for ${minerModel}`);
  log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)} BTC`);
  
  // Create daily summary
  await db.execute(sql`
    INSERT INTO bitcoin_daily_summaries (
      summary_date, miner_model, bitcoin_mined, created_at, updated_at
    ) VALUES (
      ${TARGET_DATE}, ${minerModel}, ${totalBitcoin.toString()}, NOW(), NOW()
    )
  `);
  
  log(`Updated daily Bitcoin summary for ${minerModel}: ${totalBitcoin.toFixed(8)} BTC`);
}

/**
 * Run the recalculation process
 */
async function runRecalculation(): Promise<void> {
  try {
    log(`Starting Bitcoin recalculation for ${TARGET_DATE}...`);
    
    // Step 1: Clear existing Bitcoin calculations
    await clearExistingBitcoinCalculations();
    
    // Step 2: Calculate Bitcoin for each miner model
    for (const minerModel of MINER_MODELS) {
      await calculateBitcoinForModel(minerModel);
    }
    
    // Step 3: Verify calculations
    for (const minerModel of MINER_MODELS) {
      const summary = await db
        .select()
        .from(bitcoinDailySummaries)
        .where(and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        ));
      
      if (summary.length > 0) {
        log(`Verified ${minerModel} summary: ${parseFloat(summary[0].bitcoinMined?.toString() || '0').toFixed(8)} BTC`);
      }
    }
    
    log(`Bitcoin recalculation for ${TARGET_DATE} completed successfully`);
  } catch (error) {
    log(`Error during Bitcoin recalculation: ${(error as Error).message}`);
    throw error;
  }
}

// Create logs directory if it doesn't exist
if (!fs.existsSync('./logs')) {
  fs.mkdirSync('./logs');
}

// Execute the recalculation process
runRecalculation()
  .then(() => {
    console.log('\nBitcoin recalculation completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\nBitcoin recalculation failed:', error);
    process.exit(1);
  });
/**
 * Bitcoin Calculations for 2025-04-10
 * 
 * This script focuses specifically on updating the Bitcoin calculations for 2025-04-10
 * using a fixed difficulty value to match the rest of the April data.
 */

import { db } from '../db';
import { curtailmentRecords, historicalBitcoinCalculations, bitcoinDailySummaries } from '../db/schema';
import { eq, and, sql } from 'drizzle-orm';

// Constants
const TARGET_DATE = '2025-04-10';
const DIFFICULTY = 113757508810853; // Using difficulty from April data
const BITCOIN_PRICE = 63113.219703914976; // Current price as of processing

// Miner models with their specs
const MINER_MODELS = {
  S19J_PRO: { hashrate: 104, power: 3068 },
  S9: { hashrate: 13.5, power: 1323 },
  M20S: { hashrate: 68, power: 3360 }
};

/**
 * Calculate Bitcoin based on energy, miner model, and difficulty
 */
function calculateBitcoin(mwh: number, minerModel: string, difficulty: number): number {
  // Get miner stats
  const minerStats = MINER_MODELS[minerModel as keyof typeof MINER_MODELS];
  if (!minerStats) {
    throw new Error(`Invalid miner model: ${minerModel}`);
  }
  
  // Convert MWh to joules
  const energyJoules = mwh * 3.6e9; // 1 MWh = 3.6 GJ = 3.6e9 J
  
  // Calculate hashes per joule for this miner
  const hashesPerJoule = minerStats.hashrate * 1e12 / minerStats.power; // TH/s to H/s divided by power
  
  // Calculate total hashes possible with this energy
  const totalHashes = energyJoules * hashesPerJoule;
  
  // Calculate probability of finding a block per hash
  const probabilityPerHash = 1 / (difficulty * Math.pow(2, 32));
  
  // Calculate expected bitcoin (6.25 BTC per block currently)
  const bitcoinMined = totalHashes * probabilityPerHash * 6.25;
  
  return bitcoinMined;
}

/**
 * Process Bitcoin calculations for a specific miner model
 */
async function processModelCalculations(minerModel: string): Promise<{
  success: boolean;
  recordCount: number;
  totalBitcoin: number;
}> {
  try {
    console.log(`\nProcessing Bitcoin calculations for ${TARGET_DATE} with model ${minerModel}...`);
    
    // Delete any existing calculations for this date and model
    await db.delete(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      ));
    
    console.log(`Deleted existing calculations for ${TARGET_DATE} and ${minerModel}`);
    
    // Get curtailment records for this date
    const records = await db.select()
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    if (records.length === 0) {
      console.log(`No curtailment records found for ${TARGET_DATE}`);
      return { success: false, recordCount: 0, totalBitcoin: 0 };
    }
    
    console.log(`Found ${records.length} curtailment records for ${TARGET_DATE}`);
    
    // Calculate and insert Bitcoin calculations
    let totalBitcoin = 0;
    let insertCount = 0;
    
    for (const record of records) {
      // Use absolute value for energy calculation
      const energy = Math.abs(Number(record.volume));
      
      // Skip records with zero or invalid energy
      if (energy <= 0 || isNaN(energy)) {
        continue;
      }
      
      // Calculate Bitcoin mined
      const bitcoinMined = calculateBitcoin(energy, minerModel, DIFFICULTY);
      totalBitcoin += bitcoinMined;
      
      // Insert calculation record
      await db.insert(historicalBitcoinCalculations).values({
        settlementDate: TARGET_DATE,
        settlementPeriod: record.settlementPeriod,
        minerModel: minerModel,
        farmId: record.farmId,
        bitcoinMined: bitcoinMined.toString(),
        difficulty: DIFFICULTY.toString(),
        calculationParams: JSON.stringify({
          energy,
          minerModel,
          difficulty: DIFFICULTY
        })
      });
      
      insertCount++;
    }
    
    console.log(`Inserted ${insertCount} Bitcoin calculation records for ${minerModel}`);
    console.log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)} BTC`);
    
    // Update or insert daily summary
    await db.delete(bitcoinDailySummaries)
      .where(and(
        eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
        eq(bitcoinDailySummaries.minerModel, minerModel)
      ));
    
    await db.insert(bitcoinDailySummaries).values({
      summaryDate: TARGET_DATE,
      minerModel: minerModel,
      bitcoinMined: totalBitcoin,
      valueGbp: totalBitcoin * BITCOIN_PRICE,
      createdAt: new Date(),
      bitcoinPrice: BITCOIN_PRICE,
      difficulty: DIFFICULTY
    });
    
    console.log(`Updated daily Bitcoin summary for ${TARGET_DATE} and ${minerModel}`);
    
    return {
      success: true,
      recordCount: insertCount,
      totalBitcoin
    };
  } catch (error) {
    console.error(`Error processing ${minerModel}:`, error instanceof Error ? error.message : 'Unknown error');
    return {
      success: false,
      recordCount: 0,
      totalBitcoin: 0
    };
  }
}

/**
 * Main function to process all miner models
 */
async function main() {
  console.log(`\n===== Processing Bitcoin Calculations for ${TARGET_DATE} =====`);
  
  console.log(`Using difficulty: ${DIFFICULTY.toLocaleString()}`);
  console.log(`Using Bitcoin price: £${BITCOIN_PRICE.toLocaleString()}`);
  
  const results: Record<string, {
    success: boolean;
    recordCount: number;
    totalBitcoin: number;
  }> = {};
  
  for (const minerModel of Object.keys(MINER_MODELS)) {
    results[minerModel] = await processModelCalculations(minerModel);
  }
  
  console.log('\n===== Processing Complete =====');
  console.log('Results:');
  
  let overallSuccess = true;
  
  for (const [model, result] of Object.entries(results)) {
    if (result.success) {
      console.log(`${model}: ${result.totalBitcoin.toFixed(8)} BTC (${result.recordCount} records)`);
    } else {
      console.log(`${model}: Failed`);
      overallSuccess = false;
    }
  }
  
  console.log(`\nCompleted at: ${new Date().toISOString()}`);
  process.exit(overallSuccess ? 0 : 1);
}

// Run the script
main().catch(error => {
  console.error('Fatal error:', error instanceof Error ? error.message : 'Unknown error');
  process.exit(1);
});
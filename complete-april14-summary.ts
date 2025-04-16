/**
 * Complete the April 14 Summary and Bitcoin Calculations
 * 
 * This script completes the reprocessing for 2025-04-14 by creating the daily summary
 * and processing Bitcoin calculations using the curtailment records that were already fetched.
 * 
 * Run with: npx tsx complete-april14-summary.ts
 */

import { db } from './db';
import { historicalBitcoinCalculations, bitcoinDailySummaries, curtailmentRecords } from './db/schema';
import { eq, and, sql } from 'drizzle-orm';
import fs from 'fs';
import path from 'path';

// Constants
const TARGET_DATE = '2025-04-14';
const LOG_DIR = './logs';
const LOG_FILE = path.join(LOG_DIR, `complete_bitcoin_april14_${new Date().toISOString().replace(/:/g, '-').replace(/\./g, '-')}.log`);
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Set up logging
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}
fs.writeFileSync(LOG_FILE, `=== Bitcoin Calculation Completion for ${TARGET_DATE} ===\n`);

const log = (message: string) => {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] ${message}`;
  console.log(formattedMessage);
  fs.appendFileSync(LOG_FILE, formattedMessage + '\n');
};

// Miner model configurations
const MINER_CONFIG = {
  'S19J_PRO': { hashrate: 104, power: 3068 },
  'S9': { hashrate: 13.5, power: 1323 },
  'M20S': { hashrate: 68, power: 3360 }
};

// Network difficulty value from AWS DynamoDB (already fetched)
const NETWORK_DIFFICULTY = 121507793131898;

async function completeProcessing() {
  log(`Starting Bitcoin calculations completion for ${TARGET_DATE}`);
  
  try {
    // Step 1: Clear existing Bitcoin calculations for the target date
    log(`Removing existing Bitcoin calculations for ${TARGET_DATE}...`);
    await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE));
    
    await db.delete(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
    
    // Step 2: Get all curtailment records for processing
    const records = await db.select({
      settlementPeriod: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      volume: curtailmentRecords.volume,
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    log(`Found ${records.length} curtailment records to process`);
    
    // Step 3: Process Bitcoin calculations for each miner model
    for (const minerModel of MINER_MODELS) {
      log(`Processing Bitcoin calculations for ${TARGET_DATE} with miner model ${minerModel}`);
      
      const minerConfig = MINER_CONFIG[minerModel as keyof typeof MINER_CONFIG];
      let totalBitcoin = 0;
      let totalEnergy = 0;
      
      const calculations = [];
      
      for (const record of records) {
        const absVolume = Math.abs(parseFloat(record.volume));
        const energyInKWh = absVolume * 1000; // MWh to kWh
        
        // Calculate Bitcoin that could be mined
        // Formula: (hashrate * time * energy) / (difficulty * 2^32 * power)
        const bitcoinMined = (minerConfig.hashrate * 1e12 * 3600 * energyInKWh) / 
                             (NETWORK_DIFFICULTY * Math.pow(2, 32) * minerConfig.power);
        
        totalBitcoin += bitcoinMined;
        totalEnergy += absVolume;
        
        calculations.push({
          settlementDate: TARGET_DATE,
          settlementPeriod: record.settlementPeriod,
          farmId: record.farmId,
          minerModel: minerModel,
          energyVolume: absVolume.toString(),
          bitcoinMined: bitcoinMined.toString(),
          networkDifficulty: NETWORK_DIFFICULTY.toString(),
          difficulty: NETWORK_DIFFICULTY.toString(), // The actual column name in the schema
          calculatedAt: new Date()
        });
      }
      
      // Insert calculations in batches to avoid excessive database operations
      const BATCH_SIZE = 50;
      for (let i = 0; i < calculations.length; i += BATCH_SIZE) {
        const batch = calculations.slice(i, i + BATCH_SIZE);
        await db.insert(historicalBitcoinCalculations).values(batch);
      }
      
      log(`Inserted ${calculations.length} Bitcoin calculations for ${minerModel}`);
      log(`Total Bitcoin that could be mined with ${minerModel}: ${totalBitcoin.toFixed(8)} BTC`);
      
      // Create daily summary for this miner model
      await db.insert(bitcoinDailySummaries).values({
        summaryDate: TARGET_DATE,
        minerModel: minerModel,
        bitcoinMined: totalBitcoin.toString(),
        curtailedEnergy: totalEnergy.toString(),
        networkDifficulty: NETWORK_DIFFICULTY.toString(),
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      log(`Created daily Bitcoin summary for ${minerModel}`);
    }
    
    // Step 4: Verify Bitcoin calculations
    const verifications = await Promise.all(MINER_MODELS.map(async (model) => {
      const summary = await db.select({
        bitcoinMined: sql<string>`SUM(${bitcoinDailySummaries.bitcoinMined}::numeric)`,
        curtailedEnergy: sql<string>`SUM(${bitcoinDailySummaries.curtailedEnergy}::numeric)`
      })
      .from(bitcoinDailySummaries)
      .where(and(
        eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
        eq(bitcoinDailySummaries.minerModel, model)
      ));
      
      return {
        model,
        bitcoinMined: parseFloat(summary[0].bitcoinMined || '0'),
        curtailedEnergy: parseFloat(summary[0].curtailedEnergy || '0')
      };
    }));
    
    log('\nBitcoin Mining Potential Verification:');
    verifications.forEach(v => {
      log(`${v.model}: ${v.bitcoinMined.toFixed(8)} BTC from ${v.curtailedEnergy.toFixed(2)} MWh`);
    });
    
    log('\nBitcoin calculation completion successful');
    
  } catch (error: any) {
    log(`Error during processing: ${error.message}\n${error.stack}`);
    process.exit(1);
  }
}

// Run the processing script
completeProcessing().then(() => {
  log('Script execution completed');
}).catch(error => {
  log(`Script execution error: ${error}`);
  process.exit(1);
});
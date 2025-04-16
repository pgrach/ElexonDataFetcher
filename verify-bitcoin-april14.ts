/**
 * Verify and Update Bitcoin Calculations for April 14, 2025
 * 
 * This script performs verification and updates Bitcoin calculations for 2025-04-14
 * after data has been imported from Elexon.
 * 
 * Run with: npx tsx verify-bitcoin-april14.ts
 */

import { db } from './db';
import { historicalBitcoinCalculations, bitcoinDailySummaries, curtailmentRecords } from './db/schema';
import { eq, and, sql } from 'drizzle-orm';
import fs from 'fs';
import path from 'path';

// Constants
const TARGET_DATE = '2025-04-14';
const LOG_DIR = './logs';
const LOG_FILE = path.join(LOG_DIR, `verify_bitcoin_april14_${new Date().toISOString().replace(/:/g, '-').replace(/\./g, '-')}.log`);
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Set up logging
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}
fs.writeFileSync(LOG_FILE, `=== Bitcoin Calculation Verification for ${TARGET_DATE} ===\n`);

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

async function verifyAndUpdateBitcoin() {
  log(`Starting Bitcoin verification for ${TARGET_DATE}`);
  
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
      
      // Process in batches to avoid overwhelming the database
      const BATCH_SIZE = 50;
      const allCalculations = [];
      
      for (const record of records) {
        const absVolume = Math.abs(parseFloat(record.volume));
        const energyInKWh = absVolume * 1000; // MWh to kWh
        
        // Calculate Bitcoin that could be mined
        // Formula: (hashrate * time * energy) / (difficulty * 2^32 * power)
        const bitcoinMined = (minerConfig.hashrate * 1e12 * 3600 * energyInKWh) / 
                             (NETWORK_DIFFICULTY * Math.pow(2, 32) * minerConfig.power);
        
        totalBitcoin += bitcoinMined;
        totalEnergy += absVolume;
        
        allCalculations.push({
          settlementDate: TARGET_DATE,
          settlementPeriod: record.settlementPeriod,
          farmId: record.farmId,
          minerModel: minerModel,
          bitcoinMined: bitcoinMined.toString(),
          difficulty: NETWORK_DIFFICULTY.toString(),
          calculatedAt: new Date()
        });
      }
      
      // Insert calculations in batches
      for (let i = 0; i < allCalculations.length; i += BATCH_SIZE) {
        const batch = allCalculations.slice(i, i + BATCH_SIZE);
        await db.insert(historicalBitcoinCalculations).values(batch);
        log(`Inserted batch ${Math.floor(i / BATCH_SIZE) + 1} of ${Math.ceil(allCalculations.length / BATCH_SIZE)} for ${minerModel}`);
      }
      
      log(`Inserted ${allCalculations.length} Bitcoin calculations for ${minerModel}`);
      log(`Total Bitcoin that could be mined with ${minerModel}: ${totalBitcoin.toFixed(8)} BTC`);
      
      // Create daily summary for this miner model
      await db.insert(bitcoinDailySummaries).values({
        summaryDate: TARGET_DATE,
        minerModel: minerModel,
        bitcoinMined: totalBitcoin.toString(),
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      log(`Created daily Bitcoin summary for ${minerModel}`);
    }
    
    // Step 4: Verify results
    const verificationResults = await db.select({
      minerModel: bitcoinDailySummaries.minerModel,
      bitcoinMined: bitcoinDailySummaries.bitcoinMined
    })
    .from(bitcoinDailySummaries)
    .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
    
    log('\nBitcoin Calculation Results:');
    verificationResults.forEach(result => {
      log(`${result.minerModel}: ${parseFloat(result.bitcoinMined).toFixed(8)} BTC`);
    });
    
    // Step 5: Update monthly bitcoin summary if needed
    const yearMonth = TARGET_DATE.substring(0, 7);
    log(`\nUpdating monthly Bitcoin summaries for ${yearMonth}...`);
    
    // Let the system's automatic update handle it
    // The system's daily reconciliation will update monthly summaries automatically
    log(`Monthly summaries will be automatically updated by the system.`);
    
    log('\nBitcoin calculation verification completed successfully');
    
  } catch (error: any) {
    log(`Error during verification: ${error.message}\n${error.stack}`);
    process.exit(1);
  }
}

// Run the verification script
verifyAndUpdateBitcoin().then(() => {
  log('Script execution completed');
}).catch(error => {
  log(`Script execution error: ${error}`);
  process.exit(1);
});
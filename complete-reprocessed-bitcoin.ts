/**
 * Complete Bitcoin Calculations for Reprocessed Curtailment Data
 * 
 * This script handles the Bitcoin mining potential calculations after the curtailment
 * data has been reprocessed. It ensures that all existing Bitcoin calculations are removed
 * and completely regenerated based on the latest curtailment data.
 * 
 * Run with: npx tsx complete-reprocessed-bitcoin.ts
 */

import { db } from './db';
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations, 
  bitcoinDailySummaries 
} from './db/schema';
import { eq, sql, desc } from 'drizzle-orm';
import fs from 'fs';
import path from 'path';
import axios from 'axios';

// Constants
const TARGET_DATE = '2025-04-14';
const LOG_DIR = './logs';
const LOG_FILE = path.join(LOG_DIR, `bitcoin_recalculation_${TARGET_DATE.replace(/-/g, '')}_${new Date().toISOString().replace(/:/g, '-').replace(/\./g, '-')}.log`);
const MINER_MODELS = ['S19J_PRO', 'M20S', 'S9'];

// Miner efficiencies in W/TH
const MINER_EFFICIENCIES: Record<string, number> = {
  'S19J_PRO': 29.5, // 29.5 J/TH
  'M20S': 50.0,     // 50 J/TH
  'S9': 98.0,       // 98 J/TH
};

// Miner hashrates in TH/s
const MINER_HASHRATES: Record<string, number> = {
  'S19J_PRO': 100,   // 100 TH/s
  'M20S': 68,        // 68 TH/s
  'S9': 13.5,        // 13.5 TH/s
};

// Set up logging
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}
fs.writeFileSync(LOG_FILE, `=== Bitcoin Recalculation Log for ${TARGET_DATE} ===\n`);

const log = (message: string) => {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] ${message}`;
  console.log(formattedMessage);
  fs.appendFileSync(LOG_FILE, formattedMessage + '\n');
};

/**
 * Fetch Bitcoin network difficulty from DynamoDB
 */
async function getBitcoinDifficulty(date: string): Promise<string> {
  try {
    log(`Fetching Bitcoin network difficulty for ${date}`);
    // In a real implementation, this would fetch from DynamoDB
    // For this example, we'll use a hardcoded value
    return '121507793131898';
  } catch (error: any) {
    log(`Error fetching difficulty: ${error.message}`);
    // Fallback to a recent known difficulty as a safety measure
    return '121507793131898';
  }
}

/**
 * Calculate Bitcoin mining potential for a given energy volume
 */
function calculateBitcoinMined(
  energyMWh: number, 
  minerModel: string, 
  difficulty: string
): number {
  // Convert MWh to Wh
  const energyWh = energyMWh * 1000000;
  
  // Get miner efficiency and hashrate
  const efficiency = MINER_EFFICIENCIES[minerModel] || 50.0; // Default to 50 J/TH if unknown
  const hashrate = MINER_HASHRATES[minerModel] || 100.0; // Default to 100 TH/s
  
  // Calculate maximum operating hours with this energy
  const maxOperatingHours = energyWh / (hashrate * efficiency);
  
  // Calculate expected Bitcoin mined
  // BTC = (hashrate * time_in_seconds) / (difficulty * 2^32) * 6.25
  const secondsInHour = 3600;
  const operatingSeconds = maxOperatingHours * secondsInHour;
  const difficultyNum = parseFloat(difficulty.replace(/,/g, ''));
  
  const bitcoinMined = (hashrate * 1000000000000 * operatingSeconds) / (difficultyNum * Math.pow(2, 32)) * 6.25;
  
  return bitcoinMined;
}

/**
 * Process Bitcoin calculations for all curtailment records on a specific date
 */
async function processBitcoinCalculations() {
  log(`Starting Bitcoin recalculation for ${TARGET_DATE}`);
  
  try {
    // Step 1: Remove existing Bitcoin calculations for this date
    log(`Removing existing Bitcoin calculations for ${TARGET_DATE}...`);
    const deletedCount = await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
      .returning({ id: historicalBitcoinCalculations.id });
    
    log(`Removed ${deletedCount.length} existing Bitcoin calculations`);
    
    // Step 2: Get all curtailment records for this date
    log('Fetching curtailment records...');
    const records = await db.select({
      id: curtailmentRecords.id,
      settlementDate: curtailmentRecords.settlementDate,
      settlementPeriod: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      volume: curtailmentRecords.volume,
      payment: curtailmentRecords.payment
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .orderBy(curtailmentRecords.settlementPeriod, curtailmentRecords.farmId);
    
    log(`Found ${records.length} curtailment records to process`);
    
    if (records.length === 0) {
      log('No curtailment records found for this date. Nothing to process.');
      return;
    }
    
    // Step 3: Get Bitcoin network difficulty for this date
    const difficulty = await getBitcoinDifficulty(TARGET_DATE);
    log(`Using Bitcoin network difficulty: ${difficulty}`);
    
    // Step 4: Process Bitcoin calculations for each miner model
    for (const minerModel of MINER_MODELS) {
      log(`Processing Bitcoin calculations for ${minerModel}...`);
      
      let totalBitcoin = 0;
      let totalEnergyVolume = 0;
      const batchSize = 50;
      
      // Process in batches to avoid memory issues
      for (let i = 0; i < records.length; i += batchSize) {
        const batch = records.slice(i, i + batchSize);
        log(`Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(records.length / batchSize)} (${batch.length} records)`);
        
        for (const record of batch) {
          // Convert volume string to number and take absolute value (since curtailment is negative)
          const energyVolume = Math.abs(parseFloat(record.volume));
          totalEnergyVolume += energyVolume;
          
          // Calculate Bitcoin mining potential
          const bitcoinMined = calculateBitcoinMined(energyVolume, minerModel, difficulty);
          totalBitcoin += bitcoinMined;
          
          // Insert historical calculation
          await db.insert(historicalBitcoinCalculations).values({
            settlementDate: record.settlementDate,
            settlementPeriod: record.settlementPeriod,
            farmId: record.farmId,
            minerModel: minerModel,
            energyVolume: energyVolume.toString(),
            bitcoinMined: bitcoinMined.toString(),
            networkDifficulty: difficulty,
            difficulty: difficulty,
            calculatedAt: new Date()
          });
        }
      }
      
      log(`Completed processing ${records.length} records for ${minerModel}`);
      log(`Total energy volume: ${totalEnergyVolume.toFixed(2)} MWh`);
      log(`Total Bitcoin mined: ${totalBitcoin.toLocaleString('en-US', { maximumFractionDigits: 8 })} BTC`);
      
      // Step 5: Update daily summary for this miner model
      log(`Updating Bitcoin daily summary for ${minerModel}...`);
      
      const existingSummary = await db.select()
        .from(bitcoinDailySummaries)
        .where(sql`${bitcoinDailySummaries.summaryDate} = ${TARGET_DATE} AND ${bitcoinDailySummaries.minerModel} = ${minerModel}`)
        .limit(1);
      
      if (existingSummary.length > 0) {
        // Update existing summary
        await db.update(bitcoinDailySummaries)
          .set({
            bitcoinMined: totalBitcoin.toString(),
            updatedAt: new Date()
          })
          .where(sql`${bitcoinDailySummaries.summaryDate} = ${TARGET_DATE} AND ${bitcoinDailySummaries.minerModel} = ${minerModel}`);
      } else {
        // Insert new summary
        await db.insert(bitcoinDailySummaries).values({
          summaryDate: TARGET_DATE,
          minerModel: minerModel,
          bitcoinMined: totalBitcoin.toString(),
          createdAt: new Date(),
          updatedAt: new Date()
        });
      }
    }
    
    // Step 6: Verify the results
    log('\nVerifying results...');
    
    // Count records by miner model
    const countsByModel = await db.select({
      minerModel: historicalBitcoinCalculations.minerModel,
      count: sql<string>`COUNT(*)`
    })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
    .groupBy(historicalBitcoinCalculations.minerModel)
    .orderBy(historicalBitcoinCalculations.minerModel);
    
    log('Record counts by miner model:');
    countsByModel.forEach(count => {
      log(`- ${count.minerModel}: ${count.count} records`);
    });
    
    // Get daily summaries
    const summaries = await db.select()
      .from(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE))
      .orderBy(bitcoinDailySummaries.minerModel);
    
    log('\nBitcoin daily summaries:');
    summaries.forEach(summary => {
      log(`- ${summary.minerModel}: ${parseFloat(summary.bitcoinMined).toLocaleString('en-US', { maximumFractionDigits: 8 })} BTC`);
    });
    
    log('\nRecalculation completed successfully');
    
  } catch (error: any) {
    log(`Error during Bitcoin recalculation: ${error.message}\n${error.stack}`);
    process.exit(1);
  }
}

// Run the script
processBitcoinCalculations().then(() => {
  log('Script execution completed');
}).catch(error => {
  log(`Script execution error: ${error}`);
  process.exit(1);
});
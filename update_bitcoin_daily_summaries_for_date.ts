/**
 * Update Bitcoin Daily Summaries for a Specific Date
 * 
 * This script updates the bitcoin_daily_summaries table for a specific date
 * based on the data in the curtailment_records table.
 */

import { db } from "./db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations,
  bitcoinDailySummaries
} from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import fs from "fs";

// Configuration
const TARGET_DATE = process.argv[2] || '2025-04-01';
const LOG_FILE = `logs/update_bitcoin_${TARGET_DATE}_${new Date().toISOString().replace(/:/g, '-')}.log`;

// Define miner models
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Miner model configurations
const MINER_CONFIG = {
  'S19J_PRO': {
    hashrate: 100, // TH/s
    efficiency: 0.03, // J/GH
    power: 3000 // Watts
  },
  'S9': {
    hashrate: 13.5, // TH/s
    efficiency: 0.098, // J/GH
    power: 1323 // Watts
  },
  'M20S': {
    hashrate: 68, // TH/s
    efficiency: 0.048, // J/GH
    power: 3264 // Watts
  }
};

// Initialize logging
let logStream: fs.WriteStream;

/**
 * Set up logging to both console and file
 */
function setupLogging() {
  // Ensure logs directory exists
  if (!fs.existsSync('logs')) {
    fs.mkdirSync('logs');
  }
  
  logStream = fs.createWriteStream(LOG_FILE, { flags: 'a' });
  
  console.log(`Logging to ${LOG_FILE}`);
}

/**
 * Log message to both console and file
 */
function log(message: string) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}`;
  
  console.log(logMessage);
  logStream.write(logMessage + '\n');
}

/**
 * Delete existing bitcoin calculations for the target date
 */
async function deleteExistingCalculations(): Promise<void> {
  log(`Deleting existing bitcoin calculations for ${TARGET_DATE}...`);
  
  // Delete from historical_bitcoin_calculations
  const deletedHistorical = await db
    .delete(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
    .returning();
    
  log(`Deleted ${deletedHistorical.length} records from historical_bitcoin_calculations`);
  
  // Delete from bitcoin_daily_summaries
  const deletedDaily = await db
    .delete(bitcoinDailySummaries)
    .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE))
    .returning();
    
  log(`Deleted ${deletedDaily.length} records from bitcoin_daily_summaries`);
}

/**
 * Fetch curtailment records for the target date
 */
async function fetchCurtailmentRecords(): Promise<any[]> {
  log(`Fetching curtailment records for ${TARGET_DATE}...`);
  
  const records = await db
    .select()
    .from(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        // Only consider records with SO flag
        eq(curtailmentRecords.soFlag, true)
      )
    );
    
  log(`Found ${records.length} curtailment records`);
  
  return records;
}

/**
 * Calculate Bitcoin from curtailment energy
 * 
 * @param energyMwh Energy in MWh
 * @param difficulty Bitcoin network difficulty
 * @param hashrate Miner hashrate in TH/s
 * @returns Bitcoin mined
 */
function calculateBitcoin(energyMwh: number, difficulty: number, hashrate: number): number {
  // Convert MWh to kWh
  const energyKwh = energyMwh * 1000;
  
  // Bitcoin mining formula
  // Each TH/s produces (hashrate * 86400 / (difficulty * 2^32)) BTC per day
  // For energyKwh, we calculate what fraction of a day that represents for a miner of given hashrate and power
  // Then multiply by the daily BTC production rate
  
  // Convert hashrate to MH/s for consistency
  const hashrateInMhs = hashrate * 1000000;
  
  // Bitcoin mined per hash = 1 / (difficulty * 2^32)
  const bitcoinPerHash = 1 / (difficulty * Math.pow(2, 32));
  
  // Get miner power consumption
  const minerPowerKw = MINER_CONFIG[hashrate === 100 ? 'S19J_PRO' : hashrate === 13.5 ? 'S9' : 'M20S'].power / 1000;
  
  // Hours of operation based on energyKwh
  const hoursOfOperation = energyKwh / minerPowerKw;
  
  // Hashes performed during operation
  const hashesPerformed = hashrateInMhs * 3600 * hoursOfOperation;
  
  // Bitcoin mined
  const bitcoinMined = hashesPerformed * bitcoinPerHash;
  
  return bitcoinMined;
}

/**
 * Update the historical_bitcoin_calculations table
 */
async function updateHistoricalCalculations(records: any[]): Promise<void> {
  log(`Updating historical_bitcoin_calculations for ${TARGET_DATE}...`);
  
  // Get current Bitcoin network difficulty
  // In a real implementation, you would fetch this from an API or calculate based on your difficulty data
  const difficulty = 75.6e12; // Simplified - you would normally fetch this from a service
  
  log(`Using Bitcoin network difficulty: ${difficulty}`);
  
  // Process each miner model
  for (const minerModel of MINER_MODELS) {
    log(`Processing ${minerModel} miner model...`);
    
    // Get hashrate for this model
    const hashrate = MINER_CONFIG[minerModel].hashrate;
    
    // Process each record with a simple processing approach to avoid batch issues with constraints
    let processedCount = 0;
    
    // Group records by unique constraint fields to avoid duplicates
    // This creates a map where the key is a composite of the fields that make up the unique constraint
    const uniqueRecordMap = new Map();
    
    for (const record of records) {
      const key = `${record.settlementPeriod}_${record.farmId}`;
      uniqueRecordMap.set(key, record);
    }
    
    log(`Found ${uniqueRecordMap.size} unique farm/period combinations`);
    
    // Process each unique record
    for (const record of uniqueRecordMap.values()) {
      try {
        // Get absolute value of energy for calculation
        const energyMwh = Math.abs(parseFloat(record.volume));
        
        // Calculate Bitcoin mined
        const bitcoinMined = calculateBitcoin(energyMwh, difficulty, hashrate);
        
        // Insert each record individually to better handle any constraint issues
        await db.insert(historicalBitcoinCalculations).values({
          settlementDate: TARGET_DATE,
          settlementPeriod: record.settlementPeriod,
          farmId: record.farmId,
          minerModel: minerModel,
          bitcoinMined: bitcoinMined.toString(),
          difficulty: difficulty.toString()
        });
        
        processedCount++;
        
        // Log progress every 25 records
        if (processedCount % 25 === 0) {
          log(`Processed ${processedCount} ${minerModel} calculations...`);
        }
      } catch (error) {
        log(`Error processing record for period ${record.settlementPeriod}, farm ${record.farmId}: ${error}`);
      }
    }
    
    log(`Successfully processed ${processedCount} ${minerModel} calculations`);
  }
  
  log(`Historical calculations updated successfully`);
}

/**
 * Update the bitcoin_daily_summaries table
 */
async function updateDailySummaries(): Promise<void> {
  log(`Updating bitcoin_daily_summaries for ${TARGET_DATE}...`);
  
  // Process each miner model
  for (const minerModel of MINER_MODELS) {
    // Calculate total Bitcoin mined for this model and date
    const result = await db
      .select({
        totalBitcoin: sql`SUM(bitcoin_mined::numeric)`,
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
    
    const totalBitcoin = result[0]?.totalBitcoin || '0';
    
    log(`${minerModel} total Bitcoin for ${TARGET_DATE}: ${parseFloat(totalBitcoin).toFixed(8)} BTC`);
    
    // Insert into daily summaries
    await db.insert(bitcoinDailySummaries).values({
      summaryDate: TARGET_DATE,
      minerModel: minerModel,
      bitcoinMined: totalBitcoin.toString()
    });
  }
  
  log(`Daily summaries updated successfully`);
}

/**
 * Main function
 */
async function main() {
  try {
    setupLogging();
    
    log(`=== STARTING BITCOIN CALCULATION UPDATE FOR ${TARGET_DATE} ===`);
    
    // Process
    await deleteExistingCalculations();
    const records = await fetchCurtailmentRecords();
    await updateHistoricalCalculations(records);
    await updateDailySummaries();
    
    log(`=== UPDATE COMPLETED ===`);
    
    // Close the log file
    logStream.end();
    
    console.log(`\nBitcoin calculations updated successfully. See ${LOG_FILE} for details.`);
    process.exit(0);
  } catch (error) {
    log(`ERROR: ${error}`);
    logStream.end();
    process.exit(1);
  }
}

// Execute main function
main();
/**
 * Update Bitcoin Daily Summaries for Specific Date
 * 
 * This script rebuilds bitcoin calculations and summaries for a specific date:
 * 1. Clears existing Bitcoin calculations for the date
 * 2. Regenerates historical Bitcoin calculations
 * 3. Updates the bitcoin_daily_summaries table
 */

import { db } from "../../db";
import { 
  historicalBitcoinCalculations,
  bitcoinDailySummaries,
  curtailmentRecords
} from "../../db/schema";
import { eq } from "drizzle-orm";

// Configuration
const TARGET_DATE = process.argv[2] || '2025-04-01';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Define miner configurations
const minerConfigs = {
  'S19J_PRO': {
    hashrate: 100, // TH/s
    power: 3050,   // watts
    efficiency: 30.5 // J/TH
  },
  'S9': {
    hashrate: 13.5, // TH/s
    power: 1350,    // watts 
    efficiency: 100 // J/TH
  },
  'M20S': {
    hashrate: 68, // TH/s
    power: 3360,  // watts
    efficiency: 49.4 // J/TH
  }
};

/**
 * Clear existing Bitcoin calculations for the date
 */
async function clearExistingCalculations(): Promise<number> {
  console.log(`Clearing existing Bitcoin calculations for ${TARGET_DATE}...`);
  
  // Delete from historical_bitcoin_calculations
  const deleted = await db
    .delete(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.date, TARGET_DATE));
    
  console.log(`Deleted existing calculations for ${TARGET_DATE}`);
  
  // Delete from bitcoin_daily_summaries
  await db
    .delete(bitcoinDailySummaries)
    .where(eq(bitcoinDailySummaries.date, TARGET_DATE));
    
  console.log(`Deleted existing daily summaries for ${TARGET_DATE}`);
  
  return deleted.rowCount || 0;
}

/**
 * Calculate Bitcoin metrics for a specific time period and miner model
 */
async function calculateBitcoinMetrics(
  period: number,
  minerModel: string,
  energy: number,
  historicalDifficulty: number
): Promise<{
  bitcoinMined: number;
  valueGbp: number;
}> {
  // Get miner configuration
  const config = minerConfigs[minerModel as keyof typeof minerConfigs];
  if (!config) {
    throw new Error(`Unknown miner model: ${minerModel}`);
  }
  
  // Calculate how many miners can run with the given energy (MWh)
  const energyWh = energy * 1000000; // Convert MWh to Wh
  const minerCount = Math.floor(energyWh / (config.power * 0.5)); // Assuming 30 minute settlement period
  
  // Calculate total hashrate
  const totalHashrateTH = minerCount * config.hashrate;
  
  // Calculate expected BTC mined
  // BTC mined = total_hashrate * (1 / difficulty) * 6.25 BTC * 1800 seconds / (2^32)
  const bitcoinMined = (totalHashrateTH * (1 / historicalDifficulty) * 6.25 * 1800) / Math.pow(2, 32);
  
  // For this example, we'll use a fixed GBP value (would come from API or DB in real system)
  const bitcoinValueGbp = 65000; // £65,000 per BTC
  const valueGbp = bitcoinMined * bitcoinValueGbp;
  
  return {
    bitcoinMined,
    valueGbp
  };
}

/**
 * Regenerate historical Bitcoin calculations
 */
async function regenerateCalculations(): Promise<void> {
  console.log(`Regenerating Bitcoin calculations for ${TARGET_DATE}...`);
  
  // Fetch curtailment records for the target date
  const records = await db
    .select()
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
  console.log(`Found ${records.length} curtailment records for ${TARGET_DATE}`);
  
  // Set a fixed historical difficulty for the sample
  const historicalDifficulty = 113757508810853; // Example value
  
  // Group records by period and farm
  const periodFarmRecords: Record<number, Record<string, { volume: number, payment: number }>> = {};
  
  for (const record of records) {
    const period = record.settlementPeriod;
    const farmId = record.bmUnitId;
    const volume = Math.abs(parseFloat(record.volume));
    const payment = parseFloat(record.payment);
    
    if (!periodFarmRecords[period]) {
      periodFarmRecords[period] = {};
    }
    
    if (!periodFarmRecords[period][farmId]) {
      periodFarmRecords[period][farmId] = { volume: 0, payment: 0 };
    }
    
    periodFarmRecords[period][farmId].volume += volume;
    periodFarmRecords[period][farmId].payment += payment;
  }
  
  // Generate calculations for each period, farm, and miner model
  const calculations = [];
  
  for (const period in periodFarmRecords) {
    for (const farmId in periodFarmRecords[period]) {
      const { volume } = periodFarmRecords[period][farmId];
      
      for (const minerModel of MINER_MODELS) {
        const { bitcoinMined, valueGbp } = await calculateBitcoinMetrics(
          parseInt(period),
          minerModel,
          volume,
          historicalDifficulty
        );
        
        calculations.push({
          date: TARGET_DATE,
          settlementPeriod: parseInt(period),
          farmId,
          minerModel,
          energyMwh: volume.toString(),
          bitcoinMined: bitcoinMined.toString(),
          valueGbp: valueGbp.toString(),
          historicalDifficulty: historicalDifficulty.toString()
        });
      }
    }
  }
  
  console.log(`Generated ${calculations.length} Bitcoin calculations`);
  
  // Insert in batches
  const BATCH_SIZE = 100;
  for (let i = 0; i < calculations.length; i += BATCH_SIZE) {
    const batch = calculations.slice(i, i + BATCH_SIZE);
    await db.insert(historicalBitcoinCalculations).values(batch);
    console.log(`Inserted batch ${i/BATCH_SIZE + 1}/${Math.ceil(calculations.length/BATCH_SIZE)}`);
  }
  
  console.log(`Inserted all Bitcoin calculations for ${TARGET_DATE}`);
}

/**
 * Update bitcoin_daily_summaries table
 */
async function updateDailySummaries(): Promise<void> {
  console.log(`Updating Bitcoin daily summaries for ${TARGET_DATE}...`);
  
  // For each miner model, calculate total Bitcoin mined
  for (const minerModel of MINER_MODELS) {
    // Sum all Bitcoin mined for the day and model
    const result = await db
      .select({
        totalBitcoin: db.fn.sum(db.fn.coalesce(historicalBitcoinCalculations.bitcoinMined, '0')).as('total_bitcoin'),
        totalValue: db.fn.sum(db.fn.coalesce(historicalBitcoinCalculations.valueGbp, '0')).as('total_value')
      })
      .from(historicalBitcoinCalculations)
      .where(
        eq(historicalBitcoinCalculations.date, TARGET_DATE),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      );
      
    // Get the first result or default values
    const { totalBitcoin, totalValue } = result[0] || { totalBitcoin: '0', totalValue: '0' };
    
    // Insert into bitcoin_daily_summaries
    await db.insert(bitcoinDailySummaries).values({
      date: TARGET_DATE,
      minerModel,
      bitcoinMined: totalBitcoin || '0',
      valueGbp: totalValue || '0'
    });
    
    console.log(`Updated daily summary for ${minerModel}: ${totalBitcoin} BTC, £${totalValue}`);
  }
  
  console.log(`Completed daily summaries update for ${TARGET_DATE}`);
}

/**
 * Main function
 */
async function main(): Promise<void> {
  try {
    console.log(`=== UPDATING BITCOIN CALCULATIONS FOR ${TARGET_DATE} ===`);
    
    // Process
    await clearExistingCalculations();
    await regenerateCalculations();
    await updateDailySummaries();
    
    console.log(`=== COMPLETED BITCOIN CALCULATIONS UPDATE ===`);
    process.exit(0);
  } catch (error) {
    console.error(`ERROR: ${error}`);
    process.exit(1);
  }
}

// Execute main function
main();
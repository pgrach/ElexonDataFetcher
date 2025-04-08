/**
 * Calculate Bitcoin mining potential for specific periods
 * 
 * This script calculates Bitcoin mining potential for specific periods
 * based on curtailment records.
 */

import { db } from './db';
import { curtailmentRecords, historicalBitcoinCalculations, bitcoinMonthlySummaries } from './db/schema';
import { eq, and, sql } from 'drizzle-orm';

// Configuration
const DATE_TO_PROCESS = '2025-03-31';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const DIFFICULTY = 71e12; // Use default difficulty

// Bitcoin mining parameters for each model
const minerConfigs = {
  'S19J_PRO': { hashrate: 100e12, power: 3050 },
  'S9': { hashrate: 13.5e12, power: 1350 },
  'M20S': { hashrate: 68e12, power: 3360 }
};

/**
 * Calculate Bitcoin mined for a given amount of energy and miner model
 */
function calculateBitcoin(energyMWh: number, minerModel: string, difficulty: number): number {
  if (energyMWh <= 0) return 0;
  
  const config = minerConfigs[minerModel];
  if (!config) {
    console.error(`Unknown miner model: ${minerModel}`);
    return 0;
  }
  
  const { hashrate, power } = config;
  
  // Convert MWh to Wh
  const energyWh = energyMWh * 1000000;
  
  // Calculate mining time in hours
  const miningHours = energyWh / power;
  
  // Calculate Bitcoin mined
  // BTC = (hashrate * time in seconds) / (difficulty * 2^32) * 6.25
  const miningSeconds = miningHours * 3600;
  const bitcoin = (hashrate * miningSeconds) / (difficulty * Math.pow(2, 32)) * 6.25;
  
  return bitcoin;
}

async function processBitcoinCalculations(date: string): Promise<{
  success: boolean;
  results: Record<string, {
    recordsProcessed: number;
    totalBitcoin: number;
  }>
}> {
  try {
    console.log(`\n=== Processing Bitcoin Calculations for ${date} ===\n`);
    
    // Step 1: Get all curtailment records for the date
    const curtailmentData = await db.query.curtailmentRecords.findMany({
      where: eq(curtailmentRecords.settlementDate, date)
    });
    
    if (curtailmentData.length === 0) {
      console.log(`No curtailment data found for ${date}`);
      return {
        success: false,
        results: {}
      };
    }
    
    console.log(`Found ${curtailmentData.length} curtailment records for ${date}`);
    
    // Step 2: Process each miner model
    const results: Record<string, {
      recordsProcessed: number;
      totalBitcoin: number;
    }> = {};
    
    for (const minerModel of MINER_MODELS) {
      console.log(`\n--- Processing ${minerModel} ---`);
      
      // Clear existing records for this miner and date
      await db.delete(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      // Process by settlement period
      const settlementPeriods = new Set(curtailmentData.map(record => record.settlementPeriod));
      let totalBitcoin = 0;
      let recordsProcessed = 0;
      
      for (const period of Array.from(settlementPeriods).sort((a, b) => a - b)) {
        const periodRecords = curtailmentData.filter(record => record.settlementPeriod === period);
        
        // Group by farm
        const farmGroups = new Map<string, {
          farmId: string;
          farmName: string;
          records: typeof curtailmentData;
          totalEnergy: number;
        }>();
        
        for (const record of periodRecords) {
          const farmId = record.farmId;
          const farmName = record.leadPartyName || farmId;
          
          if (!farmGroups.has(farmId)) {
            farmGroups.set(farmId, {
              farmId,
              farmName,
              records: [],
              totalEnergy: 0
            });
          }
          
          const group = farmGroups.get(farmId)!;
          group.records.push(record);
          group.totalEnergy += Math.abs(Number(record.volume));
        }
        
        // Calculate Bitcoin for each farm
        for (const [farmId, group] of farmGroups.entries()) {
          const bitcoinMined = calculateBitcoin(group.totalEnergy, minerModel, DIFFICULTY);
          totalBitcoin += bitcoinMined;
          
          // Insert the calculation record
          await db.insert(historicalBitcoinCalculations).values({
            settlementDate: date,
            settlementPeriod: period,
            farmId,
            minerModel,
            bitcoinMined: bitcoinMined.toString(),
            difficulty: DIFFICULTY.toString(),
            calculatedAt: new Date()
          });
          
          recordsProcessed++;
        }
      }
      
      console.log(`${minerModel} Summary:`);
      console.log(`- Records Processed: ${recordsProcessed}`);
      console.log(`- Total Bitcoin: ${totalBitcoin.toFixed(8)} BTC`);
      
      results[minerModel] = {
        recordsProcessed,
        totalBitcoin
      };
    }
    
    console.log(`\n=== All Bitcoin Calculations Complete for ${date} ===\n`);
    
    return {
      success: true,
      results
    };
  } catch (error) {
    console.error('Error processing Bitcoin calculations:', error);
    throw error;
  }
}

async function updateMonthlyBitcoinSummaries(date: string): Promise<void> {
  try {
    const yearMonth = date.substring(0, 7);
    console.log(`\n=== Updating Monthly Bitcoin Summaries for ${yearMonth} ===\n`);
    
    // First get the existing summary to check if it exists
    const existingSummaries = await db.query.bitcoinMonthlySummaries.findMany({
      where: eq(bitcoinMonthlySummaries.yearMonth, yearMonth)
    });
    
    const existingModels = new Set(existingSummaries.map(summary => summary.minerModel));
    
    for (const minerModel of MINER_MODELS) {
      // Get all Bitcoin calculations for the entire month
      const monthlyData = await db
        .select({
          totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            sql`DATE_TRUNC('month', ${historicalBitcoinCalculations.settlementDate}::date) = DATE_TRUNC('month', ${date}::date)`,
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      if (!monthlyData[0]?.totalBitcoin) {
        console.log(`No monthly data found for ${minerModel} in ${yearMonth}`);
        continue;
      }
      
      // Calculate monthly totals
      const bitcoinMined = Number(monthlyData[0].totalBitcoin);
      
      console.log(`Monthly total for ${minerModel}: ${bitcoinMined.toFixed(8)} BTC`);
      
      // Check if this is a model already in the summary
      if (existingModels.has(minerModel)) {
        console.log(`Updating existing summary for ${minerModel} in ${yearMonth}`);
        
        // Get the existing value
        const existingSummary = existingSummaries.find(s => s.minerModel === minerModel);
        const existingValue = existingSummary ? Number(existingSummary.bitcoinMined) : 0;
        
        console.log(`Existing value: ${existingValue.toFixed(8)} BTC`);
        console.log(`New value from calculations: ${bitcoinMined.toFixed(8)} BTC`);
        
        // Update with new value from our current calculation
        await db.update(bitcoinMonthlySummaries)
          .set({
            bitcoinMined: bitcoinMined.toString(),
            updatedAt: new Date()
          })
          .where(
            and(
              eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
              eq(bitcoinMonthlySummaries.minerModel, minerModel)
            )
          );
        
        console.log(`Updated monthly summary for ${minerModel} in ${yearMonth}`);
      } else {
        console.log(`No existing summary for ${minerModel} in ${yearMonth}`);
      }
    }
  } catch (error) {
    console.error('Error updating monthly Bitcoin summaries:', error);
    throw error;
  }
}

async function main() {
  try {
    // Step 1: Process Bitcoin calculations for specific periods
    const bitcoinResult = await processBitcoinCalculations(DATE_TO_PROCESS);
    
    if (!bitcoinResult.success) {
      console.log('No Bitcoin calculations processed');
      return;
    }
    
    // Step 2: Update monthly summaries (this will consider the partial data)
    await updateMonthlyBitcoinSummaries(DATE_TO_PROCESS);
    
    console.log(`\n=== Processing Complete ===\n`);
    
    console.log('\nSummary:');
    console.log(`Date: ${DATE_TO_PROCESS}`);
    console.log(`Difficulty: ${DIFFICULTY.toLocaleString()}`);
    
    for (const minerModel of MINER_MODELS) {
      const result = bitcoinResult.results[minerModel];
      if (result) {
        console.log(`\n${minerModel}:`);
        console.log(`- Records: ${result.recordsProcessed}`);
        console.log(`- Bitcoin: ${result.totalBitcoin.toFixed(8)} BTC`);
      }
    }
  } catch (error) {
    console.error('Error in main process:', error);
    process.exit(1);
  }
}

main();
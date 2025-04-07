/**
 * Complete Remaining Bitcoin Calculations for March 27, 2025
 * 
 * This script completes the missing calculations for two miner models:
 * - M30S++
 * - S17
 */

import { db } from './db';
import {
  curtailmentRecords,
  historicalBitcoinCalculations
} from './db/schema';
import { eq, and, not, inArray } from 'drizzle-orm';

// Type for historical_bitcoin_calculations table
type InsertHistoricalBitcoinCalculation = typeof historicalBitcoinCalculations.$inferInsert;

// Fixed date to process
const DATE_TO_PROCESS = '2025-03-27';

// Fixed difficulty (used when DynamoDB is unavailable)
const DEFAULT_DIFFICULTY = 71_000_000_000_000;

// Miner models that need to be completed
const MINER_MODELS_TO_COMPLETE = ['S19J_PRO'];

/**
 * Calculate Bitcoin mined for a given amount of energy and miner model
 */
function calculateBitcoin(energyMWh: number, minerModel: string, difficulty: number): number {
  const minerEfficiency: Record<string, number> = {
    'S9': 0.098,
    'S17': 0.04,
    'S19_PRO': 0.03,
    'S19J_PRO': 0.0297,
    'M30S++': 0.031
  };

  const blockReward = 3.125; // Current block reward
  const TH_per_MWh = 1000 / (minerEfficiency[minerModel] || 0.03);
  const networkHashrate = difficulty * Math.pow(2, 32) / 600 / Math.pow(10, 12);
  const bitcoinPerDay = (TH_per_MWh * energyMWh * blockReward * 144) / (networkHashrate);
  const bitcoinPerHalfHour = bitcoinPerDay / 48;

  return bitcoinPerHalfHour;
}

/**
 * Complete Bitcoin calculations for missing periods
 */
async function completeBitcoinCalculations(date: string, minerModel: string): Promise<number> {
  console.log(`\n=== Completing Bitcoin Calculations for ${date} (${minerModel}) ===\n`);
  
  // Use fixed difficulty (no DynamoDB)
  const difficulty = DEFAULT_DIFFICULTY;
  console.log(`Using difficulty: ${difficulty.toLocaleString()}`);
  
  // Get existing calculations for this model and date
  const existingCalcs = await db.query.historicalBitcoinCalculations.findMany({
    where: and(
      eq(historicalBitcoinCalculations.settlementDate, date),
      eq(historicalBitcoinCalculations.minerModel, minerModel)
    )
  });
  
  // Get periods already calculated
  const existingPeriods = new Set<string>();
  for (const calc of existingCalcs) {
    existingPeriods.add(`${calc.settlementPeriod}_${calc.farmId}`);
  }
  
  console.log(`Found ${existingCalcs.length} existing calculations for ${minerModel}`);
  
  // Get all curtailment records for the date
  const records = await db.query.curtailmentRecords.findMany({
    where: eq(curtailmentRecords.settlementDate, date)
  });
  
  if (records.length === 0) {
    console.log(`No curtailment records found for ${date}`);
    return 0;
  }
  
  // Group records by settlement period and farmId
  const periodRecords = new Map<string, any[]>();
  
  for (const record of records) {
    const key = `${record.settlementPeriod}_${record.farmId}`;
    if (!periodRecords.has(key)) {
      periodRecords.set(key, []);
    }
    periodRecords.get(key)!.push(record);
  }
  
  // Process each period/farmId combination that doesn't already exist
  let totalCalculations = 0;
  
  for (const [key, groupRecords] of periodRecords.entries()) {
    // Skip if this combination already has a calculation
    if (existingPeriods.has(key)) {
      continue;
    }
    
    const [periodStr, farmId] = key.split('_');
    const period = parseInt(periodStr, 10);
    
    // Calculate total energy and payment for this period/farmId
    const totalEnergy = groupRecords.reduce((sum, r) => sum + Number(r.volume), 0);
    const totalPayment = groupRecords.reduce((sum, r) => sum + Number(r.payment), 0);
    
    // Calculate Bitcoin mining potential
    const bitcoinMined = calculateBitcoin(totalEnergy, minerModel, difficulty);
    
    // Insert Bitcoin calculation into database
    const calcRecord: InsertHistoricalBitcoinCalculation = {
      settlementDate: date,
      settlementPeriod: period,
      farmId,
      minerModel,
      bitcoinMined: bitcoinMined.toString(),
      difficulty: difficulty.toString(),
      calculatedAt: new Date()
    };
    
    await db.insert(historicalBitcoinCalculations).values(calcRecord)
      .onConflictDoUpdate({
        target: [
          historicalBitcoinCalculations.settlementDate, 
          historicalBitcoinCalculations.settlementPeriod,
          historicalBitcoinCalculations.farmId,
          historicalBitcoinCalculations.minerModel
        ],
        set: {
          bitcoinMined: calcRecord.bitcoinMined,
          difficulty: calcRecord.difficulty,
          calculatedAt: calcRecord.calculatedAt
        }
      });
    
    totalCalculations++;
    
    // Log every 10 calculations
    if (totalCalculations % 10 === 0) {
      console.log(`Progress: ${totalCalculations} new calculations completed`);
    }
  }
  
  console.log(`\nProcessed ${totalCalculations} new Bitcoin calculations for ${date} (${minerModel})\n`);
  return totalCalculations;
}

/**
 * Main function to process all steps
 */
async function main() {
  try {
    console.log(`\n=== Starting to Complete Bitcoin Calculations for ${DATE_TO_PROCESS} ===\n`);
    
    let totalCompleted = 0;
    
    // Process each miner model
    for (const minerModel of MINER_MODELS_TO_COMPLETE) {
      const completed = await completeBitcoinCalculations(DATE_TO_PROCESS, minerModel);
      totalCompleted += completed;
    }
    
    console.log(`\n=== Completed ${totalCompleted} Bitcoin Calculations ===\n`);
  } catch (error) {
    console.error('Error completing calculations:', error);
    process.exit(1);
  }
}

main();
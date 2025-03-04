/**
 * Reconcile Missing Bitcoin Calculations
 * 
 * This script identifies and fixes missing Bitcoin calculations for a specific date
 * by comparing curtailment_records with historical_bitcoin_calculations.
 */
import { db } from './db';
import { curtailmentRecords, historicalBitcoinCalculations } from './db/schema';
import { eq, sql, and, inArray, notInArray } from 'drizzle-orm';
import { BitcoinCalculationSchema, DEFAULT_DIFFICULTY, minerModels } from './server/types/bitcoin';
import { format } from 'date-fns';

// Default miner models
const DEFAULT_MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

interface MissingCalculation {
  date: string;
  period: number;
  farmId: string;
  minerModel: string;
}

/**
 * Find missing Bitcoin calculations for a specific date
 */
async function findMissingCalculations(date: string): Promise<MissingCalculation[]> {
  console.log(`Finding missing Bitcoin calculations for ${date}...`);
  
  // Get all unique farm-period combinations from curtailment records
  const curtailmentCombinations = await db.select({
    settlementDate: curtailmentRecords.settlementDate,
    settlementPeriod: curtailmentRecords.settlementPeriod,
    farmId: curtailmentRecords.farmId
  })
  .from(curtailmentRecords)
  .where(eq(curtailmentRecords.settlementDate, date))
  .groupBy(curtailmentRecords.settlementDate, curtailmentRecords.settlementPeriod, curtailmentRecords.farmId);
  
  console.log(`Found ${curtailmentCombinations.length} unique farm-period combinations in curtailment records`);
  
  // Get all existing Bitcoin calculations
  const existingCalculations = await db.select({
    settlementDate: historicalBitcoinCalculations.settlementDate,
    settlementPeriod: historicalBitcoinCalculations.settlementPeriod,
    farmId: historicalBitcoinCalculations.farmId,
    minerModel: historicalBitcoinCalculations.minerModel
  })
  .from(historicalBitcoinCalculations)
  .where(eq(historicalBitcoinCalculations.settlementDate, date));
  
  console.log(`Found ${existingCalculations.length} existing Bitcoin calculations`);
  
  // Create a set of farm-period-model combinations that already exist
  const existingSet = new Set(
    existingCalculations.map(record => 
      `${record.settlementDate}-${record.settlementPeriod}-${record.farmId}-${record.minerModel}`
    )
  );
  
  // Find missing calculations
  const missingCalculations: MissingCalculation[] = [];
  
  for (const comb of curtailmentCombinations) {
    for (const minerModel of DEFAULT_MINER_MODELS) {
      const key = `${comb.settlementDate}-${comb.settlementPeriod}-${comb.farmId}-${minerModel}`;
      if (!existingSet.has(key)) {
        missingCalculations.push({
          date: comb.settlementDate,
          period: comb.settlementPeriod,
          farmId: comb.farmId,
          minerModel
        });
      }
    }
  }
  
  console.log(`Found ${missingCalculations.length} missing Bitcoin calculations`);
  return missingCalculations;
}

/**
 * Fix missing Bitcoin calculations by processing them
 */
async function fixMissingCalculations(missingCalculations: MissingCalculation[]): Promise<number> {
  if (missingCalculations.length === 0) {
    console.log('No missing calculations to fix');
    return 0;
  }
  
  console.log(`Fixing ${missingCalculations.length} missing Bitcoin calculations...`);
  
  // Group by date-period-minerModel
  const groupedCalculations: Record<string, MissingCalculation[]> = {};
  
  for (const calc of missingCalculations) {
    const key = `${calc.date}-${calc.period}-${calc.minerModel}`;
    if (!groupedCalculations[key]) {
      groupedCalculations[key] = [];
    }
    groupedCalculations[key].push(calc);
  }
  
  let fixedCount = 0;
  
  // Process each group
  for (const [key, calculations] of Object.entries(groupedCalculations)) {
    const [date, periodStr, minerModel] = key.split('-');
    const period = parseInt(periodStr);
    
    console.log(`Processing ${calculations.length} calculations for ${date} period ${period} model ${minerModel}`);
    
    // Get difficulty (simplified implementation - would normally use DynamoDB)
    const difficulty = DEFAULT_DIFFICULTY; 
    
    // Get farmIds for this group
    const farmIds = calculations.map(calc => calc.farmId);
    
    // Fetch curtailment records for these farms in this period
    const curtailmentData = await db.select({
      farmId: curtailmentRecords.farmId,
      volume: curtailmentRecords.volume,
      payment: curtailmentRecords.payment
    })
    .from(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, date),
        eq(curtailmentRecords.settlementPeriod, period),
        inArray(curtailmentRecords.farmId, farmIds)
      )
    );
    
    // Group by farmId
    const volumeByFarm: Record<string, number> = {};
    
    for (const record of curtailmentData) {
      if (!volumeByFarm[record.farmId]) {
        volumeByFarm[record.farmId] = 0;
      }
      volumeByFarm[record.farmId] += Math.abs(record.volume); // Use absolute value
    }
    
    // Calculate Bitcoin for each farm using the appropriate miner model
    const insertRecords = [];
    const minerStats = minerModels[minerModel] || minerModels['S19J_PRO']; // Default to S19J_PRO if not found
    
    for (const farmId of farmIds) {
      const curtailedMwh = volumeByFarm[farmId] || 0;
      if (curtailedMwh === 0) {
        console.log(`Skipping ${farmId} with zero volume`);
        continue;
      }
      
      // Convert MWh to kWh
      const curtailedKwh = curtailedMwh * 1000;
      
      // Calculate potential hashrate
      const hours = 0.5; // Each settlement period is 30 minutes
      const potentialHashrate = (curtailedKwh / (minerStats.power / 1000)) * hours;
      
      // Calculate bitcoin mined
      const bitcoinMined = (potentialHashrate * 6.25) / (difficulty * 2 ** 32 / minerStats.hashrate / 600);
      
      insertRecords.push({
        settlementDate: date,
        settlementPeriod: period,
        farmId: farmId,
        minerModel: minerModel,
        difficulty: difficulty,
        curtailedMwh: curtailedMwh,
        bitcoinMined: bitcoinMined,
        createdAt: new Date()
      });
    }
    
    // Insert the records
    if (insertRecords.length > 0) {
      console.log(`Inserting ${insertRecords.length} Bitcoin calculation records`);
      await db.insert(historicalBitcoinCalculations).values(insertRecords);
      fixedCount += insertRecords.length;
    }
  }
  
  console.log(`Fixed ${fixedCount} missing Bitcoin calculations`);
  return fixedCount;
}

/**
 * Verify that all Curtailment Records have corresponding Bitcoin Calculations
 */
async function verifyCompleteness(date: string): Promise<boolean> {
  // Check for any remaining missing calculations
  const missingCalculations = await findMissingCalculations(date);
  
  if (missingCalculations.length === 0) {
    console.log(`✅ Verification successful! All Bitcoin calculations are present for ${date}`);
    return true;
  } else {
    console.log(`❌ Verification failed! Still missing ${missingCalculations.length} Bitcoin calculations for ${date}`);
    return false;
  }
}

/**
 * Main function to reconcile missing Bitcoin calculations
 */
async function main() {
  const date = '2025-03-02';
  
  try {
    // Find missing calculations
    const missingCalculations = await findMissingCalculations(date);
    
    if (missingCalculations.length === 0) {
      console.log(`No missing calculations found for ${date}`);
      return;
    }
    
    // Analyze missing data patterns
    console.log('\nAnalyzing missing calculation patterns:');
    
    // Count by period
    const periodCounts: Record<number, number> = {};
    for (const calc of missingCalculations) {
      periodCounts[calc.period] = (periodCounts[calc.period] || 0) + 1;
    }
    
    console.log('Missing calculations by period:');
    for (const [period, count] of Object.entries(periodCounts)) {
      console.log(`Period ${period}: ${count} missing calculations`);
    }
    
    // Count by miner model
    const modelCounts: Record<string, number> = {};
    for (const calc of missingCalculations) {
      modelCounts[calc.minerModel] = (modelCounts[calc.minerModel] || 0) + 1;
    }
    
    console.log('\nMissing calculations by miner model:');
    for (const [model, count] of Object.entries(modelCounts)) {
      console.log(`Model ${model}: ${count} missing calculations`);
    }
    
    // Fix missing calculations
    const fixedCount = await fixMissingCalculations(missingCalculations);
    
    // Verify completeness
    await verifyCompleteness(date);
    
  } catch (error) {
    console.error('Error during reconciliation:', error);
  }
}

main();
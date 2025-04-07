/**
 * Update Bitcoin Calculations for March 27, 2025
 * 
 * This script updates Bitcoin calculations for all periods and all miner models
 * using the complete set of curtailment records for March 27, 2025.
 */

import { db } from './db';
import {
  curtailmentRecords,
  historicalBitcoinCalculations,
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries
} from './db/schema';
import { eq, and, sql } from 'drizzle-orm';
import { parse, format } from 'date-fns';

// Type for historical_bitcoin_calculations table
type InsertHistoricalBitcoinCalculation = typeof historicalBitcoinCalculations.$inferInsert;
type InsertBitcoinMonthlySummary = typeof bitcoinMonthlySummaries.$inferInsert;
type InsertBitcoinYearlySummary = typeof bitcoinYearlySummaries.$inferInsert;

// Fixed date to process
const DATE_TO_PROCESS = '2025-03-27';

// Fixed difficulty (used when DynamoDB is unavailable)
const DEFAULT_DIFFICULTY = 71_000_000_000_000;

// Miner models - only process the remaining ones
const MINER_MODELS = [
  'S19_PRO',
  'M30S++'
];

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
 * Process Bitcoin calculations for all miner models
 */
async function processBitcoinCalculations(date: string): Promise<number> {
  console.log(`\n=== Processing Bitcoin Calculations for ${date} ===\n`);
  
  // Use fixed difficulty (no DynamoDB)
  const difficulty = DEFAULT_DIFFICULTY;
  console.log(`Using difficulty: ${difficulty.toLocaleString()}`);
  
  // Get curtailment records for the date
  const records = await db.query.curtailmentRecords.findMany({
    where: eq(curtailmentRecords.settlementDate, date)
  });
  
  if (records.length === 0) {
    console.log(`No curtailment records found for ${date}`);
    return 0;
  }
  
  // Process each miner model
  let totalCalculations = 0;
  
  for (const minerModel of MINER_MODELS) {
    console.log(`\n--- Processing ${minerModel} ---\n`);
    
    // Group records by settlement period and farmId
    const periodRecords = new Map<string, any[]>();
    
    for (const record of records) {
      const key = `${record.settlementPeriod}_${record.farmId}`;
      if (!periodRecords.has(key)) {
        periodRecords.set(key, []);
      }
      periodRecords.get(key)!.push(record);
    }
    
    // Process each period/farmId combination
    for (const [key, groupRecords] of periodRecords.entries()) {
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
    }
  }
  
  console.log(`\nProcessed ${totalCalculations} Bitcoin calculations for ${date}\n`);
  return totalCalculations;
}

/**
 * Update monthly Bitcoin summaries
 */
async function updateMonthlyBitcoinSummaries(date: string): Promise<void> {
  console.log('\n=== Updating Monthly Bitcoin Summaries ===\n');
  
  const parsedDate = parse(date, 'yyyy-MM-dd', new Date());
  const yearMonth = format(parsedDate, 'yyyy-MM');
  
  // Use all miner models for summaries
  const allMinerModels = ['S9', 'S17', 'S19_PRO', 'S19J_PRO', 'M30S++'];
  
  // For each miner model, calculate monthly totals
  for (const minerModel of allMinerModels) {
    console.log(`Processing monthly summary for ${yearMonth} (${minerModel})...`);
    
    // Get all Bitcoin calculations for this month and miner model
    const monthStart = `${yearMonth}-01`;
    const monthEnd = `${yearMonth}-31`; // This is safe since we're using >= and <
    
    const monthlyData = await db.query.historicalBitcoinCalculations.findMany({
      where: and(
        eq(historicalBitcoinCalculations.minerModel, minerModel),
        sql`${historicalBitcoinCalculations.settlementDate} >= ${monthStart}`,
        sql`${historicalBitcoinCalculations.settlementDate} <= ${monthEnd}`
      )
    });
    
    if (monthlyData.length === 0) {
      console.log(`No Bitcoin calculations found for ${yearMonth} (${minerModel})`);
      continue;
    }
    
    // Get curtailment records to calculate energy total
    const curtailmentData = await db.query.curtailmentRecords.findMany({
      where: and(
        sql`${curtailmentRecords.settlementDate} >= ${monthStart}`,
        sql`${curtailmentRecords.settlementDate} <= ${monthEnd}`
      )
    });
    
    // Calculate totals
    const totalEnergy = curtailmentData.reduce((sum, r) => sum + Number(r.volume), 0);
    const totalBitcoin = monthlyData.reduce((sum, r) => sum + Number(r.bitcoinMined), 0);
    const avgDifficulty = monthlyData.reduce((sum, r) => sum + Number(r.difficulty), 0) / monthlyData.length;
    
    // Update or insert monthly summary
    const monthlySummary: InsertBitcoinMonthlySummary = {
      yearMonth,
      minerModel,
      bitcoinMined: totalBitcoin.toString(),
      valueAtMining: '0', // Not calculating value in this fix script
      createdAt: new Date(),
      updatedAt: new Date()
    };
    
    await db.insert(bitcoinMonthlySummaries).values(monthlySummary)
      .onConflictDoUpdate({
        target: [bitcoinMonthlySummaries.yearMonth, bitcoinMonthlySummaries.minerModel],
        set: {
          bitcoinMined: totalBitcoin.toString(),
          updatedAt: new Date()
        }
      });
    
    console.log(`Updated monthly summary for ${yearMonth} (${minerModel}): ${totalEnergy.toFixed(2)} MWh, ${totalBitcoin.toFixed(8)} BTC`);
  }
}

/**
 * Update yearly Bitcoin summaries
 */
async function updateYearlyBitcoinSummaries(date: string): Promise<void> {
  console.log('\n=== Updating Yearly Bitcoin Summaries ===\n');
  
  const parsedDate = parse(date, 'yyyy-MM-dd', new Date());
  const year = format(parsedDate, 'yyyy');
  
  // Use all miner models for summaries
  const allMinerModels = ['S9', 'S17', 'S19_PRO', 'S19J_PRO', 'M30S++'];
  
  // For each miner model, calculate yearly totals from monthly summaries
  for (const minerModel of allMinerModels) {
    console.log(`Processing yearly summary for ${year} (${minerModel})...`);
    
    // Get all monthly summaries for this year and miner model
    const yearStart = `${year}-01`;
    const yearEnd = `${year}-12`;
    
    const monthlyData = await db.query.bitcoinMonthlySummaries.findMany({
      where: and(
        eq(bitcoinMonthlySummaries.minerModel, minerModel),
        sql`${bitcoinMonthlySummaries.yearMonth} >= ${yearStart}`,
        sql`${bitcoinMonthlySummaries.yearMonth} <= ${yearEnd}`
      )
    });
    
    if (monthlyData.length === 0) {
      console.log(`No monthly summaries found for ${year} (${minerModel})`);
      continue;
    }
    
    // Calculate totals based on actual monthly summaries
    // For yearly summary, we rely on the bitcoin amount from monthly summaries
    const totalBitcoin = monthlyData.reduce((sum, r) => sum + Number(r.bitcoinMined), 0);
    
    // Get yearly energy from curtailment records
    const yearStartDate = `${year}-01-01`;
    const yearEndDate = `${year}-12-31`;
    
    const curtailmentData = await db.query.curtailmentRecords.findMany({
      where: and(
        sql`${curtailmentRecords.settlementDate} >= ${yearStartDate}`,
        sql`${curtailmentRecords.settlementDate} <= ${yearEndDate}`
      )
    });
    
    const totalEnergy = curtailmentData.reduce((sum, r) => sum + Number(r.volume), 0);
    
    // Update or insert yearly summary
    const yearlySummary: InsertBitcoinYearlySummary = {
      year,
      minerModel,
      bitcoinMined: totalBitcoin.toString(),
      valueAtMining: '0', // Not calculating value in this fix script
      createdAt: new Date(),
      updatedAt: new Date()
    };
    
    await db.insert(bitcoinYearlySummaries).values(yearlySummary)
      .onConflictDoUpdate({
        target: [bitcoinYearlySummaries.year, bitcoinYearlySummaries.minerModel],
        set: {
          bitcoinMined: totalBitcoin.toString(),
          updatedAt: new Date()
        }
      });
    
    console.log(`Updated yearly summary for ${year} (${minerModel}): ${totalEnergy.toFixed(2)} MWh, ${totalBitcoin.toFixed(8)} BTC`);
  }
}

/**
 * Main function to process all steps
 */
async function main() {
  try {
    console.log(`\n=== Starting Bitcoin Update for ${DATE_TO_PROCESS} ===\n`);
    
    // Step 1: Process Bitcoin calculations
    console.log('\n--- Step 1: Processing Bitcoin Calculations ---\n');
    const bitcoinCount = await processBitcoinCalculations(DATE_TO_PROCESS);
    
    if (bitcoinCount === 0) {
      console.log('No Bitcoin calculations to process, skipping summaries');
      return;
    }
    
    // Step 2: Update summaries
    console.log('\n--- Step 2: Updating Summary Tables ---\n');
    await updateMonthlyBitcoinSummaries(DATE_TO_PROCESS);
    await updateYearlyBitcoinSummaries(DATE_TO_PROCESS);
    
    console.log('\n=== Processing Complete ===\n');
  } catch (error) {
    console.error('Error processing data:', error);
    process.exit(1);
  }
}

main();
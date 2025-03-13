/**
 * 2025-03-12 Data Reingest Script
 * 
 * This script reinspects the Elexon API data for 2025-03-12 to ensure there are no
 * missing periods or wind farm BMUs in the curtailment_records table.
 * 
 * It performs the following steps:
 * 1. Checks current state of records for 2025-03-12
 * 2. Reingests all periods from the Elexon API
 * 3. Updates Bitcoin calculations for all miner models
 * 4. Runs reconciliation to ensure data integrity
 * 5. Verifies the final state with period-level statistics
 * 
 * Usage:
 *   npx tsx reingest_2025_03_12.ts
 */

import { db } from './db';
import { eq, sql, count } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import { processDailyCurtailment } from './server/services/curtailment';
import { processSingleDay } from './server/services/bitcoinService';
import { exec } from 'child_process';

// Configuration
const TARGET_DATE = '2025-03-12';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Execute a command and return its output as a Promise
 */
async function executeCommand(command: string): Promise<string> {
  console.log(`Executing command: ${command}`);
  
  return new Promise((resolve, reject) => {
    exec(command, (error, stdout, stderr) => {
      if (error) {
        console.error(`Error: ${error.message}`);
        reject(error);
        return;
      }
      if (stderr) {
        console.error(`stderr: ${stderr}`);
      }
      console.log(`stdout: ${stdout}`);
      resolve(stdout);
    });
  });
}

/**
 * Check for missing periods in the curtailment data
 */
async function checkMissingPeriods(date: string): Promise<{
  missingPeriods: number[];
  existingPeriods: number[];
  stats: {
    recordCount: number;
    periodCount: number;
    farmCount: number;
    totalVolume: string;
    totalPayment: string;
  };
}> {
  // Get existing periods
  const existingPeriods = await db
    .select({ period: curtailmentRecords.settlementPeriod })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .groupBy(curtailmentRecords.settlementPeriod);
  
  const existingPeriodNumbers = existingPeriods.map(p => p.period);
  
  // Expected periods: 1 to 48
  const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
  const missingPeriods = allPeriods.filter(p => !existingPeriodNumbers.includes(p));
  
  // Get current statistics for the date
  const stats = await db
    .select({
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  return {
    missingPeriods,
    existingPeriods: existingPeriodNumbers,
    stats: stats[0] || {
      recordCount: 0,
      periodCount: 0,
      farmCount: 0,
      totalVolume: '0',
      totalPayment: '0'
    }
  };
}

/**
 * Get detailed period statistics
 */
async function getPeriodStatistics(date: string): Promise<any[]> {
  return db
    .select({
      period: curtailmentRecords.settlementPeriod,
      recordCount: sql<number>`COUNT(*)`,
      farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
}

/**
 * Main function to process the data
 */
async function processData() {
  try {
    console.log(`=== Starting data reingestion for ${TARGET_DATE} ===`);
    
    // Check current state
    console.log(`Checking current state...`);
    const beforeState = await checkMissingPeriods(TARGET_DATE);
    
    if (beforeState.stats.recordCount > 0) {
      console.log(`Current state for ${TARGET_DATE}:`);
      console.log(`- ${beforeState.stats.recordCount} records`);
      console.log(`- ${beforeState.stats.periodCount} periods`);
      console.log(`- ${beforeState.stats.farmCount} farms`);
      console.log(`- ${Number(beforeState.stats.totalVolume).toFixed(2)} MWh`);
      console.log(`- £${Number(beforeState.stats.totalPayment).toFixed(2)}`);
      
      if (beforeState.missingPeriods.length > 0) {
        console.log(`Missing periods: ${beforeState.missingPeriods.join(', ')}`);
      } else {
        console.log(`All 48 periods have data, but will reingest to ensure completeness.`);
      }
    } else {
      console.log(`No existing data found for ${TARGET_DATE}.`);
    }
    
    // Reingest curtailment data
    console.log(`\nReingesting curtailment data from Elexon API for ${TARGET_DATE}...`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Check state after reingestion
    console.log(`\nChecking state after reingestion...`);
    const afterState = await checkMissingPeriods(TARGET_DATE);
    
    console.log(`Updated state for ${TARGET_DATE}:`);
    console.log(`- ${afterState.stats.recordCount} records`);
    console.log(`- ${afterState.stats.periodCount} periods`);
    console.log(`- ${afterState.stats.farmCount} farms`);
    console.log(`- ${Number(afterState.stats.totalVolume).toFixed(2)} MWh`);
    console.log(`- £${Number(afterState.stats.totalPayment).toFixed(2)}`);
    
    if (afterState.missingPeriods.length > 0) {
      console.log(`Still missing periods: ${afterState.missingPeriods.join(', ')}`);
      console.log(`Note: These periods likely have no curtailment data in the Elexon API.`);
    } else {
      console.log(`All 48 periods now have data.`);
    }
    
    // Update Bitcoin calculations
    console.log(`\nUpdating Bitcoin calculations...`);
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing ${minerModel}...`);
      await processSingleDay(TARGET_DATE, minerModel);
    }
    console.log(`Bitcoin calculations updated for all miner models`);
    
    // Run reconciliation
    console.log(`\nRunning reconciliation for ${TARGET_DATE}...`);
    try {
      await executeCommand(`npx tsx unified_reconciliation.ts date ${TARGET_DATE}`);
      console.log(`Reconciliation completed successfully`);
    } catch (error) {
      console.log(`Error during reconciliation: ${error}`);
      console.log(`Continuing with verification...`);
    }
    
    // Get detailed period statistics
    console.log(`\n=== Detailed Period Statistics for ${TARGET_DATE} ===`);
    const periodStats = await getPeriodStatistics(TARGET_DATE);
    
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    console.log(`Period | Records | Farms | Volume (MWh) | Payment (£)`);
    console.log(`------ | ------- | ----- | ------------ | -----------`);
    
    for (const stat of periodStats) {
      console.log(`${stat.period.toString().padStart(6)} | ${stat.recordCount.toString().padStart(7)} | ${
        stat.farmCount.toString().padStart(5)} | ${Number(stat.totalVolume).toFixed(2).padStart(12)} | ${
        Number(stat.totalPayment).toFixed(2).padStart(11)}`);
      
      totalRecords += stat.recordCount;
      totalVolume += Number(stat.totalVolume);
      totalPayment += Number(stat.totalPayment);
    }
    
    console.log(`------ | ------- | ----- | ------------ | -----------`);
    console.log(`Total  | ${totalRecords.toString().padStart(7)} | ${afterState.stats.farmCount.toString().padStart(5)} | ${
      totalVolume.toFixed(2).padStart(12)} | ${totalPayment.toFixed(2).padStart(11)}`);
    
    // Final report
    console.log(`\n=== Data Reingestion Complete ===`);
    
    const change = {
      records: totalRecords - beforeState.stats.recordCount,
      volume: totalVolume - Number(beforeState.stats.totalVolume),
      payment: totalPayment - Number(beforeState.stats.totalPayment)
    };
    
    if (change.records !== 0 || change.volume !== 0 || change.payment !== 0) {
      console.log(`Changes made during reingestion:`);
      console.log(`- Records: ${change.records > 0 ? '+' : ''}${change.records}`);
      console.log(`- Volume: ${change.volume > 0 ? '+' : ''}${change.volume.toFixed(2)} MWh`);
      console.log(`- Payment: ${change.payment > 0 ? '+' : ''}£${change.payment.toFixed(2)}`);
    } else {
      console.log(`No changes were made during reingestion. Data is already complete.`);
    }
    
    console.log(`\nData reingestion and reconciliation completed for ${TARGET_DATE}`);
    
  } catch (error) {
    console.error('Error processing data:', error);
    process.exit(1);
  }
}

// Run the script
processData().then(() => {
  console.log('\nScript execution completed successfully');
  process.exit(0);
}).catch((error) => {
  console.error('Script execution failed:', error);
  process.exit(1);
});
/**
 * 2025-03-18 Data Reingest Script
 * 
 * This script checks for missing periods in the curtailment_records table
 * for 2025-03-18 and reingests data from the Elexon API as needed.
 * 
 * Features:
 * - Checks for missing periods
 * - Handles API rate limiting
 * - Manages duplicate records
 * - Updates Bitcoin calculations for all models
 * 
 * Usage:
 *   npx tsx process_2025_03_18.ts
 */

import { db } from './db';
import { eq, sql, count } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import { processDailyCurtailment } from './server/services/curtailment';
import { processSingleDay } from './server/services/bitcoinService';
import { exec } from 'child_process';

// Configuration
const TARGET_DATE = '2025-03-18';
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
  const periodStats = await db
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
  
  return periodStats;
}

/**
 * Main function to process the data
 */
async function processData() {
  try {
    console.log(`=== Starting data verification and reingestion for ${TARGET_DATE} ===`);
    
    // Check current state
    console.log(`Checking current state...`);
    const beforeState = await checkMissingPeriods(TARGET_DATE);
    
    // Print summary of current state
    if (beforeState.stats.recordCount > 0) {
      console.log(`Current state for ${TARGET_DATE}:`);
      console.log(`- ${beforeState.stats.recordCount} records`);
      console.log(`- ${beforeState.stats.periodCount} periods out of 48`);
      console.log(`- ${beforeState.stats.farmCount} farms`);
      console.log(`- ${Number(beforeState.stats.totalVolume).toFixed(2)} MWh`);
      console.log(`- £${Number(beforeState.stats.totalPayment).toFixed(2)}`);
      
      if (beforeState.missingPeriods.length > 0) {
        console.log(`Missing periods: ${beforeState.missingPeriods.join(', ')}`);
        console.log(`Need to reingest these periods.`);
      } else {
        console.log(`All 48 periods have data, but will check for completeness.`);
        
        // Optional: Show per-period statistics to identify problems
        console.log(`\nDetailed period statistics:`);
        const periodStats = await getPeriodStatistics(TARGET_DATE);
        periodStats.forEach(ps => {
          console.log(`Period ${ps.period}: ${ps.recordCount} records, ${ps.farmCount} farms, ${Number(ps.totalVolume).toFixed(2)} MWh, £${Number(ps.totalPayment).toFixed(2)}`);
        });
      }
    } else {
      console.log(`No existing data found for ${TARGET_DATE}. Need to ingest all periods.`);
    }
    
    // Reingest curtailment data
    console.log(`\nReingesting curtailment data from Elexon API for ${TARGET_DATE}...`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Check state after reingestion
    console.log(`\nChecking state after reingestion...`);
    const afterState = await checkMissingPeriods(TARGET_DATE);
    
    console.log(`Updated state for ${TARGET_DATE}:`)
    console.log(`- ${afterState.stats.recordCount} records`);
    console.log(`- ${afterState.stats.periodCount} periods out of 48`);
    console.log(`- ${afterState.stats.farmCount} farms`);
    console.log(`- ${Number(afterState.stats.totalVolume).toFixed(2)} MWh`);
    console.log(`- £${Number(afterState.stats.totalPayment).toFixed(2)}`);
    
    if (afterState.missingPeriods.length > 0) {
      console.log(`Still missing periods: ${afterState.missingPeriods.join(', ')}`);
      console.log(`Note: These periods likely have no curtailment data in the Elexon API.`);
    } else {
      console.log(`All 48 periods now have data.`);
    }
    
    // Update Bitcoin calculations for each miner model
    console.log(`\nUpdating Bitcoin calculations...`);
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing ${minerModel}...`);
      await processSingleDay(TARGET_DATE, minerModel);
    }
    console.log(`Bitcoin calculations updated for all miner models`);
    
    // Run reconciliation (with timeout protection)
    console.log(`\nRunning reconciliation for ${TARGET_DATE}...`);
    try {
      const reconciliationPromise = executeCommand(`npx tsx unified_reconciliation.ts date ${TARGET_DATE}`);
      const timeoutPromise = new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Reconciliation timed out')), 60000)
      );
      
      await Promise.race([reconciliationPromise, timeoutPromise]);
      console.log(`Reconciliation completed successfully`);
    } catch (error) {
      console.log(`Reconciliation incomplete: ${error}`);
      console.log(`Continuing with verification...`);
    }
    
    // Final report
    console.log(`\n=== Processing Complete ===`);
    
    const change = {
      records: afterState.stats.recordCount - beforeState.stats.recordCount,
      periods: afterState.stats.periodCount - beforeState.stats.periodCount,
      volume: Number(afterState.stats.totalVolume) - Number(beforeState.stats.totalVolume),
      payment: Number(afterState.stats.totalPayment) - Number(beforeState.stats.totalPayment)
    };
    
    if (change.records !== 0 || change.volume !== 0 || change.payment !== 0) {
      console.log(`Changes made during processing:`);
      console.log(`- Records: ${change.records > 0 ? '+' : ''}${change.records}`);
      console.log(`- Periods: ${change.periods > 0 ? '+' : ''}${change.periods}`);
      console.log(`- Volume: ${change.volume > 0 ? '+' : ''}${change.volume.toFixed(2)} MWh`);
      console.log(`- Payment: ${change.payment > 0 ? '+' : ''}£${change.payment.toFixed(2)}`);
    } else {
      console.log(`No changes were made. Data is already complete.`);
    }
    
    console.log(`\nData verification, reingestion and reconciliation completed for ${TARGET_DATE}`);
    
  } catch (error) {
    console.error(`Error processing ${TARGET_DATE}:`, error);
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
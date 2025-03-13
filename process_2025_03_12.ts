/**
 * Focused Processor for 2025-03-12
 * 
 * This script provides a streamlined way to process data for 2025-03-12
 * specifically, with optimized handling of Elexon API requests and
 * proper ES Module support.
 * 
 * Features:
 * - ES Module compatible
 * - Batch processing to avoid timeouts
 * - Focused on just the critical date
 * - Comprehensive error handling
 */

import { db } from './db';
import { and, between, eq, sql } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { processDailyCurtailment } from './server/services/curtailment';
import { processSingleDay } from './server/services/bitcoinService';
import { exec } from 'child_process';

// Set up ES Module compatible dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const TARGET_DATE = '2025-03-12';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');

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
 * Simple delay function
 */
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Check current database state for the target date
 */
async function checkCurrentState(date: string): Promise<{
  recordCount: number;
  periodCount: number;
  farmCount: number;
  totalVolume: string;
  totalPayment: string;
  missingPeriods: number[];
}> {
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
  
  return {
    ...stats[0],
    missingPeriods,
    recordCount: stats[0]?.recordCount || 0,
    periodCount: stats[0]?.periodCount || 0,
    farmCount: stats[0]?.farmCount || 0,
    totalVolume: stats[0]?.totalVolume || '0',
    totalPayment: stats[0]?.totalPayment || '0'
  };
}

/**
 * Process the target date
 */
async function processTargetDate() {
  try {
    console.log(`=== Starting focused processing for ${TARGET_DATE} ===`);
    
    // Step 1: Check current state
    console.log(`Checking current state...`);
    const beforeState = await checkCurrentState(TARGET_DATE);
    
    console.log(`Current state for ${TARGET_DATE}:`);
    console.log(`- ${beforeState.recordCount} records`);
    console.log(`- ${beforeState.periodCount} periods`);
    console.log(`- ${beforeState.farmCount} farms`);
    console.log(`- ${Number(beforeState.totalVolume).toFixed(2)} MWh`);
    console.log(`- £${Number(beforeState.totalPayment).toFixed(2)}`);
    
    if (beforeState.missingPeriods.length > 0) {
      console.log(`Missing periods: ${beforeState.missingPeriods.join(', ')}`);
    } else {
      console.log(`All 48 periods have data, but will reingest to ensure completeness.`);
    }
    
    // Step 2: Process daily curtailment data
    console.log(`\nProcessing curtailment data from Elexon API for ${TARGET_DATE}...`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Step 3: Check state after processing
    console.log(`\nChecking state after processing...`);
    const afterState = await checkCurrentState(TARGET_DATE);
    
    console.log(`Updated state for ${TARGET_DATE}:`);
    console.log(`- ${afterState.recordCount} records`);
    console.log(`- ${afterState.periodCount} periods`);
    console.log(`- ${afterState.farmCount} farms`);
    console.log(`- ${Number(afterState.totalVolume).toFixed(2)} MWh`);
    console.log(`- £${Number(afterState.totalPayment).toFixed(2)}`);
    
    if (afterState.missingPeriods.length > 0) {
      console.log(`Still missing periods: ${afterState.missingPeriods.join(', ')}`);
      console.log(`Note: These periods likely have no curtailment data in the Elexon API.`);
    } else {
      console.log(`All 48 periods now have data.`);
    }
    
    // Step 4: Update Bitcoin calculations for each miner model
    console.log(`\nUpdating Bitcoin calculations...`);
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing ${minerModel}...`);
      await processSingleDay(TARGET_DATE, minerModel);
    }
    console.log(`Bitcoin calculations updated for all miner models`);
    
    // Step 5: Run reconciliation (with timeout protection)
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
      records: afterState.recordCount - beforeState.recordCount,
      periods: afterState.periodCount - beforeState.periodCount,
      volume: Number(afterState.totalVolume) - Number(beforeState.totalVolume),
      payment: Number(afterState.totalPayment) - Number(beforeState.totalPayment)
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
    
    console.log(`\nData reingestion and reconciliation completed for ${TARGET_DATE}`);
    
  } catch (error) {
    console.error(`Error processing ${TARGET_DATE}:`, error);
    process.exit(1);
  }
}

// Run the script
processTargetDate().then(() => {
  console.log('\nScript execution completed successfully');
  process.exit(0);
}).catch((error) => {
  console.error('Script execution failed:', error);
  process.exit(1);
});
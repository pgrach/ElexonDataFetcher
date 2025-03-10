/**
 * Simplified script to process missing periods for 2025-03-07
 */

import { db } from './db';
import { and, eq, sql, inArray, not } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import { fileURLToPath } from 'url';
import path from 'path';
import fs from 'fs';
import { exec } from 'child_process';
import { isValidDateString } from './server/utils/dates';
import { processDailyCurtailment } from './server/services/curtailment';
import { processSingleDay } from './server/services/bitcoinService';

// Configuration
const date = '2025-03-07';

// Default configuration
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Execute a command and return its output as a Promise

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

async function processData() {
  try {
    console.log(`Starting data processing for ${date}`);
    
    // Determine which periods are missing
    const existingPeriods = await db
      .select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.settlementPeriod);
    
    const existingPeriodNumbers = existingPeriods.map(p => p.period);
    console.log(`Found existing periods: ${existingPeriodNumbers.join(', ')}`);
    
    // Expected periods: 1 to 48
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    const missingPeriods = allPeriods.filter(p => !existingPeriodNumbers.includes(p));
    
    if (missingPeriods.length === 0) {
      console.log('No missing periods found. All 48 periods have data.');
      return;
    }
    
    console.log(`Missing periods: ${missingPeriods.join(', ')}`);
    
    // Use processDailyCurtailment to get all data for that day
    console.log(`Reingesting curtailment data from Elexon API for ${date}...`);
    await processDailyCurtailment(date);
    console.log('Curtailment data reingestion completed');
    
    // Process Bitcoin calculations
    console.log('Updating Bitcoin calculations...');
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing ${minerModel}...`);
      await processSingleDay(date, minerModel);
    }
    console.log('Bitcoin calculations completed');
    
    // Verify results
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
    
    console.log('\n=== Results ===');
    console.log(`Date: ${date}`);
    console.log(`Records: ${stats[0]?.recordCount || 0}`);
    console.log(`Periods: ${stats[0]?.periodCount || 0}`);
    console.log(`Farms: ${stats[0]?.farmCount || 0}`);
    console.log(`Volume: ${Number(stats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Payment: £${Number(stats[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Run reconciliation for this date
    try {
      await executeCommand(`npx tsx unified_reconciliation.ts date ${date}`);
    } catch (error) {
      console.log(`Error during reconciliation: ${error}`);
    }
    
    // Check status again after processing
    const updatedPeriods = await db
      .select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.settlementPeriod);
    
    const updatedPeriodNumbers = updatedPeriods.map(p => p.period);
    console.log(`\nUpdated periods: ${updatedPeriodNumbers.join(', ')}`);
    
    const stillMissingPeriods = allPeriods.filter(p => !updatedPeriodNumbers.includes(p));
    if (stillMissingPeriods.length === 0) {
      console.log('Success! All 48 periods now have data.');
    } else {
      console.log(`Still missing periods: ${stillMissingPeriods.join(', ')}`);
      console.log('Note: If some periods are still missing, they may not have curtailment data in the Elexon API.');
    }
    
  } catch (error) {
    console.error('Error processing data:', error);
    process.exit(1);
  }
}

// Run the script
processData().then(() => {
  console.log('Script execution completed successfully');
  process.exit(0);
}).catch((error) => {
  console.error('Script execution failed:', error);
  process.exit(1);
});
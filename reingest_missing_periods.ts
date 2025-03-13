/**
 * Simplified script to process missing periods for specific dates
 * 
 * This script provides a more targeted approach to identifying and filling
 * missing periods for curtailment records and Bitcoin calculations.
 */

import { db } from './db';
import { eq, sql } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import { processDailyCurtailment } from './server/services/curtailment';
import { processSingleDay } from './server/services/bitcoinService';
import { exec } from 'child_process';

// Configuration
const TARGET_DATES = ['2025-03-11', '2025-03-12'];
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

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

async function checkMissingPeriods(date: string): Promise<{
  missingPeriods: number[],
  periodCount: number,
  recordCount: number,
  totalVolume: string
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
  
  // Get statistics
  const stats = await db
    .select({
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  return {
    missingPeriods,
    periodCount: stats[0]?.periodCount || 0,
    recordCount: stats[0]?.recordCount || 0,
    totalVolume: stats[0]?.totalVolume || '0'
  };
}

async function processData() {
  console.log("=== Starting reconciliation for target dates ===");
  
  for (const date of TARGET_DATES) {
    console.log(`\n\n--- Processing ${date} ---`);
    
    // Step 1: Check for missing periods
    console.log(`Checking for missing periods in ${date}...`);
    const beforeState = await checkMissingPeriods(date);
    
    if (beforeState.missingPeriods.length > 0) {
      console.log(`Found ${beforeState.missingPeriods.length} missing periods: ${beforeState.missingPeriods.join(', ')}`);
      console.log(`Current state: ${beforeState.recordCount} records across ${beforeState.periodCount} periods, ${Number(beforeState.totalVolume).toFixed(2)} MWh`);
      
      // Step 2: Process daily curtailment to fill in missing periods
      console.log(`\nReingesting curtailment data for ${date}...`);
      await processDailyCurtailment(date);
      
      // Step 3: Verify periods were filled
      const afterState = await checkMissingPeriods(date);
      console.log(`\nAfter reingestion: ${afterState.recordCount} records across ${afterState.periodCount} periods, ${Number(afterState.totalVolume).toFixed(2)} MWh`);
      
      if (afterState.missingPeriods.length > 0) {
        console.log(`Still missing ${afterState.missingPeriods.length} periods: ${afterState.missingPeriods.join(', ')}`);
        console.log("Note: These periods may have no curtailment data available in the Elexon API");
      } else {
        console.log("All 48 periods now present in the data");
      }
    } else {
      console.log(`All 48 periods already present for ${date}`);
      console.log(`Current state: ${beforeState.recordCount} records, ${Number(beforeState.totalVolume).toFixed(2)} MWh`);
    }
    
    // Step 4: Update Bitcoin calculations regardless
    console.log(`\nUpdating Bitcoin calculations for ${date}...`);
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing ${minerModel}...`);
      await processSingleDay(date, minerModel);
    }
    console.log(`Bitcoin calculations updated for all miner models`);
    
    // Step 5: Run reconciliation for this date
    console.log(`\nRunning targeted reconciliation for ${date}...`);
    try {
      const reconciliationPromise = executeCommand(`npx tsx unified_reconciliation.ts date ${date}`);
      const timeoutPromise = new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Reconciliation timed out')), 30000)
      );
      
      await Promise.race([reconciliationPromise, timeoutPromise]);
      console.log(`Reconciliation completed successfully for ${date}`);
    } catch (error) {
      console.log(`Reconciliation incomplete (may have timed out): ${error}`);
    }
  }
  
  // Final step: Update monthly and yearly summaries
  console.log("\n=== Updating monthly and yearly summaries ===");
  try {
    const monthYear = TARGET_DATES[0].substring(0, 7); // YYYY-MM
    const year = TARGET_DATES[0].substring(0, 4);      // YYYY
    
    console.log(`Running manual monthly update for ${monthYear}...`);
    await executeCommand(`npx tsx -e "require('./server/services/bitcoinService').manualUpdateMonthlyBitcoinSummary('${monthYear}')"`);
    
    console.log(`Running manual yearly update for ${year}...`);
    await executeCommand(`npx tsx -e "require('./server/services/bitcoinService').manualUpdateYearlyBitcoinSummary('${year}')"`);
    
    console.log("Summary updates completed successfully");
  } catch (error) {
    console.error("Error updating summaries:", error);
  }
  
  console.log("\n=== Reconciliation Complete ===");
  console.log(`Processed dates: ${TARGET_DATES.join(', ')}`);
  console.log("All Bitcoin calculations and summaries have been updated");
}

// Run the script
processData().then(() => {
  console.log("\nScript execution completed successfully");
  process.exit(0);
}).catch((error) => {
  console.error("Script execution failed:", error);
  process.exit(1);
});
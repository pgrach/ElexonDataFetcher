/**
 * Fix Missing Periods for 2025-03-12
 * 
 * This script fixes the data for 2025-03-12 by:
 * 1. Removing all duplicate records
 * 2. Processing the specific missing periods (39, 40, 41, 42, 43, 44, 47)
 * 3. Updating Bitcoin calculations for all miner models
 * 4. Running reconciliation
 * 
 * Usage:
 *   npx tsx fix_missing_periods_2025_03_12.ts
 */

import { db } from './db';
import { eq, sql, count, inArray, and, not } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import { processDailyCurtailment } from './server/services/curtailment';
import { processSingleDay } from './server/services/bitcoinService';
import { exec } from 'child_process';
import { fetchBidsOffers, delay } from './server/services/elexon';

// Configuration
const TARGET_DATE = '2025-03-12';
const MISSING_PERIODS = [39, 40, 41, 42, 43, 44, 47];
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
 * Find and remove duplicate records
 */
async function removeDuplicateRecords(date: string): Promise<number> {
  console.log(`Finding duplicate records for ${date}...`);
  
  // Instead of trying to identify all duplicates in one go, we'll take a different approach
  // We'll recreate the table without duplicates
  
  // Step 1: Count the current records
  const beforeCount = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
    
  console.log(`Before cleanup: ${beforeCount[0].count} records`);
  
  // Step 2: Create a temporary table with unique records (keeping the lowest ID for each group)
  await db.execute(sql`
    CREATE TEMPORARY TABLE temp_curtailment AS
    WITH unique_records AS (
      SELECT DISTINCT ON (settlement_date, settlement_period, farm_id) 
        id, 
        settlement_date, 
        settlement_period,
        farm_id,
        lead_party_name,
        volume,
        payment,
        original_price,
        final_price,
        so_flag,
        cadl_flag,
        created_at
      FROM curtailment_records
      WHERE settlement_date = ${date}
      ORDER BY settlement_date, settlement_period, farm_id, id
    )
    SELECT * FROM unique_records;
  `);
  
  // Step 3: Delete all records for the date
  await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  // Step 4: Insert the unique records back
  await db.execute(sql`
    INSERT INTO curtailment_records (
      settlement_date, 
      settlement_period,
      farm_id,
      lead_party_name,
      volume,
      payment,
      original_price,
      final_price,
      so_flag,
      cadl_flag,
      created_at
    )
    SELECT 
      settlement_date, 
      settlement_period,
      farm_id,
      lead_party_name,
      volume,
      payment,
      original_price,
      final_price,
      so_flag,
      cadl_flag,
      created_at
    FROM temp_curtailment;
  `);
  
  // Step 5: Drop the temporary table
  await db.execute(sql`DROP TABLE temp_curtailment;`);
  
  // Step 6: Count the new records
  const afterCount = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  const removedCount = beforeCount[0].count - afterCount[0].count;
  console.log(`After cleanup: ${afterCount[0].count} records (removed ${removedCount} duplicates)`);
  
  return removedCount;
}

/**
 * Process only the missing periods for 2025-03-12
 */
async function processMissingPeriods(date: string, periods: number[]): Promise<void> {
  console.log(`Processing missing periods for ${date}: ${periods.join(', ')}`);
  
  for (const period of periods) {
    try {
      console.log(`Processing period ${period}...`);
      
      // Check if we already have data for this period (double check)
      const existingRecords = await db
        .select({ count: sql<number>`COUNT(*)` })
        .from(curtailmentRecords)
        .where(and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period)
        ));
      
      if (existingRecords[0]?.count > 0) {
        console.log(`Period ${period} already has ${existingRecords[0].count} records, skipping.`);
        continue;
      }
      
      // Fetch data from Elexon for this period
      const records = await fetchBidsOffers(date, period);
      if (records.length === 0) {
        console.log(`No records found for period ${period}`);
        continue;
      }
      
      console.log(`Found ${records.length} records for period ${period}`);
      
      // Wait a bit to avoid rate limiting
      await delay(2000);
    } catch (error) {
      console.error(`Error processing period ${period}:`, error);
    }
  }
}

/**
 * Main function to process the data
 */
async function processData() {
  try {
    console.log(`=== Starting fix for ${TARGET_DATE} ===`);
    
    // Check current state
    console.log(`Checking current state...`);
    const beforeState = await checkMissingPeriods(TARGET_DATE);
    
    console.log(`Current state for ${TARGET_DATE}:`);
    console.log(`- ${beforeState.stats.recordCount} records`);
    console.log(`- ${beforeState.stats.periodCount} periods`);
    console.log(`- ${beforeState.stats.farmCount} farms`);
    console.log(`- ${Number(beforeState.stats.totalVolume).toFixed(2)} MWh`);
    console.log(`- £${Number(beforeState.stats.totalPayment).toFixed(2)}`);
    
    if (beforeState.missingPeriods.length > 0) {
      console.log(`Missing periods: ${beforeState.missingPeriods.join(', ')}`);
    }
    
    // Step 1: Remove duplicate records
    await removeDuplicateRecords(TARGET_DATE);
    
    // Step 2: Process only the missing periods
    await processMissingPeriods(TARGET_DATE, MISSING_PERIODS);
    
    // Step 3: Run the full daily curtailment process to ensure all data is consistent
    console.log(`\nRunning full daily curtailment process for ${TARGET_DATE}...`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Check state after reingestion
    console.log(`\nChecking state after fixing...`);
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
    
    // Get current statistics for verification
    const finalStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Final report
    console.log(`\n=== Fix Complete ===`);
    console.log(`Final state for ${TARGET_DATE}:`);
    console.log(`- ${finalStats[0].recordCount} records`);
    console.log(`- ${finalStats[0].periodCount} periods`);
    console.log(`- ${finalStats[0].farmCount} farms`);
    console.log(`- ${Number(finalStats[0].totalVolume).toFixed(2)} MWh`);
    console.log(`- £${Number(finalStats[0].totalPayment).toFixed(2)}`);
    
    const change = {
      records: finalStats[0].recordCount - beforeState.stats.recordCount,
      volume: Number(finalStats[0].totalVolume) - Number(beforeState.stats.totalVolume),
      payment: Number(finalStats[0].totalPayment) - Number(beforeState.stats.totalPayment)
    };
    
    console.log(`Changes made during fix:`);
    console.log(`- Records: ${change.records > 0 ? '+' : ''}${change.records}`);
    console.log(`- Volume: ${change.volume > 0 ? '+' : ''}${change.volume.toFixed(2)} MWh`);
    console.log(`- Payment: ${change.payment > 0 ? '+' : ''}£${change.payment.toFixed(2)}`);
    
    console.log(`\nData fix completed for ${TARGET_DATE}`);
    
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
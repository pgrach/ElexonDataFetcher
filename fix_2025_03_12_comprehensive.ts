/**
 * Comprehensive Fix for 2025-03-12 Data
 * 
 * This script performs a complete data remediation process:
 * 1. Removes duplicate curtailment records
 * 2. Processes missing periods (39-44 and 47)
 * 3. Updates Bitcoin calculations for all miner models
 * 4. Runs reconciliation to ensure data integrity
 * 
 * Usage:
 *   npx tsx fix_2025_03_12_comprehensive.ts
 */

import { db } from './db';
import { eq, and, sql, count } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import { fetchBidsOffers, delay } from './server/services/elexon';
import fs from 'fs/promises';
import { exec } from 'child_process';

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
      resolve(stdout);
    });
  });
}

/**
 * Remove duplicate records for the target date
 */
async function removeDuplicateRecords(): Promise<number> {
  console.log(`\n=== Removing duplicate records for ${TARGET_DATE} ===`);
  
  // First check current state
  const beforeState = await db
    .select({
      recordCount: count(),
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  console.log(`Current state for ${TARGET_DATE}:`);
  console.log(`- ${beforeState[0].recordCount} records`);
  console.log(`- ${beforeState[0].periodCount} periods`);
  console.log(`- ${Number(beforeState[0].totalVolume || 0).toFixed(2)} MWh`);
  console.log(`- £${Number(beforeState[0].totalPayment || 0).toFixed(2)}`);
  
  // Create temporary table with only unique records
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
      WHERE settlement_date = ${TARGET_DATE}
      ORDER BY settlement_date, settlement_period, farm_id, id
    )
    SELECT * FROM unique_records;
  `);
  
  // Delete all current records for the date
  const deleted = await db.delete(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .returning({ id: curtailmentRecords.id });
  
  console.log(`Deleted ${deleted.length} records`);
  
  // Insert the unique records back
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
  
  // Check how many records were inserted
  const inserted = await db
    .select({ count: count() })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  console.log(`Inserted ${inserted[0].count} unique records`);
  
  // Drop the temporary table
  await db.execute(sql`DROP TABLE temp_curtailment;`);
  
  // Check final state
  const afterState = await db
    .select({
      recordCount: count(),
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  console.log(`\nAfter deduplication for ${TARGET_DATE}:`);
  console.log(`- ${afterState[0].recordCount} records`);
  console.log(`- ${afterState[0].periodCount} periods`);
  console.log(`- ${Number(afterState[0].totalVolume || 0).toFixed(2)} MWh`);
  console.log(`- £${Number(afterState[0].totalPayment || 0).toFixed(2)}`);
  
  const removedCount = beforeState[0].recordCount - afterState[0].recordCount;
  console.log(`\nRemoved ${removedCount} duplicate records`);
  
  return removedCount;
}

/**
 * Process the missing periods for the target date
 */
async function processMissingPeriods(): Promise<void> {
  console.log(`\n=== Processing missing periods for ${TARGET_DATE} ===`);
  
  // Load BMU mapping for looking up lead party names
  console.log('Loading BMU mapping from server/data/bmuMapping.json');
  const mappingContent = await fs.readFile('./server/data/bmuMapping.json', 'utf8');
  const bmuMapping = JSON.parse(mappingContent);
  
  // Create a map for lead party names
  const bmuLeadPartyMap = new Map(
    bmuMapping
      .filter((bmu: any) => bmu.fuelType === "WIND")
      .map((bmu: any) => [bmu.elexonBmUnit, bmu.leadPartyName || 'Unknown'])
  );
  
  // Get the set of valid wind farm IDs
  const validWindFarmIds = new Set(
    bmuMapping
      .filter((bmu: any) => bmu.fuelType === "WIND")
      .map((bmu: any) => bmu.elexonBmUnit)
  );
  
  console.log(`Loaded ${validWindFarmIds.size} wind farm BMUs`);
  
  // Find which periods are still missing
  const missingPeriodsResult = [];
  for (const period of MISSING_PERIODS) {
    const countResult = await db
      .select({ count: count() })
      .from(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        eq(curtailmentRecords.settlementPeriod, period)
      ));
      
    if (countResult[0].count === 0) {
      missingPeriodsResult.push(period);
    }
  }
  
  if (missingPeriodsResult.length === 0) {
    console.log('No missing periods to process!');
    return;
  }
  
  console.log(`Found ${missingPeriodsResult.length} missing periods: ${missingPeriodsResult.join(', ')}`);
  
  // Process each missing period
  for (const period of missingPeriodsResult) {
    try {
      console.log(`\nProcessing period ${period}...`);
      
      // Fetch data from Elexon API
      const records = await fetchBidsOffers(TARGET_DATE, period);
      
      if (records.length === 0) {
        console.log(`No records found for period ${period} from Elexon API`);
        continue;
      }
      
      // Filter for valid wind farm records
      const validRecords = records.filter(record =>
        record.volume < 0 &&
        (record.soFlag || record.cadlFlag) &&
        validWindFarmIds.has(record.id)
      );
      
      if (validRecords.length === 0) {
        console.log(`No valid wind farm records for period ${period}`);
        continue;
      }
      
      console.log(`Found ${validRecords.length} valid wind farm records for period ${period}`);
      
      // Insert records to the database
      let insertedCount = 0;
      let totalVolume = 0;
      let totalPayment = 0;
      
      for (const record of validRecords) {
        const volume = record.volume;
        const payment = volume * record.originalPrice;
        
        try {
          await db.insert(curtailmentRecords).values({
            settlementDate: TARGET_DATE,
            settlementPeriod: period,
            farmId: record.id,
            leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
            volume: volume.toString(), // Keep the original negative value
            payment: payment.toString(),
            originalPrice: record.originalPrice.toString(),
            finalPrice: record.finalPrice.toString(),
            soFlag: record.soFlag,
            cadlFlag: record.cadlFlag || false
          });
          
          insertedCount++;
          totalVolume += Math.abs(volume);
          totalPayment += Math.abs(payment);
        } catch (error) {
          console.error(`Error inserting record for ${record.id}:`, error);
        }
      }
      
      console.log(`[${TARGET_DATE} P${period}] Records: ${insertedCount} (${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)})`);
      
      // Wait between periods to avoid rate limiting
      await delay(2000);
      
    } catch (error) {
      console.error(`Error processing period ${period}:`, error);
    }
  }
}

/**
 * Update Bitcoin calculations for the target date
 */
async function updateBitcoinCalculations(): Promise<void> {
  console.log(`\n=== Updating Bitcoin calculations for ${TARGET_DATE} ===`);
  
  try {
    await executeCommand(`npx tsx unified_reconciliation.ts date ${TARGET_DATE}`);
    console.log('Bitcoin calculations updated successfully');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    console.log('Continuing with verification...');
  }
}

/**
 * Verify the final state after all fixes
 */
async function verifyFinalState(): Promise<void> {
  console.log(`\n=== Verifying final state for ${TARGET_DATE} ===`);
  
  // Get curtailment record stats
  const curtailmentStats = await db
    .select({
      recordCount: count(),
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  console.log(`Curtailment records:`);
  console.log(`- ${curtailmentStats[0].recordCount} records`);
  console.log(`- ${curtailmentStats[0].periodCount} periods`);
  console.log(`- ${curtailmentStats[0].farmCount} farms`);
  console.log(`- ${Number(curtailmentStats[0].totalVolume || 0).toFixed(2)} MWh`);
  console.log(`- £${Number(curtailmentStats[0].totalPayment || 0).toFixed(2)}`);
  
  // Check if any periods are still missing
  const missingPeriods = [];
  for (let i = 1; i <= 48; i++) {
    const periodCount = await db
      .select({ count: count() })
      .from(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        eq(curtailmentRecords.settlementPeriod, i)
      ));
      
    if (periodCount[0].count === 0) {
      missingPeriods.push(i);
    }
  }
  
  if (missingPeriods.length > 0) {
    console.log(`\nWARNING: Still missing ${missingPeriods.length} periods: ${missingPeriods.join(', ')}`);
  } else {
    console.log(`\nSUCCESS: All 48 periods now have data!`);
  }
}

/**
 * Main function to run the entire fix process
 */
async function main() {
  try {
    console.log(`===============================================`);
    console.log(`    COMPREHENSIVE FIX FOR ${TARGET_DATE} DATA`);
    console.log(`===============================================`);
    
    const startTime = Date.now();
    
    // Step 1: Remove duplicate records
    await removeDuplicateRecords();
    
    // Step 2: Process missing periods
    await processMissingPeriods();
    
    // Step 3: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Step 4: Verify final state
    await verifyFinalState();
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(1);
    console.log(`\n=== Process completed in ${duration}s ===`);
    
  } catch (error) {
    console.error('\nError during fix process:', error);
    process.exit(1);
  }
}

// Run the script
main().then(() => {
  console.log('\nScript execution completed successfully');
  process.exit(0);
}).catch((error) => {
  console.error('\nScript execution failed:', error);
  process.exit(1);
});
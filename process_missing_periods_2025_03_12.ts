/**
 * Process Missing Periods for 2025-03-12
 * 
 * This script focuses just on the missing periods for 2025-03-12:
 * - Processes periods 39, 40, 41, 42, 43, 44, 47
 * - Uses the existing Elexon API service
 * - Inserts records directly without going through the full reingestion process
 * 
 * Usage:
 *   npx tsx process_missing_periods_2025_03_12.ts
 */

import { db } from './db';
import { eq, and, sql, count } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import { fetchBidsOffers, delay } from './server/services/elexon';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { processDailyCurtailment } from './server/services/curtailment';
import { processSingleDay } from './server/services/bitcoinService';
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

// Main function
async function processData() {
  try {
    console.log(`=== Processing missing periods for ${TARGET_DATE} ===`);
    
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
    
    // Process each missing period
    for (const period of MISSING_PERIODS) {
      try {
        console.log(`\nProcessing period ${period}...`);
        
        // Check if data already exists for this period
        const existingCount = await db
          .select({ count: count() })
          .from(curtailmentRecords)
          .where(and(
            eq(curtailmentRecords.settlementDate, TARGET_DATE),
            eq(curtailmentRecords.settlementPeriod, period)
          ));
        
        if (existingCount[0].count > 0) {
          console.log(`Period ${period} already has ${existingCount[0].count} records, skipping.`);
          continue;
        }
        
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
      console.error(`Error during reconciliation:`, error);
      console.log(`Continuing with verification...`);
    }
    
    // Verify the final state
    const finalStats = await db
      .select({
        recordCount: count(),
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\n=== Final State ===`);
    console.log(`- Records: ${finalStats[0].recordCount}`);
    console.log(`- Periods: ${finalStats[0].periodCount}`);
    console.log(`- Farms: ${finalStats[0].farmCount}`);
    console.log(`- Volume: ${Number(finalStats[0].totalVolume || 0).toFixed(2)} MWh`);
    console.log(`- Payment: £${Number(finalStats[0].totalPayment || 0).toFixed(2)}`);
    
    // Check if any periods are still missing
    const missingPeriods = [];
    for (let i = 1; i <= 48; i++) {
      const count = await db
        .select({ count: count() })
        .from(curtailmentRecords)
        .where(and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, i)
        ));
        
      if (count[0].count === 0) {
        missingPeriods.push(i);
      }
    }
    
    if (missingPeriods.length > 0) {
      console.log(`\nStill missing periods: ${missingPeriods.join(', ')}`);
    } else {
      console.log(`\nAll 48 periods now have data!`);
    }
    
    console.log(`\nProcessing complete.`);
    
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
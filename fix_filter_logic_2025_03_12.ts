/**
 * Fix Filter Logic for 2025-03-12
 * 
 * This script fixes the data for 2025-03-12 by:
 * 1. Removing all existing records (which might include records with incorrect filtering)
 * 2. Reprocessing all 48 periods using the correct filter logic from elexon.ts
 * 3. Running reconciliation to update all Bitcoin calculations
 * 
 * Usage:
 *   npx tsx fix_filter_logic_2025_03_12.ts
 */

// Import required modules
import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { and, eq, count } from 'drizzle-orm';
import { fetchBidsOffers, delay } from './server/services/elexon';
import fs from 'fs/promises';
import { exec } from 'child_process';

// Configuration
const TARGET_DATE = '2025-03-12';

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
 * Remove all existing records for the target date
 */
async function removeExistingRecords(): Promise<number> {
  try {
    console.log(`\nRemoving all existing records for ${TARGET_DATE}...`);
    
    // Get the current count
    const countResult = await db
      .select({ count: count() })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const currentCount = countResult[0]?.count || 0;
    console.log(`Found ${currentCount} records to remove`);
    
    if (currentCount === 0) {
      return 0;
    }
    
    // Delete all records for the date
    await db
      .delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Removed ${currentCount} records for ${TARGET_DATE}`);
    return currentCount;
  } catch (error) {
    console.error('Error removing existing records:', error);
    throw error;
  }
}

/**
 * Process all periods for the target date using correct filter logic
 */
async function processAllPeriods(): Promise<void> {
  try {
    console.log(`\nProcessing all periods for ${TARGET_DATE}...`);
    
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
    
    let totalInsertedRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Process each period
    for (let period = 1; period <= 48; period++) {
      try {
        console.log(`\nProcessing period ${period}...`);
        
        // Fetch data from Elexon API
        const records = await fetchBidsOffers(TARGET_DATE, period);
        
        if (records.length === 0) {
          console.log(`No records found for period ${period} from Elexon API`);
          continue;
        }
        
        // Filter for valid wind farm records using EXACT same logic as elexon.ts
        const validRecords = records.filter(record =>
          record.volume < 0 &&
          record.soFlag &&
          validWindFarmIds.has(record.id)
        );
        
        if (validRecords.length === 0) {
          console.log(`No valid wind farm records for period ${period}`);
          continue;
        }
        
        console.log(`Found ${validRecords.length} valid wind farm records for period ${period}`);
        
        // Insert records to the database
        let insertedCount = 0;
        let periodVolume = 0;
        let periodPayment = 0;
        
        for (const record of validRecords) {
          const volume = record.volume;
          const payment = volume * record.originalPrice;
          
          try {
            await db.insert(curtailmentRecords).values({
              settlementDate: TARGET_DATE,
              settlementPeriod: period,
              farmId: record.id,
              leadPartyName: bmuLeadPartyMap.get(record.id) || 'Unknown',
              volume: volume.toString(), // Negative value
              payment: payment.toString(),
              originalPrice: record.originalPrice.toString(),
              finalPrice: record.finalPrice.toString(),
              soFlag: record.soFlag,
              cadlFlag: record.cadlFlag || false
            });
            
            insertedCount++;
            periodVolume += Math.abs(volume);
            periodPayment += payment;
          } catch (error) {
            console.error(`Error inserting record for ${record.id}:`, error);
          }
        }
        
        totalInsertedRecords += insertedCount;
        totalVolume += periodVolume;
        totalPayment += periodPayment;
        
        console.log(`[${TARGET_DATE} P${period}] Records: ${insertedCount} (${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
        
        // Wait between periods to avoid rate limiting
        await delay(2000);
        
      } catch (error) {
        console.error(`Error processing period ${period}:`, error);
      }
    }
    
    console.log(`\nProcessing complete for ${TARGET_DATE}`);
    console.log(`Total inserted records: ${totalInsertedRecords}`);
    console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);
    
    // Check periods with no data
    const missingPeriods = [];
    for (let i = 1; i <= 48; i++) {
      const countResult = await db
        .select({ count: count() })
        .from(curtailmentRecords)
        .where(and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, i)
        ));
        
      if (countResult[0].count === 0) {
        missingPeriods.push(i);
      }
    }
    
    if (missingPeriods.length > 0) {
      console.log(`\nMissing periods (${missingPeriods.length}): ${missingPeriods.join(', ')}`);
    } else {
      console.log(`\nAll 48 periods now have data!`);
    }
    
  } catch (error) {
    console.error('Error processing all periods:', error);
    throw error;
  }
}

/**
 * Update Bitcoin calculations for the target date
 */
async function updateBitcoinCalculations(): Promise<void> {
  try {
    console.log(`\nUpdating Bitcoin calculations for ${TARGET_DATE}...`);
    
    // Run the unified reconciliation for the date
    const command = `npx tsx unified_reconciliation.ts date ${TARGET_DATE}`;
    
    try {
      // We don't wait for this to complete as it might timeout
      // The process will continue in the background
      console.log(`Executing command in background: ${command}`);
      executeCommand(command).catch(err => {
        console.error('Error executing reconciliation command:', err);
      });
      
      console.log(`Bitcoin calculation update initiated for ${TARGET_DATE}`);
      console.log('This will continue in the background');
    } catch (error) {
      console.error('Error initiating reconciliation:', error);
    }
    
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    throw error;
  }
}

/**
 * Main function to run the entire fix process
 */
async function main() {
  try {
    console.log(`=== Starting filter logic fix for ${TARGET_DATE} ===`);
    
    // Step 1: Remove all existing records
    await removeExistingRecords();
    
    // Step 2: Process all periods using correct filter logic
    await processAllPeriods();
    
    // Step 3: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    console.log(`\n=== Filter logic fix for ${TARGET_DATE} completed ===`);
    console.log(`\nTo verify the results, run the following query:`);
    console.log(`SELECT COUNT(*) as total_records, SUM(ABS(volume::numeric)) as total_volume, SUM(payment::numeric) as total_payment FROM curtailment_records WHERE settlement_date = '${TARGET_DATE}';`);
    
  } catch (error) {
    console.error('Fix process failed:', error);
    process.exit(1);
  }
}

// Run the script
main().then(() => {
  console.log('Script executed successfully');
}).catch((error) => {
  console.error('Script execution failed:', error);
  process.exit(1);
});
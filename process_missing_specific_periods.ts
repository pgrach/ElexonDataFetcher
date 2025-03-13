/**
 * Process Specific Missing Periods for 2025-03-12
 * 
 * This script processes only the remaining missing periods (42, 43, 44, 47)
 * for 2025-03-12 using the Elexon API.
 * 
 * Usage:
 *   npx tsx process_missing_specific_periods.ts
 */

import { db } from './db';
import { eq, and, count } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import { fetchBidsOffers, delay } from './server/services/elexon';
import fs from 'fs/promises';
import { exec } from 'child_process';

// Configuration
const TARGET_DATE = '2025-03-12';
const SPECIFIC_PERIODS = [43, 44, 47]; // Remaining periods to process

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
async function processSpecificPeriods() {
  try {
    console.log(`=== Processing specific missing periods for ${TARGET_DATE} ===`);
    console.log(`Target periods: ${SPECIFIC_PERIODS.join(', ')}`);
    
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
    
    // Process each specific period
    for (const period of SPECIFIC_PERIODS) {
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
        // NOTE: We're using only soFlag (not cadlFlag) to match the original elexon.ts service logic
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
    
    // Check if all periods are now covered
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
      console.log(`\nStill missing periods (${missingPeriods.length}): ${missingPeriods.join(', ')}`);
    } else {
      console.log(`\nAll 48 periods now have data!`);
      
      // If we've fixed all periods, run the Bitcoin calculations update and reconciliation
      console.log(`\nAll periods are now complete, running Bitcoin calculations update...`);
      
      // We'll do this as a separate step to avoid timeout
      console.log(`\nProcessing complete. To update Bitcoin calculations, run:`);
      console.log(`npx tsx unified_reconciliation.ts date ${TARGET_DATE}`);
    }
    
  } catch (error) {
    console.error('Error processing specific periods:', error);
    process.exit(1);
  }
}

// Run the script
processSpecificPeriods().then(() => {
  console.log('Script execution completed successfully');
  process.exit(0);
}).catch((error) => {
  console.error('Script execution failed:', error);
  process.exit(1);
});
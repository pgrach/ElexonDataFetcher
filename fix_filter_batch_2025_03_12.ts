/**
 * Fix Filter Logic for 2025-03-12 (Batch Processing)
 * 
 * This script fixes the data for 2025-03-12 by:
 * 1. Processing a specific range of periods using the correct filter logic from elexon.ts
 * 2. Each batch can be run independently to avoid timeouts
 * 
 * Usage:
 *   npx tsx fix_filter_batch_2025_03_12.ts [startPeriod=1] [endPeriod=48]
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
const DEFAULT_START_PERIOD = 1;
const DEFAULT_END_PERIOD = 48;

// Parse command line arguments
const startPeriod = parseInt(process.argv[2]) || DEFAULT_START_PERIOD;
const endPeriod = parseInt(process.argv[3]) || DEFAULT_END_PERIOD;

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
 * Process a specific range of periods
 */
async function processPeriodsRange(startPeriod: number, endPeriod: number): Promise<void> {
  try {
    console.log(`\nProcessing periods ${startPeriod}-${endPeriod} for ${TARGET_DATE}...`);
    
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
    
    // Process the specified range of periods
    for (let period = startPeriod; period <= endPeriod; period++) {
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
          console.log(`Period ${period} already has ${existingCount[0].count} records, clearing...`);
          
          // Delete existing records for this period
          await db
            .delete(curtailmentRecords)
            .where(and(
              eq(curtailmentRecords.settlementDate, TARGET_DATE),
              eq(curtailmentRecords.settlementPeriod, period)
            ));
            
          console.log(`Cleared ${existingCount[0].count} records for period ${period}`);
        }
        
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
    
    console.log(`\nProcessing complete for periods ${startPeriod}-${endPeriod} of ${TARGET_DATE}`);
    console.log(`Total inserted records: ${totalInsertedRecords}`);
    console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);
    
  } catch (error) {
    console.error('Error processing periods range:', error);
    throw error;
  }
}

/**
 * Check for missing periods after processing
 */
async function checkMissingPeriods(): Promise<number[]> {
  console.log(`\nChecking for missing periods in ${TARGET_DATE}...`);
  
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
    console.log(`Missing periods (${missingPeriods.length}): ${missingPeriods.join(', ')}`);
  } else {
    console.log(`All 48 periods have data!`);
  }
  
  return missingPeriods;
}

/**
 * Main function to run the batch process
 */
async function main() {
  try {
    console.log(`=== Starting batch processing (periods ${startPeriod}-${endPeriod}) for ${TARGET_DATE} ===`);
    
    // Process the specified range of periods
    await processPeriodsRange(startPeriod, endPeriod);
    
    // Check if we still have any missing periods
    const missingPeriods = await checkMissingPeriods();
    
    // Print overall stats for the date
    const totals = await db
      .select({
        totalRecords: count(),
        totalVolume: sql`SUM(ABS(volume::numeric))`,
        totalPayment: sql`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nCurrent overall totals for ${TARGET_DATE}:`);
    console.log(`- Total records: ${totals[0].totalRecords}`);
    console.log(`- Total volume: ${Number(totals[0].totalVolume || 0).toFixed(2)} MWh`);
    console.log(`- Total payment: £${Number(totals[0].totalPayment || 0).toFixed(2)}`);
    
    if (missingPeriods.length === 0 && startPeriod === 1 && endPeriod === 48) {
      console.log(`\nAll periods processed successfully, initiating Bitcoin calculation update...`);
      
      // Run the unified reconciliation for the date in the background
      const command = `npx tsx unified_reconciliation.ts date ${TARGET_DATE}`;
      
      try {
        // We don't wait for this to complete as it might timeout
        console.log(`Executing command in background: ${command}`);
        executeCommand(command).catch(err => {
          console.error('Error executing reconciliation command:', err);
        });
        
        console.log(`Bitcoin calculation update initiated for ${TARGET_DATE}`);
        console.log('This will continue in the background');
      } catch (error) {
        console.error('Error initiating reconciliation:', error);
      }
    } else if (missingPeriods.length > 0) {
      console.log(`\nStill missing ${missingPeriods.length} periods. Run the script again with these periods.`);
      
      // Group missing periods into ranges for easier processing
      const ranges = [];
      let rangeStart = missingPeriods[0];
      let prev = missingPeriods[0];
      
      for (let i = 1; i < missingPeriods.length; i++) {
        if (missingPeriods[i] !== prev + 1) {
          ranges.push([rangeStart, prev]);
          rangeStart = missingPeriods[i];
        }
        prev = missingPeriods[i];
      }
      ranges.push([rangeStart, prev]);
      
      console.log(`Suggested commands to run for missing periods:`);
      ranges.forEach(([start, end]) => {
        console.log(`npx tsx fix_filter_batch_2025_03_12.ts ${start} ${end}`);
      });
    }
    
    console.log(`\n=== Batch processing completed ===`);
    
  } catch (error) {
    console.error('Batch processing failed:', error);
    process.exit(1);
  }
}

// Import sql for the query
import { sql } from 'drizzle-orm';

// Run the script
main().then(() => {
  console.log('Script executed successfully');
}).catch((error) => {
  console.error('Script execution failed:', error);
  process.exit(1);
});
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
import { eq, and, sql } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import { fetchBidsOffers, delay } from './server/services/elexon';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// Configuration
const TARGET_DATE = '2025-03-12';
const MISSING_PERIODS = [39, 40, 41, 42, 43, 44, 47];
const BMU_MAPPING_PATH = path.join(path.dirname(fileURLToPath(import.meta.url)), 'server/data/bmuMapping.json');

// Main function
async function processData() {
  try {
    console.log(`=== Processing missing periods for ${TARGET_DATE} ===`);
    
    // Load BMU mapping for looking up lead party names
    console.log('Loading BMU mapping...');
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
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
    
    // Process each missing period
    for (const period of MISSING_PERIODS) {
      try {
        console.log(`\nProcessing period ${period}...`);
        
        // Check if data already exists for this period
        const existingCount = await db
          .select({ count: db.fn.count() })
          .from(curtailmentRecords)
          .where(and(
            eq(curtailmentRecords.settlementDate, TARGET_DATE),
            eq(curtailmentRecords.settlementPeriod, period)
          ));
        
        if (Number(existingCount[0].count) > 0) {
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
              cadlFlag: record.cadlFlag
            });
            
            console.log(`Added record for ${record.id}: ${Math.abs(volume)} MWh, £${Math.abs(payment)}`);
          } catch (error) {
            console.error(`Error inserting record for ${record.id}:`, error);
          }
        }
        
        // Wait between periods to avoid rate limiting
        await delay(2000);
        
      } catch (error) {
        console.error(`Error processing period ${period}:`, error);
      }
    }
    
    // Verify the final state
    const finalStats = await db.execute(sql`
      SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT settlement_period) as period_count,
        COUNT(DISTINCT farm_id) as farm_count,
        SUM(ABS(volume::numeric)) as total_volume,
        SUM(payment::numeric) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    console.log(`\n=== Final State ===`);
    console.log(`- Records: ${finalStats.rows[0].record_count}`);
    console.log(`- Periods: ${finalStats.rows[0].period_count}`);
    console.log(`- Farms: ${finalStats.rows[0].farm_count}`);
    console.log(`- Volume: ${Number(finalStats.rows[0].total_volume).toFixed(2)} MWh`);
    console.log(`- Payment: £${Number(finalStats.rows[0].total_payment).toFixed(2)}`);
    
    // Check if any periods are still missing
    const missingPeriods = [];
    for (let i = 1; i <= 48; i++) {
      const count = await db
        .select({ count: db.fn.count() })
        .from(curtailmentRecords)
        .where(and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, i)
        ));
        
      if (Number(count[0].count) === 0) {
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
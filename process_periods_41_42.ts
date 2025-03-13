/**
 * Process Specific Missing Periods 41 and 42 for 2025-03-12
 * 
 * This script processes only the remaining missing periods (41, 42)
 * for 2025-03-12 using the Elexon API.
 * 
 * Usage:
 *   npx tsx process_periods_41_42.ts
 */

import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { fetchBidsOffers, delay } from './server/services/elexon';
import { eq, and } from 'drizzle-orm';
import fs from 'fs';
import path from 'path';

const TARGET_DATE = '2025-03-12';
const TARGET_PERIODS = [41, 42];

/**
 * Execute a command and return its output as a Promise
 */
async function executeCommand(command: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const { execSync } = require('child_process');
    try {
      const output = execSync(command).toString();
      resolve(output);
    } catch (error: any) {
      reject(new Error(`Command failed: ${error.message}`));
    }
  });
}

async function processSpecificPeriods() {
  console.log(`=== Processing periods 41 and 42 for ${TARGET_DATE} ===`);
  
  // Load BMU mapping
  console.log('Loading BMU mapping from server/data/bmuMapping.json');
  const bmuMappingPath = path.join('server', 'data', 'bmuMapping.json');
  const bmuMapping = JSON.parse(fs.readFileSync(bmuMappingPath, 'utf8'));
  console.log(`Loaded ${Object.keys(bmuMapping).length} wind farm BMUs`);
  
  // For each period
  for (const period of TARGET_PERIODS) {
    console.log(`\nProcessing period ${period}...`);
    
    try {
      // Check if period already exists
      const existingRecords = await db.select()
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, TARGET_DATE),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );
      
      if (existingRecords.length > 0) {
        console.log(`Period ${period} already has ${existingRecords.length} records, skipping.`);
        continue;
      }
      
      // Fetch data from Elexon API
      console.log(`Loading BMU mapping from: ${path.resolve(bmuMappingPath)}`);
      const validFarmIds = new Set(Object.keys(bmuMapping));
      console.log(`Loaded ${validFarmIds.size} wind farm BMU IDs`);
      
      // Fetch data from Elexon API
      const apiData = await fetchBidsOffers(TARGET_DATE, period);
      
      // Filter valid wind farm records with soFlag=true
      const validRecords = apiData.filter(record => 
        validFarmIds.has(record.bmUnit || '') && 
        record.soFlag === true
      );
      
      console.log(`[${TARGET_DATE} P${period}] Records: ${validRecords.length} (${
        validRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0).toFixed(2)
      } MWh, £${
        validRecords.reduce((sum, r) => sum + r.volume * r.finalPrice, 0).toFixed(2)
      })`);
      
      if (validRecords.length === 0) {
        console.log(`No valid wind farm records found for period ${period}`);
        continue;
      }
      
      // Insert into database
      const recordsToInsert = validRecords.map(record => ({
        settlementDate: TARGET_DATE,
        settlementPeriod: period,
        farmId: record.bmUnit || '',
        leadPartyName: record.leadPartyName || '',
        volume: record.volume,
        price: record.finalPrice,
        originalPrice: record.originalPrice,
        payment: record.volume * record.finalPrice,
        soFlag: record.soFlag,
        cadlFlag: record.cadlFlag || false,
        createdAt: new Date()
      }));
      
      // Insert records
      if (recordsToInsert.length > 0) {
        try {
          // Try inserting one by one
          for (const record of recordsToInsert) {
            await db.insert(curtailmentRecords).values([record]);
          }
          console.log(`Inserted ${recordsToInsert.length} records successfully`);
        } catch (err) {
          console.error('Error inserting records:', err);
        }
      }
      
      console.log(`Found ${validRecords.length} valid wind farm records for period ${period}`);
      console.log(`[${TARGET_DATE} P${period}] Records: ${validRecords.length} (${
        validRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0).toFixed(2)
      } MWh, £${
        validRecords.reduce((sum, r) => sum + r.volume * r.finalPrice, 0).toFixed(2)
      })`);
      
      // Wait a bit between periods
      await delay(2000);
      
    } catch (error) {
      console.error(`Error processing period ${period}:`, error);
    }
  }
  
  // Check remaining missing periods
  const remainingPeriods = await findMissingPeriods();
  
  if (remainingPeriods.length > 0) {
    console.log(`\nStill missing periods (${remainingPeriods.length}): ${remainingPeriods.join(', ')}`);
  } else {
    console.log('\nAll periods processed successfully!');
    
    // Update Bitcoin calculations
    try {
      console.log('\nUpdating Bitcoin calculations...');
      await executeCommand(`npx tsx unified_reconciliation.ts date ${TARGET_DATE}`);
      console.log('Bitcoin calculations updated successfully');
    } catch (error) {
      console.error('Error updating Bitcoin calculations:', error);
    }
  }
  
  console.log('Script execution completed successfully');
}

async function findMissingPeriods(): Promise<number[]> {
  const result = await db.execute(
    `WITH all_periods AS (
      SELECT generate_series(1, 48) AS period
    )
    SELECT 
      ap.period 
    FROM 
      all_periods ap
    LEFT JOIN (
      SELECT DISTINCT settlement_period 
      FROM curtailment_records 
      WHERE settlement_date = '${TARGET_DATE}'
    ) cr ON ap.period = cr.settlement_period
    WHERE cr.settlement_period IS NULL
    ORDER BY ap.period`
  );
  
  return result.rows.map(row => parseInt(row.period.toString()));
}

// Run the script
processSpecificPeriods().catch(error => {
  console.error('Error:', error);
  process.exit(1);
});
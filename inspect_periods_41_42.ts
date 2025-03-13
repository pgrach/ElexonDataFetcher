/**
 * Inspect periods 41 and 42 for 2025-03-12 to understand why they're not processing
 * 
 * This script analyzes data from the Elexon API for these periods
 * to identify why valid records aren't being found despite data being present.
 * 
 * Usage:
 *   npx tsx inspect_periods_41_42.ts
 */

import { fetchBidsOffers } from './server/services/elexon';
import fs from 'fs';
import path from 'path';

const TARGET_DATE = '2025-03-12';
const TARGET_PERIODS = [41, 42];

async function inspectPeriods() {
  console.log(`=== Inspecting periods 41 and 42 for ${TARGET_DATE} ===`);
  
  // Load BMU mapping
  console.log('Loading BMU mapping from server/data/bmuMapping.json');
  const bmuMappingPath = path.join('server', 'data', 'bmuMapping.json');
  const bmuMapping = JSON.parse(fs.readFileSync(bmuMappingPath, 'utf8'));
  const validFarmIds = new Set(Object.keys(bmuMapping));
  console.log(`Loaded ${Object.keys(bmuMapping).length} wind farm BMUs`);
  
  // For each period
  for (const period of TARGET_PERIODS) {
    console.log(`\nInspecting period ${period}...`);
    
    try {
      // Fetch data from Elexon API
      const apiData = await fetchBidsOffers(TARGET_DATE, period);
      
      // Check all records
      console.log(`Total records: ${apiData.length}`);
      
      // Filter wind farm records
      const windFarmRecords = apiData.filter(record => validFarmIds.has(record.bmUnit || ''));
      console.log(`Wind farm records: ${windFarmRecords.length}`);
      
      // Filter by soFlag
      const soFlagTrue = windFarmRecords.filter(record => record.soFlag === true);
      const soFlagFalse = windFarmRecords.filter(record => record.soFlag === false);
      const soFlagNull = windFarmRecords.filter(record => record.soFlag === null || record.soFlag === undefined);
      
      console.log(`soFlag === true: ${soFlagTrue.length}`);
      console.log(`soFlag === false: ${soFlagFalse.length}`);
      console.log(`soFlag === null/undefined: ${soFlagNull.length}`);
      
      if (windFarmRecords.length > 0) {
        // Analyze a few records
        console.log('\nSample record analysis:');
        for (let i = 0; i < Math.min(3, windFarmRecords.length); i++) {
          const record = windFarmRecords[i];
          console.log(`Record ${i + 1}:`);
          console.log(`  BMU: ${record.bmUnit}`);
          console.log(`  Lead Party: ${record.leadPartyName}`);
          console.log(`  Volume: ${record.volume}`);
          console.log(`  Price: ${record.finalPrice}`);
          console.log(`  soFlag: ${record.soFlag}`);
          console.log(`  cadlFlag: ${record.cadlFlag}`);
        }
      }
      
      // Special case for these periods: Consider using records with soFlag=false
      console.log('\nAnalyzing all wind farm records (including soFlag=false):');
      console.log(`Volume: ${windFarmRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0).toFixed(2)} MWh`);
      console.log(`Payment: £${windFarmRecords.reduce((sum, r) => sum + r.volume * r.finalPrice, 0).toFixed(2)}`);
      
    } catch (error) {
      console.error(`Error inspecting period ${period}:`, error);
    }
  }
  
  console.log('\nInspection completed');
}

// Run the script
inspectPeriods().catch(error => {
  console.error('Error:', error);
  process.exit(1);
});
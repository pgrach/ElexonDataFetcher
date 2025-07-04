/**
 * Check July 3, 2025 data against Elexon API
 * 
 * This script compares our database records with the Elexon API
 * to identify any missing data and reingest if necessary.
 */

import { db } from '../db';
import { curtailmentRecords } from '../db/schema';
import { fetchBidsOffers } from '../server/services/elexon';
import { eq, and } from 'drizzle-orm';

const TARGET_DATE = '2025-07-03';

async function checkJuly3Data() {
  console.log(`\n=== Checking July 3, 2025 Data ===`);
  
  // Get current database records
  const dbRecords = await db
    .select()
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .orderBy(curtailmentRecords.settlementPeriod, curtailmentRecords.farmId);
  
  console.log(`Database records: ${dbRecords.length}`);
  
  // Get periods with data in our database
  const dbPeriods = [...new Set(dbRecords.map(r => r.settlementPeriod))].sort((a, b) => a - b);
  console.log(`Database periods: ${dbPeriods.join(', ')}`);
  
  // Check each period against the Elexon API
  const apiDataByPeriod = new Map();
  const missingPeriods = [];
  
  console.log(`\nChecking each period against Elexon API...`);
  
  for (let period = 1; period <= 48; period++) {
    try {
      console.log(`\nChecking period ${period}...`);
      
      const apiRecords = await fetchBidsOffers(TARGET_DATE, period);
      
      if (apiRecords.length > 0) {
        console.log(`✓ Period ${period}: ${apiRecords.length} records from API`);
        apiDataByPeriod.set(period, apiRecords);
        
        // Check if we have this period in our database
        const dbPeriodRecords = dbRecords.filter(r => r.settlementPeriod === period);
        
        if (dbPeriodRecords.length === 0) {
          console.log(`❌ Period ${period}: Missing from database!`);
          missingPeriods.push(period);
        } else if (dbPeriodRecords.length !== apiRecords.length) {
          console.log(`⚠️  Period ${period}: Record count mismatch (DB: ${dbPeriodRecords.length}, API: ${apiRecords.length})`);
          missingPeriods.push(period);
        } else {
          console.log(`✓ Period ${period}: Record counts match`);
        }
      } else {
        console.log(`- Period ${period}: No curtailment data in API`);
      }
      
      // Rate limiting - small delay between requests
      await new Promise(resolve => setTimeout(resolve, 200));
      
    } catch (error) {
      console.error(`Error checking period ${period}:`, error.message);
    }
  }
  
  // Summary
  console.log(`\n=== Summary ===`);
  console.log(`Total periods with API data: ${apiDataByPeriod.size}`);
  console.log(`Total periods in database: ${dbPeriods.length}`);
  console.log(`Missing or mismatched periods: ${missingPeriods.length}`);
  
  if (missingPeriods.length > 0) {
    console.log(`Periods needing reingest: ${missingPeriods.join(', ')}`);
    
    // Show detailed comparison for missing periods
    console.log(`\n=== Detailed Analysis ===`);
    for (const period of missingPeriods) {
      const apiRecords = apiDataByPeriod.get(period) || [];
      const dbPeriodRecords = dbRecords.filter(r => r.settlementPeriod === period);
      
      console.log(`\nPeriod ${period}:`);
      console.log(`  API records: ${apiRecords.length}`);
      console.log(`  DB records: ${dbPeriodRecords.length}`);
      
      if (apiRecords.length > 0) {
        const apiTotal = apiRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
        const apiPayment = apiRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);
        console.log(`  API total: ${apiTotal.toFixed(2)} MWh, £${apiPayment.toFixed(2)}`);
        
        const apiFarms = [...new Set(apiRecords.map(r => r.id))];
        console.log(`  API farms: ${apiFarms.join(', ')}`);
      }
      
      if (dbPeriodRecords.length > 0) {
        const dbTotal = dbPeriodRecords.reduce((sum, r) => sum + Math.abs(Number(r.volume)), 0);
        const dbPayment = dbPeriodRecords.reduce((sum, r) => sum + Number(r.payment), 0);
        console.log(`  DB total: ${dbTotal.toFixed(2)} MWh, £${dbPayment.toFixed(2)}`);
        
        const dbFarms = [...new Set(dbPeriodRecords.map(r => r.farmId))];
        console.log(`  DB farms: ${dbFarms.join(', ')}`);
      }
    }
  } else {
    console.log(`✅ All data matches between database and API!`);
  }
  
  return {
    missingPeriods,
    apiDataByPeriod,
    dbRecords,
    needsReingest: missingPeriods.length > 0
  };
}

async function main() {
  try {
    const result = await checkJuly3Data();
    
    if (result.needsReingest) {
      console.log(`\n=== Reingest Required ===`);
      console.log(`Would you like to proceed with reingesting missing data?`);
      console.log(`Missing periods: ${result.missingPeriods.join(', ')}`);
    }
    
  } catch (error) {
    console.error('Error:', error);
  }
}

main();
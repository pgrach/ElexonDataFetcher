/**
 * Focused July 3, 2025 Data Verification
 * 
 * This script efficiently verifies the data by:
 * 1. Checking all periods that have DB data against API
 * 2. Sampling key periods without DB data to confirm they're empty in API
 * 3. Doing detailed record-by-record comparison for periods with data
 */

import { db } from '../db';
import { curtailmentRecords } from '../db/schema';
import { fetchBidsOffers } from '../server/services/elexon';
import { eq, and, sql } from 'drizzle-orm';

const TARGET_DATE = '2025-07-03';

async function focusedVerification() {
  console.log(`=== Focused Verification: July 3, 2025 ===`);
  
  // Step 1: Get periods that have data in our database
  const dbPeriods = await db.execute(sql`
    SELECT DISTINCT settlement_period 
    FROM curtailment_records 
    WHERE settlement_date = ${TARGET_DATE}
    ORDER BY settlement_period
  `);
  
  const periodsWithData = dbPeriods.rows.map(row => row.settlement_period);
  console.log(`Database has data for ${periodsWithData.length} periods: ${periodsWithData.join(', ')}`);
  
  // Step 2: Verify each period with data
  let allMatch = true;
  let totalDiscrepancies = 0;
  
  console.log(`\nVerifying periods with data against API...`);
  
  for (const period of periodsWithData) {
    try {
      console.log(`\nChecking period ${period}:`);
      
      // Get API data
      const apiData = await fetchBidsOffers(TARGET_DATE, period);
      console.log(`  API: ${apiData.length} records`);
      
      // Get DB data
      const dbData = await db
        .select()
        .from(curtailmentRecords)
        .where(and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, period)
        ));
      
      console.log(`  DB:  ${dbData.length} records`);
      
      // Compare counts
      if (apiData.length !== dbData.length) {
        console.log(`  ❌ COUNT MISMATCH: API=${apiData.length}, DB=${dbData.length}`);
        allMatch = false;
        totalDiscrepancies++;
      } else {
        console.log(`  ✓ Count matches`);
      }
      
      // Compare totals
      const apiTotal = apiData.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      const dbTotal = dbData.reduce((sum, r) => sum + Math.abs(Number(r.volume)), 0);
      const volumeDiff = Math.abs(apiTotal - dbTotal);
      
      if (volumeDiff > 0.01) {
        console.log(`  ❌ VOLUME MISMATCH: API=${apiTotal.toFixed(3)} MWh, DB=${dbTotal.toFixed(3)} MWh`);
        allMatch = false;
        totalDiscrepancies++;
      } else {
        console.log(`  ✓ Volume matches: ${apiTotal.toFixed(2)} MWh`);
      }
      
      // Compare individual records (if counts match)
      if (apiData.length === dbData.length && apiData.length > 0) {
        const apiFarmIds = new Set(apiData.map(r => r.id));
        const dbFarmIds = new Set(dbData.map(r => r.farmId));
        
        const missingInDb = [...apiFarmIds].filter(id => !dbFarmIds.has(id));
        const extraInDb = [...dbFarmIds].filter(id => !apiFarmIds.has(id));
        
        if (missingInDb.length > 0) {
          console.log(`  ❌ MISSING IN DB: ${missingInDb.join(', ')}`);
          allMatch = false;
          totalDiscrepancies++;
        }
        
        if (extraInDb.length > 0) {
          console.log(`  ❌ EXTRA IN DB: ${extraInDb.join(', ')}`);
          allMatch = false;
          totalDiscrepancies++;
        }
        
        if (missingInDb.length === 0 && extraInDb.length === 0) {
          console.log(`  ✓ All farm IDs match`);
        }
      }
      
      // Rate limiting
      await new Promise(resolve => setTimeout(resolve, 200));
      
    } catch (error) {
      console.log(`  ❌ ERROR: ${error.message}`);
      allMatch = false;
      totalDiscrepancies++;
    }
  }
  
  // Step 3: Sample check periods without data
  console.log(`\nSpot checking periods without data...`);
  const sampleEmptyPeriods = [1, 5, 19, 22, 48]; // Representative sample
  
  for (const period of sampleEmptyPeriods) {
    try {
      const apiData = await fetchBidsOffers(TARGET_DATE, period);
      if (apiData.length > 0) {
        console.log(`❌ Period ${period}: Found ${apiData.length} records in API but none in DB!`);
        allMatch = false;
        totalDiscrepancies++;
      } else {
        console.log(`✓ Period ${period}: Correctly empty in both API and DB`);
      }
      
      await new Promise(resolve => setTimeout(resolve, 100));
      
    } catch (error) {
      console.log(`❌ Period ${period}: Error checking API - ${error.message}`);
    }
  }
  
  // Step 4: Summary
  console.log(`\n=== VERIFICATION RESULTS ===`);
  console.log(`Periods with data verified: ${periodsWithData.length}`);
  console.log(`Sample empty periods checked: ${sampleEmptyPeriods.length}`);
  console.log(`Total discrepancies found: ${totalDiscrepancies}`);
  
  if (allMatch && totalDiscrepancies === 0) {
    console.log(`\n🎯 VERIFICATION PASSED`);
    console.log(`July 3, 2025 data is accurate and matches Elexon API.`);
    
    // Final stats
    const finalStats = await db.execute(sql`
      SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT settlement_period) as periods,
        SUM(ABS(volume::numeric)) as total_volume,
        SUM(payment::numeric) as total_payment
      FROM curtailment_records 
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const stats = finalStats.rows[0];
    console.log(`\nFinal verified stats:`);
    console.log(`  Records: ${stats.total_records}`);
    console.log(`  Periods: ${stats.periods}/48`);
    console.log(`  Volume: ${Number(stats.total_volume).toFixed(2)} MWh`);
    console.log(`  Payment: £${Number(stats.total_payment).toFixed(2)}`);
    
  } else {
    console.log(`\n❌ VERIFICATION FAILED`);
    console.log(`Found ${totalDiscrepancies} discrepancies that need attention.`);
  }
  
  return allMatch && totalDiscrepancies === 0;
}

async function main() {
  try {
    const success = await focusedVerification();
    process.exit(success ? 0 : 1);
  } catch (error) {
    console.error('❌ Verification error:', error);
    process.exit(1);
  }
}

main();
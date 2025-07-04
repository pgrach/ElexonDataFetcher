/**
 * Comprehensive July 3, 2025 Data Verification
 * 
 * This script will check every single settlement period (1-48) for July 3, 2025
 * and compare each record with the Elexon API to ensure complete data accuracy.
 */

import { db } from '../db';
import { curtailmentRecords } from '../db/schema';
import { fetchBidsOffers } from '../server/services/elexon';
import { eq, and } from 'drizzle-orm';

const TARGET_DATE = '2025-07-03';

interface VerificationResult {
  period: number;
  apiRecords: number;
  dbRecords: number;
  match: boolean;
  apiTotal: number;
  dbTotal: number;
  discrepancies: string[];
}

async function verifyPeriod(period: number): Promise<VerificationResult> {
  const result: VerificationResult = {
    period,
    apiRecords: 0,
    dbRecords: 0,
    match: false,
    apiTotal: 0,
    dbTotal: 0,
    discrepancies: []
  };

  try {
    // Fetch from API
    const apiData = await fetchBidsOffers(TARGET_DATE, period);
    result.apiRecords = apiData.length;
    result.apiTotal = apiData.reduce((sum, r) => sum + Math.abs(r.volume), 0);
    
    // Fetch from database
    const dbData = await db
      .select()
      .from(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, TARGET_DATE),
        eq(curtailmentRecords.settlementPeriod, period)
      ));
    
    result.dbRecords = dbData.length;
    result.dbTotal = dbData.reduce((sum, r) => sum + Math.abs(Number(r.volume)), 0);
    
    // Basic count match
    result.match = result.apiRecords === result.dbRecords;
    
    // Detailed verification if both have data
    if (result.apiRecords > 0 && result.dbRecords > 0) {
      // Check volume match (within small tolerance for rounding)
      const volumeDiff = Math.abs(result.apiTotal - result.dbTotal);
      if (volumeDiff > 0.01) {
        result.match = false;
        result.discrepancies.push(`Volume mismatch: API=${result.apiTotal.toFixed(3)}, DB=${result.dbTotal.toFixed(3)}`);
      }
      
      // Check individual records
      const apiFarmIds = new Set(apiData.map(r => r.id));
      const dbFarmIds = new Set(dbData.map(r => r.farmId));
      
      // Missing in DB
      const missingInDb = [...apiFarmIds].filter(id => !dbFarmIds.has(id));
      if (missingInDb.length > 0) {
        result.match = false;
        result.discrepancies.push(`Missing in DB: ${missingInDb.join(', ')}`);
      }
      
      // Extra in DB
      const extraInDb = [...dbFarmIds].filter(id => !apiFarmIds.has(id));
      if (extraInDb.length > 0) {
        result.match = false;
        result.discrepancies.push(`Extra in DB: ${extraInDb.join(', ')}`);
      }
      
      // Check individual record details for common farms
      for (const apiRecord of apiData) {
        const dbRecord = dbData.find(r => r.farmId === apiRecord.id);
        if (dbRecord) {
          const apiVol = Math.abs(apiRecord.volume);
          const dbVol = Math.abs(Number(dbRecord.volume));
          if (Math.abs(apiVol - dbVol) > 0.001) {
            result.match = false;
            result.discrepancies.push(`${apiRecord.id}: Volume API=${apiVol}, DB=${dbVol}`);
          }
        }
      }
    }
    
    // Add delay to respect API rate limits
    await new Promise(resolve => setTimeout(resolve, 150));
    
  } catch (error) {
    result.discrepancies.push(`Error: ${error.message}`);
    result.match = false;
  }
  
  return result;
}

async function comprehensiveVerification() {
  console.log(`=== Comprehensive Verification: July 3, 2025 ===`);
  console.log(`Checking all 48 settlement periods against Elexon API...`);
  
  const results: VerificationResult[] = [];
  let totalApiRecords = 0;
  let totalDbRecords = 0;
  let matchingPeriods = 0;
  let periodsWithData = 0;
  let periodsWithDiscrepancies = 0;
  
  // Check every period
  for (let period = 1; period <= 48; period++) {
    process.stdout.write(`\rPeriod ${period.toString().padStart(2, '0')}/48...`);
    
    const result = await verifyPeriod(period);
    results.push(result);
    
    totalApiRecords += result.apiRecords;
    totalDbRecords += result.dbRecords;
    
    if (result.match) {
      matchingPeriods++;
    }
    
    if (result.apiRecords > 0 || result.dbRecords > 0) {
      periodsWithData++;
    }
    
    if (result.discrepancies.length > 0) {
      periodsWithDiscrepancies++;
    }
  }
  
  console.log(`\n\n=== Verification Complete ===`);
  console.log(`Total API records: ${totalApiRecords}`);
  console.log(`Total DB records: ${totalDbRecords}`);
  console.log(`Periods with data: ${periodsWithData}/48`);
  console.log(`Matching periods: ${matchingPeriods}/${periodsWithData}`);
  console.log(`Periods with discrepancies: ${periodsWithDiscrepancies}`);
  
  // Show periods with data
  const periodsWithDataList = results.filter(r => r.apiRecords > 0 || r.dbRecords > 0);
  console.log(`\nPeriods with curtailment data: ${periodsWithDataList.map(r => r.period).join(', ')}`);
  
  // Show discrepancies
  if (periodsWithDiscrepancies > 0) {
    console.log(`\n=== DISCREPANCIES FOUND ===`);
    for (const result of results.filter(r => r.discrepancies.length > 0)) {
      console.log(`\nPeriod ${result.period}:`);
      console.log(`  API: ${result.apiRecords} records, ${result.apiTotal.toFixed(2)} MWh`);
      console.log(`  DB:  ${result.dbRecords} records, ${result.dbTotal.toFixed(2)} MWh`);
      console.log(`  Issues:`);
      for (const issue of result.discrepancies) {
        console.log(`    - ${issue}`);
      }
    }
  } else {
    console.log(`\n✅ NO DISCREPANCIES FOUND`);
    console.log(`All ${periodsWithData} periods with data match perfectly between API and database.`);
  }
  
  // Show periods without data for confirmation
  const periodsWithoutData = results.filter(r => r.apiRecords === 0 && r.dbRecords === 0);
  console.log(`\nPeriods without curtailment data (expected): ${periodsWithoutData.map(r => r.period).join(', ')}`);
  
  return {
    totalApiRecords,
    totalDbRecords,
    matchingPeriods,
    periodsWithData,
    periodsWithDiscrepancies,
    results,
    isDataComplete: periodsWithDiscrepancies === 0 && totalApiRecords === totalDbRecords
  };
}

async function main() {
  try {
    const verification = await comprehensiveVerification();
    
    if (verification.isDataComplete) {
      console.log(`\n🎯 VERIFICATION PASSED`);
      console.log(`July 3, 2025 data is completely accurate and matches Elexon API.`);
    } else {
      console.log(`\n❌ VERIFICATION FAILED`);
      console.log(`Discrepancies found that need to be addressed.`);
    }
    
    process.exit(verification.isDataComplete ? 0 : 1);
    
  } catch (error) {
    console.error('❌ Verification failed:', error);
    process.exit(1);
  }
}

main();
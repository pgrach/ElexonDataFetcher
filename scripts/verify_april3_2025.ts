/**
 * Verification script for 2025-04-03 data
 * 
 * This script compares the original API data with what's stored in the database
 * to ensure complete accuracy of the reprocessing
 */

import { db } from "../db";
import { curtailmentRecords } from "../db/schema";
import { fetchBidsOffers } from "../server/services/elexon";
import { eq, sql } from "drizzle-orm";

// Target date for verification
const TARGET_DATE = "2025-04-03";

// Function to fetch all periods from Elexon API
async function fetchAllPeriodsFromAPI(date: string): Promise<any[]> {
  console.log(`\nFetching all periods from Elexon API for ${date}...`);
  const allRecords: any[] = [];
  
  for (let period = 1; period <= 48; period++) {
    try {
      console.log(`Fetching period ${period}...`);
      const records = await fetchBidsOffers(date, period);
      
      if (records && records.length > 0) {
        console.log(`Period ${period}: Found ${records.length} records`);
        allRecords.push(...records.map(r => ({
          ...r,
          settlementPeriod: period,
          apiVolume: Math.abs(Number(r.volume)),
          apiPayment: Math.abs(Number(r.volume)) * Number(r.originalPrice) * -1
        })));
      } else {
        console.log(`Period ${period}: No records found`);
      }
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
    }
  }
  
  console.log(`Completed API fetch. Found ${allRecords.length} total records across all periods.`);
  return allRecords;
}

// Function to get database records
async function getDBRecords(date: string): Promise<any[]> {
  console.log(`\nFetching database records for ${date}...`);
  
  const records = await db
    .select({
      settlementPeriod: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      leadPartyName: curtailmentRecords.leadPartyName,
      volume: curtailmentRecords.volume,
      payment: curtailmentRecords.payment
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  console.log(`Found ${records.length} records in database for ${date}`);
  return records.map(r => ({
    ...r,
    dbVolume: Math.abs(Number(r.volume)),
    dbPayment: Number(r.payment)
  }));
}

// Main verification function
async function verifyData() {
  try {
    console.log("===== VERIFICATION OF 2025-04-03 DATA =====");
    
    // Step 1: Fetch API data directly from Elexon
    const apiRecords = await fetchAllPeriodsFromAPI(TARGET_DATE);
    
    // Step 2: Fetch DB records
    const dbRecords = await getDBRecords(TARGET_DATE);
    
    // Step 3: Verify top-level metrics
    console.log("\n=== Summary Verification ===");
    
    const apiTotalVolume = apiRecords.reduce((sum, r) => sum + r.apiVolume, 0);
    const apiTotalPayment = apiRecords.reduce((sum, r) => sum + r.apiPayment, 0);
    const apiPeriods = new Set(apiRecords.map(r => r.settlementPeriod)).size;
    
    const dbTotalVolume = dbRecords.reduce((sum, r) => sum + r.dbVolume, 0);
    const dbTotalPayment = dbRecords.reduce((sum, r) => sum + r.dbPayment, 0);
    const dbPeriods = new Set(dbRecords.map(r => r.settlementPeriod)).size;
    
    console.log(`API Records: ${apiRecords.length}, DB Records: ${dbRecords.length}`);
    console.log(`API Periods: ${apiPeriods}, DB Periods: ${dbPeriods}`);
    console.log(`API Volume: ${apiTotalVolume.toFixed(2)} MWh, DB Volume: ${dbTotalVolume.toFixed(2)} MWh`);
    console.log(`API Payment: £${apiTotalPayment.toFixed(2)}, DB Payment: £${dbTotalPayment.toFixed(2)}`);
    
    const volumeDiff = Math.abs(apiTotalVolume - dbTotalVolume);
    const paymentDiff = Math.abs(apiTotalPayment - dbTotalPayment);
    
    console.log(`\nDifferences:`);
    console.log(`Volume Difference: ${volumeDiff.toFixed(2)} MWh (${(volumeDiff / apiTotalVolume * 100).toFixed(2)}%)`);
    console.log(`Payment Difference: £${paymentDiff.toFixed(2)} (${(paymentDiff / Math.abs(apiTotalPayment) * 100).toFixed(2)}%)`);
    
    // Step 4: Verify period-by-period
    console.log("\n=== Period-by-Period Verification ===");
    
    const apiPeriodTotals = new Map<number, { volume: number, payment: number, count: number }>();
    for (const record of apiRecords) {
      const period = record.settlementPeriod;
      if (!apiPeriodTotals.has(period)) {
        apiPeriodTotals.set(period, { volume: 0, payment: 0, count: 0 });
      }
      const current = apiPeriodTotals.get(period)!;
      current.volume += record.apiVolume;
      current.payment += record.apiPayment;
      current.count += 1;
      apiPeriodTotals.set(period, current);
    }
    
    const dbPeriodTotals = new Map<number, { volume: number, payment: number, count: number }>();
    for (const record of dbRecords) {
      const period = record.settlementPeriod;
      if (!dbPeriodTotals.has(period)) {
        dbPeriodTotals.set(period, { volume: 0, payment: 0, count: 0 });
      }
      const current = dbPeriodTotals.get(period)!;
      current.volume += record.dbVolume;
      current.payment += record.dbPayment;
      current.count += 1;
      dbPeriodTotals.set(period, current);
    }
    
    // Sort periods numerically
    const allPeriods = Array.from(new Set([
      ...Array.from(apiPeriodTotals.keys()),
      ...Array.from(dbPeriodTotals.keys())
    ])).sort((a, b) => a - b);
    
    for (const period of allPeriods) {
      const apiTotals = apiPeriodTotals.get(period);
      const dbTotals = dbPeriodTotals.get(period);
      
      if (apiTotals && dbTotals) {
        const volumeDiff = Math.abs(apiTotals.volume - dbTotals.volume);
        const paymentDiff = Math.abs(apiTotals.payment - dbTotals.payment);
        const volumeDiffPercent = (volumeDiff / apiTotals.volume * 100).toFixed(2);
        const paymentDiffPercent = (paymentDiff / Math.abs(apiTotals.payment) * 100).toFixed(2);
        
        console.log(`Period ${period}:`);
        console.log(`  API: ${apiTotals.count} records, ${apiTotals.volume.toFixed(2)} MWh, £${apiTotals.payment.toFixed(2)}`);
        console.log(`  DB:  ${dbTotals.count} records, ${dbTotals.volume.toFixed(2)} MWh, £${dbTotals.payment.toFixed(2)}`);
        
        if (volumeDiff > 0.01 || paymentDiff > 0.01) {
          console.log(`  DISCREPANCY: Volume diff: ${volumeDiff.toFixed(2)} MWh (${volumeDiffPercent}%), Payment diff: £${paymentDiff.toFixed(2)} (${paymentDiffPercent}%)`);
        } else {
          console.log(`  ✓ Data matches`);
        }
      } else if (apiTotals) {
        console.log(`Period ${period}: MISSING IN DATABASE. API has ${apiTotals.count} records, ${apiTotals.volume.toFixed(2)} MWh`);
      } else if (dbTotals) {
        console.log(`Period ${period}: MISSING IN API. DB has ${dbTotals.count} records, ${dbTotals.volume.toFixed(2)} MWh`);
      }
      
      console.log("");
    }
    
    // Step 5: Check for farms in API but not in DB
    console.log("\n=== Farm-Level Verification ===");
    
    const apiFarmTotals = new Map<string, { volume: number, count: number }>();
    for (const record of apiRecords) {
      const farm = record.id;
      if (!apiFarmTotals.has(farm)) {
        apiFarmTotals.set(farm, { volume: 0, count: 0 });
      }
      const current = apiFarmTotals.get(farm)!;
      current.volume += record.apiVolume;
      current.count += 1;
      apiFarmTotals.set(farm, current);
    }
    
    const dbFarmTotals = new Map<string, { volume: number, count: number }>();
    for (const record of dbRecords) {
      const farm = record.farmId;
      if (!dbFarmTotals.has(farm)) {
        dbFarmTotals.set(farm, { volume: 0, count: 0 });
      }
      const current = dbFarmTotals.get(farm)!;
      current.volume += record.dbVolume;
      current.count += 1;
      dbFarmTotals.set(farm, current);
    }
    
    // Find farms in API but not in DB (or with volume discrepancies)
    for (const [farm, apiTotals] of apiFarmTotals.entries()) {
      const dbTotals = dbFarmTotals.get(farm);
      
      if (!dbTotals) {
        console.log(`Farm ${farm}: MISSING IN DATABASE. API has ${apiTotals.count} records, ${apiTotals.volume.toFixed(2)} MWh`);
      } else {
        const volumeDiff = Math.abs(apiTotals.volume - dbTotals.volume);
        if (volumeDiff > 0.01) {
          const volumeDiffPercent = (volumeDiff / apiTotals.volume * 100).toFixed(2);
          console.log(`Farm ${farm}:`);
          console.log(`  API: ${apiTotals.count} records, ${apiTotals.volume.toFixed(2)} MWh`);
          console.log(`  DB:  ${dbTotals.count} records, ${dbTotals.volume.toFixed(2)} MWh`);
          console.log(`  DISCREPANCY: Volume diff: ${volumeDiff.toFixed(2)} MWh (${volumeDiffPercent}%)`);
        }
      }
    }
    
    // Find farms in DB but not in API
    for (const [farm, dbTotals] of dbFarmTotals.entries()) {
      const apiTotals = apiFarmTotals.get(farm);
      
      if (!apiTotals) {
        console.log(`Farm ${farm}: MISSING IN API. DB has ${dbTotals.count} records, ${dbTotals.volume.toFixed(2)} MWh`);
      }
    }
    
    console.log("\n===== VERIFICATION COMPLETE =====");
    
  } catch (error) {
    console.error("ERROR DURING VERIFICATION:", error);
    process.exit(1);
  }
}

// Execute the verification
verifyData();
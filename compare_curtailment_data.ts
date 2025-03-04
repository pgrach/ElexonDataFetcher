/**
 * Compare Curtailment Data Script
 * 
 * This script compares curtailment records in the database with data directly from the Elexon API
 * to identify any discrepancies for a specific date and period.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { eq, and } from "drizzle-orm";

const TARGET_DATE = "2025-03-02";
const TARGET_PERIOD = 18;

async function compareCurtailmentData() {
  console.log(`Comparing curtailment data for ${TARGET_DATE} period ${TARGET_PERIOD}`);
  
  try {
    // 1. Fetch data from database
    console.log("Fetching data from database...");
    const dbRecords = await db
      .select({
        farmId: curtailmentRecords.farmId,
        volume: curtailmentRecords.volume,
        payment: curtailmentRecords.payment,
        leadPartyName: curtailmentRecords.leadPartyName
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, TARGET_DATE),
          eq(curtailmentRecords.settlementPeriod, TARGET_PERIOD)
        )
      );
    
    console.log(`Found ${dbRecords.length} records in database for ${TARGET_DATE} period ${TARGET_PERIOD}`);
    
    // 2. Calculate totals from database
    const dbTotalVolume = dbRecords.reduce((sum, record) => sum + Math.abs(Number(record.volume)), 0);
    const dbTotalPayment = dbRecords.reduce((sum, record) => sum + Number(record.payment), 0);
    
    console.log(`Database totals: ${dbTotalVolume.toFixed(2)} MWh, £${dbTotalPayment.toFixed(2)}`);
    
    // 3. Fetch data from Elexon API
    console.log("Fetching data from Elexon API...");
    const apiRecords = await fetchBidsOffers(TARGET_DATE, TARGET_PERIOD);
    
    console.log(`Found ${apiRecords.length} records from Elexon API for ${TARGET_DATE} period ${TARGET_PERIOD}`);
    
    // 4. Calculate totals from API
    const apiTotalVolume = apiRecords.reduce((sum, record) => sum + Math.abs(Number(record.volume)), 0);
    const apiTotalPayment = apiRecords.reduce((sum, record) => sum + (Math.abs(Number(record.volume)) * record.originalPrice * -1), 0);
    
    console.log(`API totals: ${apiTotalVolume.toFixed(2)} MWh, £${apiTotalPayment.toFixed(2)}`);
    
    // 5. Compare totals
    const volumeDiff = Math.abs(dbTotalVolume - apiTotalVolume);
    const paymentDiff = Math.abs(dbTotalPayment - apiTotalPayment);
    
    console.log(`Discrepancies:\n - Volume: ${volumeDiff.toFixed(2)} MWh\n - Payment: £${paymentDiff.toFixed(2)}`);
    
    // 6. Farm-level comparison
    console.log("\nFarm-level comparison:");
    
    // Create a map of farm IDs to API records
    const apiRecordsMap = new Map();
    apiRecords.forEach(record => {
      const farmId = record.id;
      if (!apiRecordsMap.has(farmId)) {
        apiRecordsMap.set(farmId, {
          volume: Math.abs(Number(record.volume)),
          payment: Math.abs(Number(record.volume)) * record.originalPrice * -1
        });
      } else {
        const existing = apiRecordsMap.get(farmId);
        apiRecordsMap.set(farmId, {
          volume: existing.volume + Math.abs(Number(record.volume)),
          payment: existing.payment + (Math.abs(Number(record.volume)) * record.originalPrice * -1)
        });
      }
    });
    
    // Create a map of farm IDs to DB records
    const dbRecordsMap = new Map();
    dbRecords.forEach(record => {
      const farmId = record.farmId;
      if (!dbRecordsMap.has(farmId)) {
        dbRecordsMap.set(farmId, {
          volume: Math.abs(Number(record.volume)),
          payment: Number(record.payment)
        });
      } else {
        const existing = dbRecordsMap.get(farmId);
        dbRecordsMap.set(farmId, {
          volume: existing.volume + Math.abs(Number(record.volume)),
          payment: existing.payment + Number(record.payment)
        });
      }
    });
    
    // Compare each farm's data
    const allFarmIds = new Set([...dbRecordsMap.keys(), ...apiRecordsMap.keys()]);
    
    const farmDiscrepancies: any[] = [];
    
    allFarmIds.forEach(farmId => {
      const dbData = dbRecordsMap.get(farmId) || { volume: 0, payment: 0 };
      const apiData = apiRecordsMap.get(farmId) || { volume: 0, payment: 0 };
      
      const volumeDiff = Math.abs(dbData.volume - apiData.volume);
      const paymentDiff = Math.abs(dbData.payment - apiData.payment);
      
      if (volumeDiff > 0.01 || paymentDiff > 0.01) {
        farmDiscrepancies.push({
          farmId,
          dbVolume: dbData.volume,
          apiVolume: apiData.volume,
          volumeDiff,
          dbPayment: dbData.payment,
          apiPayment: apiData.payment,
          paymentDiff
        });
      }
    });
    
    // Sort discrepancies by volume difference (largest first)
    farmDiscrepancies.sort((a, b) => b.volumeDiff - a.volumeDiff);
    
    if (farmDiscrepancies.length > 0) {
      console.log("Farms with discrepancies:");
      farmDiscrepancies.forEach(disc => {
        console.log(`${disc.farmId}:
  - Volume: DB ${disc.dbVolume.toFixed(2)} MWh vs API ${disc.apiVolume.toFixed(2)} MWh (diff: ${disc.volumeDiff.toFixed(2)} MWh)
  - Payment: DB £${disc.dbPayment.toFixed(2)} vs API £${disc.apiPayment.toFixed(2)} (diff: £${disc.paymentDiff.toFixed(2)})`);
      });
    } else {
      console.log("No farm-level discrepancies found!");
    }
    
  } catch (error) {
    console.error("Error comparing curtailment data:", error);
  }
}

// Run the comparison
compareCurtailmentData();
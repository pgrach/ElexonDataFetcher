/**
 * Quick verification script for 2025-04-03 data
 * 
 * This script only checks the periods we know have data
 */

import { db } from "../db";
import { curtailmentRecords } from "../db/schema";
import { eq, sql } from "drizzle-orm";

// Target date for verification
const TARGET_DATE = "2025-04-03";

// Main verification function
async function verifyData() {
  try {
    console.log("===== VERIFICATION OF 2025-04-03 DATA =====");
    
    // Step 1: Get period-by-period breakdown from the database
    const dbPeriodSummary = await db
      .select({
        settlementPeriod: curtailmentRecords.settlementPeriod,
        recordCount: sql<number>`COUNT(*)::int`,
        totalVolume: sql<string>`SUM(ABS(volume))::text`,
        totalPayment: sql<string>`SUM(payment)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    // Step 2: Get farm-level breakdown from the database
    const dbFarmSummary = await db
      .select({
        farmId: curtailmentRecords.farmId,
        recordCount: sql<number>`COUNT(*)::int`,
        totalVolume: sql<string>`SUM(ABS(volume))::text`,
        totalPayment: sql<string>`SUM(payment)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.farmId)
      .orderBy(sql<string>`SUM(ABS(volume))::numeric DESC`);
    
    // Step 3: Compare with known values from logs
    console.log("\n=== Period-by-Period Summary ===");
    console.log("Settlement Period | Records | Volume (MWh) | Payment (£)");
    console.log("--------------------------------------------------");
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const period of dbPeriodSummary) {
      console.log(`Period ${period.settlementPeriod.toString().padStart(2, '0')} | ${period.recordCount.toString().padStart(7, ' ')} | ${Number(period.totalVolume).toFixed(2).padStart(11, ' ')} | ${Number(period.totalPayment).toFixed(2).padStart(11, ' ')}`);
      totalRecords += period.recordCount;
      totalVolume += Number(period.totalVolume);
      totalPayment += Number(period.totalPayment);
    }
    
    console.log("--------------------------------------------------");
    console.log(`TOTAL           | ${totalRecords.toString().padStart(7, ' ')} | ${totalVolume.toFixed(2).padStart(11, ' ')} | ${totalPayment.toFixed(2).padStart(11, ' ')}`);
    
    // Step 4: Print farm summary
    console.log("\n=== Farm-Level Summary ===");
    console.log("Farm ID       | Records | Volume (MWh) | Payment (£)");
    console.log("--------------------------------------------------");
    
    for (const farm of dbFarmSummary) {
      console.log(`${farm.farmId.padEnd(12, ' ')} | ${farm.recordCount.toString().padStart(7, ' ')} | ${Number(farm.totalVolume).toFixed(2).padStart(11, ' ')} | ${Number(farm.totalPayment).toFixed(2).padStart(11, ' ')}`);
    }
    
    // Step 5: Compare with known API log values
    console.log("\n=== API Log vs Database Comparison ===");
    console.log("This compares the log output vs what's in the database");
    
    const apiLogPeriods = [
      { period: 35, records: 5, volume: 41.68, payment: 525.85 },
      { period: 36, records: 9, volume: 162.45, payment: 2066.72 },
      { period: 37, records: 10, volume: 147.99, payment: 1864.72 },
      { period: 38, records: 9, volume: 128.00, payment: 1589.69 },
      { period: 39, records: 9, volume: 117.42, payment: 1454.79 },
      { period: 40, records: 2, volume: 23.65, payment: 293.02 },
      { period: 45, records: 4, volume: 84.66, payment: 1368.93 },
      { period: 46, records: 8, volume: 128.04, payment: 2070.43 },
      { period: 47, records: 10, volume: 178.15, payment: 2880.69 },
      { period: 48, records: 15, volume: 213.57, payment: 3407.73 }
    ];
    
    console.log("Period | API Records | DB Records | API Volume | DB Volume | API Payment | DB Payment | Match?");
    console.log("----------------------------------------------------------------------------------------");
    
    for (const apiPeriod of apiLogPeriods) {
      const dbPeriod = dbPeriodSummary.find(p => p.settlementPeriod === apiPeriod.period);
      
      if (dbPeriod) {
        const volumeMatch = Math.abs(apiPeriod.volume - Number(dbPeriod.totalVolume)) < 0.1;
        const paymentMatch = Math.abs(Math.abs(apiPeriod.payment) - Math.abs(Number(dbPeriod.totalPayment))) < 0.1;
        const recordsMatch = apiPeriod.records === dbPeriod.recordCount || 
                             // Log message includes all records, but some might be filtered out for saving
                             (apiPeriod.records >= dbPeriod.recordCount && dbPeriod.recordCount > 0);
        
        const matchStatus = (volumeMatch && paymentMatch && recordsMatch) ? "✓" : "✗";
        
        console.log(`${apiPeriod.period.toString().padStart(2, '0')}    | ${apiPeriod.records.toString().padStart(11, ' ')} | ${dbPeriod.recordCount.toString().padStart(10, ' ')} | ${apiPeriod.volume.toFixed(2).padStart(10, ' ')} | ${Number(dbPeriod.totalVolume).toFixed(2).padStart(9, ' ')} | ${apiPeriod.payment.toFixed(2).padStart(11, ' ')} | ${Number(dbPeriod.totalPayment).toFixed(2).padStart(10, ' ')} | ${matchStatus}`);
        
        if (!volumeMatch || !paymentMatch || !recordsMatch) {
          console.log(`       Volume diff: ${Math.abs(apiPeriod.volume - Number(dbPeriod.totalVolume)).toFixed(2)} MWh`);
          console.log(`       Payment diff: £${Math.abs(Math.abs(apiPeriod.payment) - Math.abs(Number(dbPeriod.totalPayment))).toFixed(2)}`);
        }
      } else {
        console.log(`${apiPeriod.period.toString().padStart(2, '0')}    | ${apiPeriod.records.toString().padStart(11, ' ')} | ${' '.padStart(10, ' ')} | ${apiPeriod.volume.toFixed(2).padStart(10, ' ')} | ${'0.00'.padStart(9, ' ')} | ${apiPeriod.payment.toFixed(2).padStart(11, ' ')} | ${'0.00'.padStart(10, ' ')} | ✗ MISSING`);
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
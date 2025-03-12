#!/usr/bin/env tsx
/**
 * Final period check for 2025-03-11
 * 
 * This script specifically verifies that all 48 periods are present
 * and lists them all to confirm visually.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, count, sql } from "drizzle-orm";

const DATE = "2025-03-11";

async function main() {
  console.log(`\nVerifying all periods for ${DATE}...\n`);
  
  // Get data for all periods
  const periodStats = await db
    .select({
      period: curtailmentRecords.settlementPeriod,
      recordCount: count(),
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
  
  console.log("Period | Records | Volume (MWh) | Payment (£)");
  console.log("-------|---------|-------------|------------");
  
  const periodMap = new Map<number, boolean>();
  let totalRecords = 0;
  let totalVolume = 0;
  let totalPayment = 0;
  
  periodStats.forEach(period => {
    const periodNum = period.period;
    const recordCount = period.recordCount;
    const volume = Number(period.totalVolume);
    const payment = Number(period.totalPayment);
    
    console.log(
      `${periodNum.toString().padStart(6)} | ` +
      `${recordCount.toString().padStart(7)} | ` +
      `${volume.toFixed(2).padStart(11)} | ` +
      `${payment.toFixed(2).padStart(10)}`
    );
    
    periodMap.set(periodNum, true);
    totalRecords += recordCount;
    totalVolume += volume;
    totalPayment += payment;
  });
  
  // Check for missing periods
  const missingPeriods = [];
  for (let i = 1; i <= 48; i++) {
    if (!periodMap.has(i)) {
      missingPeriods.push(i);
    }
  }
  
  console.log("\n=== Summary ===");
  console.log(`Total periods found: ${periodStats.length} of 48`);
  console.log(`Total records: ${totalRecords}`);
  console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total payment: £${totalPayment.toFixed(2)}`);
  
  if (missingPeriods.length > 0) {
    console.log(`\n⚠️ Missing periods: ${missingPeriods.join(', ')}`);
  } else {
    console.log("\n✅ All 48 periods are present in the database!");
  }
  
  // Verify daily summary matches
  const dailySummary = await db
    .select({
      totalCurtailedEnergy: sql<string>`total_curtailed_energy::numeric`,
      totalPayment: sql<string>`total_payment::numeric`
    })
    .from(sql`daily_summaries`)
    .where(sql`summary_date = ${DATE}`)
    .limit(1);
  
  console.log("\n=== Daily Summary ===");
  
  if (dailySummary && dailySummary.length > 0) {
    const summaryEnergy = Number(dailySummary[0].totalCurtailedEnergy);
    const summaryPayment = Number(dailySummary[0].totalPayment);
    
    console.log(`Total Energy: ${summaryEnergy.toFixed(2)} MWh`);
    console.log(`Total Payment: £${summaryPayment.toFixed(2)}`);
    
    // Check if totals match
    const energyMatch = Math.abs(summaryEnergy - totalVolume) < 0.1;
    const paymentMatch = Math.abs(summaryPayment - totalPayment) < 0.1;
    
    if (energyMatch && paymentMatch) {
      console.log("✅ Daily summary matches the sum of all periods");
    } else {
      console.log("❌ Daily summary does not match the sum of all periods");
      if (!energyMatch) {
        console.log(`   Energy difference: ${Math.abs(summaryEnergy - totalVolume).toFixed(2)} MWh`);
      }
      if (!paymentMatch) {
        console.log(`   Payment difference: £${Math.abs(summaryPayment - totalPayment).toFixed(2)}`);
      }
    }
  } else {
    console.log("No daily summary found for this date");
  }
}

main().catch(console.error);
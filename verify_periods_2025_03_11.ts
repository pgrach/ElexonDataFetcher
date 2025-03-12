#!/usr/bin/env tsx
/**
 * Verify 1:00 and 3:00 periods (3, 4, 7, 8) for 2025-03-11
 * 
 * This script provides a simple verification of the periods we fixed
 * to ensure they now contain the correct data.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, sql, count, inArray, and } from "drizzle-orm";

const DATE = "2025-03-11";
const PERIODS_TO_CHECK = [3, 4, 7, 8]; // 1:00 and 3:00 periods

async function main() {
  console.log(`\nVerifying periods ${PERIODS_TO_CHECK.join(', ')} for ${DATE}...\n`);
  
  // Get current state of these periods
  const periodStats = await db
    .select({
      period: curtailmentRecords.settlementPeriod,
      recordCount: count(),
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, DATE),
        inArray(curtailmentRecords.settlementPeriod, PERIODS_TO_CHECK)
      )
    )
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
  
  console.log("Period | Records | Volume (MWh) | Payment (£)");
  console.log("-------|---------|-------------|------------");
  
  // Expected values from the Elexon API
  const expected = {
    3: { count: 15, volume: 453.07 },
    4: { count: 18, volume: 389.10 },
    7: { count: 10, volume: 339.13 },
    8: { count: 9, volume: 336.97 }
  };
  
  let allMatch = true;
  
  periodStats.forEach(period => {
    const periodNum = period.period;
    const recordCount = period.recordCount;
    const volume = Number(period.totalVolume);
    
    // Check if values match expected
    const expectedData = expected[periodNum as keyof typeof expected];
    const countMatch = recordCount === expectedData.count;
    const volumeMatch = Math.abs(volume - expectedData.volume) < 0.1; // Allow for rounding differences
    
    console.log(
      `${periodNum.toString().padStart(6)} | ` +
      `${recordCount.toString().padStart(7)} | ` +
      `${volume.toFixed(2).padStart(11)} | ` +
      `${Number(period.totalPayment).toFixed(2).padStart(10)} | ` +
      `${countMatch && volumeMatch ? '✅' : '❌'}`
    );
    
    if (!countMatch || !volumeMatch) {
      allMatch = false;
    }
  });
  
  // Also check if we have all the periods we expect
  const missingPeriods = PERIODS_TO_CHECK.filter(
    p => !periodStats.some(stat => stat.period === p)
  );
  
  if (missingPeriods.length > 0) {
    console.log(`\n⚠️ Missing periods: ${missingPeriods.join(', ')}`);
    allMatch = false;
  }
  
  console.log(`\n=== Summary ===`);
  if (allMatch) {
    console.log("✅ All periods match expected values from Elexon API");
  } else {
    console.log("❌ Some periods do not match expected values");
  }
  
  // Check daily summary to ensure it's updated
  const dailySummary = await db
    .select({
      totalCurtailedEnergy: dailySummaries.totalCurtailedEnergy,
      totalPayment: dailySummaries.totalPayment
    })
    .from(dailySummaries)
    .where(eq(dailySummaries.summaryDate, DATE))
    .limit(1);
  
  console.log(`\n=== Daily Summary ===`);
  if (dailySummary && dailySummary.length > 0) {
    console.log(`Total Energy: ${Number(dailySummary[0].totalCurtailedEnergy).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(dailySummary[0].totalPayment).toFixed(2)}`);
  } else {
    console.log("No daily summary found for this date");
  }
}

main().catch(console.error);
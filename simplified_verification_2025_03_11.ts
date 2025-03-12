#!/usr/bin/env tsx
/**
 * Simplified Verification for 2025-03-11 Data
 * 
 * This script does a quick check to confirm we've successfully
 * fixed periods 11 and 12 for 2025-03-11.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, sql, count } from "drizzle-orm";

const DATE = "2025-03-11";

async function main() {
  console.log(`\n=== DATA VERIFICATION FOR ${DATE} ===\n`);
  
  // Check periods
  const periodCount = await db
    .select({
      count: sql<number>`COUNT(DISTINCT settlement_period)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE));
  
  const isComplete = (periodCount[0]?.count || 0) === 48;
  console.log(`Periods found: ${periodCount[0]?.count || 0}/48 ${isComplete ? '✅' : '❌'}`);
  
  // Check periods 11 and 12 specifically
  const specificPeriods = await db
    .select({
      period: curtailmentRecords.settlementPeriod,
      recordCount: count(),
      volume: sql<string>`SUM(ABS(volume::numeric))`,
      payment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(
      eq(curtailmentRecords.settlementDate, DATE) && 
      sql`settlement_period IN (11, 12)`
    )
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
  
  if (specificPeriods.length === 2) {
    console.log(`\nPreviously missing periods:`);
    specificPeriods.forEach(period => {
      console.log(`- Period ${period.period}: ${period.recordCount} records, ${Number(period.volume).toFixed(2)} MWh, £${Number(period.payment).toFixed(2)}`);
    });
    console.log(`\nStatus: ✅ FIXED - Both periods now present`);
  } else {
    console.log(`\nStatus: ❌ ERROR - Expected 2 periods, found ${specificPeriods.length}`);
  }
  
  // Check daily summary
  const dailySummaryData = await db.query.dailySummaries.findFirst({
    where: eq(dailySummaries.summaryDate, DATE)
  });
  
  // Check curtailment record totals
  const curtailmentTotal = await db
    .select({
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE));
  
  console.log(`\nDaily Summary: ${Number(dailySummaryData?.totalCurtailedEnergy || 0).toFixed(2)} MWh, £${Number(dailySummaryData?.totalPayment || 0).toFixed(2)}`);
  console.log(`Records Total: ${Number(curtailmentTotal[0]?.totalVolume || 0).toFixed(2)} MWh, £${Number(curtailmentTotal[0]?.totalPayment || 0).toFixed(2)}`);
  
  const summaryMatch = 
    Number(dailySummaryData?.totalCurtailedEnergy || 0).toFixed(2) === 
    Number(curtailmentTotal[0]?.totalVolume || 0).toFixed(2);
  
  console.log(`\nSummary/Records Match: ${summaryMatch ? '✅ YES' : '❌ NO'}`);
  
  // Handle the case where we have all 48 periods
  const periodsFixed = specificPeriods.length === 2;
  
  console.log(`\n=== SUMMARY ===`);
  if ((periodCount[0]?.count || 0) === 48 && summaryMatch && periodsFixed) {
    console.log(`✅ Fix complete - All periods present with correct totals`);
  } else {
    console.log(`❌ Issues remain - Check details above`);
  }
}

main().catch(console.error);
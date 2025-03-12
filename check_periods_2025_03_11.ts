#!/usr/bin/env tsx
/**
 * Final period check for 2025-03-11
 * 
 * This script specifically verifies that all 48 periods are present
 * and lists them all to confirm visually.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, sql, count } from "drizzle-orm";

const DATE = "2025-03-11";

async function main() {
  console.log(`\nChecking all periods for ${DATE}...\n`);
  
  // Get detailed period stats
  const periodStats = await db
    .select({
      period: curtailmentRecords.settlementPeriod,
      recordCount: count(curtailmentRecords.id),
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
  
  // Display a simplified table of all periods
  console.log("Period | Records | Present");
  console.log("-------|---------|--------");
  
  // Generate a list of all expected periods (1-48)
  const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
  
  // Create a map for quick lookup
  const periodMap = new Map(
    periodStats.map(p => [p.period, { 
      count: p.recordCount,
      volume: p.totalVolume
    }])
  );
  
  // Check each period and display
  let missingPeriods = 0;
  
  for (const period of allPeriods) {
    const present = periodMap.has(period);
    const recordCount = present ? periodMap.get(period)?.count : 0;
    
    console.log(
      `${period.toString().padStart(6)} | ` +
      `${(recordCount || 0).toString().padStart(7)} | ` +
      `${present ? '✅' : '❌'}`
    );
    
    if (!present) {
      missingPeriods++;
    }
  }
  
  // Summary
  console.log(`\nTotal periods: ${periodStats.length} / 48`);
  
  if (missingPeriods === 0) {
    console.log("\n✅ SUCCESS: All 48 periods are present");
  } else {
    console.log(`\n❌ ISSUE: Still missing ${missingPeriods} periods`);
  }
}

main().catch(console.error);
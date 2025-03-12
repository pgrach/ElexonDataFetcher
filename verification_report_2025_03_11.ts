#!/usr/bin/env tsx
/**
 * Verification Report for 2025-03-11 Data Reingestion
 * 
 * This script generates a comprehensive report showing the completeness
 * and consistency of data for 2025-03-11 after reingestion.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, historicalBitcoinCalculations } from "./db/schema";
import { eq, count, sql, inArray, and } from "drizzle-orm";

const DATE = "2025-03-11";
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
const CRITICAL_PERIODS = [3, 4, 7, 8]; // 1:00 and 3:00 periods we fixed

// Helper function to format numbers
function formatNumber(value: number | string | null, decimals: number = 2): string {
  if (value === null) return "N/A";
  return Number(value).toFixed(decimals);
}

// Helper function to format percentages
function formatPercentage(value: number): string {
  return `${(value * 100).toFixed(2)}%`;
}

async function main() {
  console.log(`\n========================================================`);
  console.log(`📊 VERIFICATION REPORT FOR ${DATE}`);
  console.log(`========================================================\n`);
  
  // PART 1: Check Period Coverage
  console.log(`PART 1: SETTLEMENT PERIOD COVERAGE`);
  console.log(`-----------------------------------\n`);
  
  const periodStats = await db
    .select({
      count: count(),
      minPeriod: sql<number>`MIN(settlement_period)`,
      maxPeriod: sql<number>`MAX(settlement_period)`,
      distinctPeriods: sql<number>`COUNT(DISTINCT settlement_period)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE));
  
  console.log(`Period range: ${periodStats[0].minPeriod} to ${periodStats[0].maxPeriod}`);
  console.log(`Total periods: ${periodStats[0].distinctPeriods} of 48 expected`);
  console.log(`Period coverage: ${formatPercentage(periodStats[0].distinctPeriods / 48)}`);
  
  const missingPeriodCount = 48 - Number(periodStats[0].distinctPeriods);
  
  if (missingPeriodCount === 0) {
    console.log(`✅ All 48 settlement periods are present\n`);
  } else {
    console.log(`❌ Missing ${missingPeriodCount} settlement periods\n`);
  }
  
  // PART 2: Critical Period Verification
  console.log(`PART 2: CRITICAL PERIOD VERIFICATION`);
  console.log(`------------------------------------\n`);
  
  console.log(`Checking periods that were specifically fixed (1:00 and 3:00):`);
  
  const criticalPeriodStats = await db
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
        inArray(curtailmentRecords.settlementPeriod, CRITICAL_PERIODS)
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
  
  let allCriticalMatch = true;
  
  criticalPeriodStats.forEach(period => {
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
      `${formatNumber(volume).padStart(11)} | ` +
      `${formatNumber(period.totalPayment).padStart(10)} | ` +
      `${countMatch && volumeMatch ? '✅' : '❌'}`
    );
    
    if (!countMatch || !volumeMatch) {
      allCriticalMatch = false;
    }
  });
  
  if (allCriticalMatch) {
    console.log(`\n✅ All critical periods match expected values from Elexon API\n`);
  } else {
    console.log(`\n❌ Some critical periods do not match expected values\n`);
  }
  
  // PART 3: Daily Summary Verification
  console.log(`PART 3: DAILY SUMMARY VERIFICATION`);
  console.log(`---------------------------------\n`);
  
  // Calculate totals from all periods
  const totals = await db
    .select({
      recordCount: count(),
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE));
  
  // Get daily summary
  const dailySummary = await db
    .select({
      totalCurtailedEnergy: dailySummaries.totalCurtailedEnergy,
      totalPayment: dailySummaries.totalPayment
    })
    .from(dailySummaries)
    .where(eq(dailySummaries.summaryDate, DATE))
    .limit(1);
  
  console.log(`Total records: ${totals[0].recordCount}`);
  console.log(`Total volume from all periods: ${formatNumber(totals[0].totalVolume)} MWh`);
  console.log(`Total payment from all periods: £${formatNumber(totals[0].totalPayment)}`);
  
  if (dailySummary.length > 0) {
    console.log(`\nDaily summary volume: ${formatNumber(dailySummary[0].totalCurtailedEnergy)} MWh`);
    console.log(`Daily summary payment: £${formatNumber(dailySummary[0].totalPayment)}`);
    
    // Check if totals match
    const energyMatch = Math.abs(Number(dailySummary[0].totalCurtailedEnergy) - Number(totals[0].totalVolume)) < 0.1;
    const paymentMatch = Math.abs(Number(dailySummary[0].totalPayment) - Number(totals[0].totalPayment)) < 0.1;
    
    if (energyMatch && paymentMatch) {
      console.log(`\n✅ Daily summary matches the sum of all periods\n`);
    } else {
      console.log(`\n❌ Daily summary does not match the sum of all periods`);
      if (!energyMatch) {
        console.log(`   Energy difference: ${Math.abs(Number(dailySummary[0].totalCurtailedEnergy) - Number(totals[0].totalVolume)).toFixed(2)} MWh`);
      }
      if (!paymentMatch) {
        console.log(`   Payment difference: £${Math.abs(Number(dailySummary[0].totalPayment) - Number(totals[0].totalPayment)).toFixed(2)}`);
      }
      console.log();
    }
  } else {
    console.log(`\n❌ No daily summary found for ${DATE}\n`);
  }
  
  // PART 4: Bitcoin Calculation Verification
  console.log(`PART 4: BITCOIN CALCULATION VERIFICATION`);
  console.log(`---------------------------------------\n`);
  
  let bitcoinCalcValid = true;
  
  for (const minerModel of MINER_MODELS) {
    // Check bitcoin calculations
    const bitcoinStats = await db
      .select({
        count: count(),
        distinctPeriods: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, DATE),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
    
    console.log(`${minerModel}:`);
    console.log(`- Bitcoin calculation records: ${bitcoinStats[0].count}`);
    console.log(`- Periods with calculations: ${bitcoinStats[0].distinctPeriods} of 48`);
    console.log(`- Total Bitcoin: ${formatNumber(bitcoinStats[0].totalBitcoin, 8)} BTC`);
    
    const missingPeriodCount = 48 - bitcoinStats[0].distinctPeriods;
    
    if (missingPeriodCount > 0) {
      console.log(`  ❌ Missing Bitcoin calculations for ${missingPeriodCount} periods`);
      bitcoinCalcValid = false;
    } else {
      console.log(`  ✅ All 48 periods have Bitcoin calculations`);
    }
    
    // Check critical periods specifically
    const criticalPeriodResults: Array<{period: number, count: number}> = [];
    
    for (const period of CRITICAL_PERIODS) {
      const result = await db
        .select({ count: count() })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, DATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel),
            eq(historicalBitcoinCalculations.settlementPeriod, period)
          )
        );
      
      criticalPeriodResults.push({
        period,
        count: result[0].count
      });
    }
    
    // Check if any critical periods are missing calculations
    const missingPeriods = criticalPeriodResults.filter(p => p.count === 0).map(p => p.period);
    
    if (missingPeriods.length === 0) {
      console.log(`  ✅ All critical periods have Bitcoin calculations`);
    } else {
      console.log(`  ❌ Missing Bitcoin calculations for periods: ${missingPeriods.join(', ')}`);
      bitcoinCalcValid = false;
    }
    
    console.log();
  }
  
  // PART 5: Final Summary
  console.log(`PART 5: FINAL SUMMARY`);
  console.log(`--------------------\n`);
  
  const allTests = [
    periodStats[0].distinctPeriods === 48,
    allCriticalMatch,
    dailySummary.length > 0 && 
    Math.abs(Number(dailySummary[0].totalCurtailedEnergy) - Number(totals[0].totalVolume)) < 0.1 &&
    Math.abs(Number(dailySummary[0].totalPayment) - Number(totals[0].totalPayment)) < 0.1,
    bitcoinCalcValid
  ];
  
  const passedTests = allTests.filter(Boolean).length;
  
  console.log(`Verification tests passed: ${passedTests} of ${allTests.length}`);
  console.log(`Overall verification score: ${formatPercentage(passedTests / allTests.length)}`);
  
  if (passedTests === allTests.length) {
    console.log(`\n✅ ALL VERIFICATION TESTS PASSED. DATA FOR ${DATE} IS COMPLETE AND ACCURATE.`);
  } else {
    console.log(`\n⚠️ SOME VERIFICATION TESTS FAILED. REVIEW THE REPORT FOR DETAILS.`);
  }
  
  console.log(`\n========================================================\n`);
}

main().catch(console.error);
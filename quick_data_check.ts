#!/usr/bin/env tsx
/**
 * Quick Data Check Tool
 * 
 * A simplified verification tool to check the integrity and completeness
 * of data for any settlement date. This script focuses on the key metrics:
 * - Period coverage (all 48 settlement periods present)
 * - Daily summary calculations
 * - Bitcoin calculations for all miner models
 * 
 * Usage:
 *   npx tsx quick_data_check.ts [date]
 * 
 * Example:
 *   npx tsx quick_data_check.ts 2025-03-11
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries, historicalBitcoinCalculations } from "./db/schema";
import { eq, count, sql, and, min, max, distinct } from "drizzle-orm";

// Default to yesterday if no date provided
const DEFAULT_DATE = new Date();
DEFAULT_DATE.setDate(DEFAULT_DATE.getDate() - 1);
const DEFAULT_DATE_STR = DEFAULT_DATE.toISOString().split('T')[0];

// Get date from command line or use default
const DATE = process.argv[2] || DEFAULT_DATE_STR;
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

// Helper functions
function formatNumber(value: number | string | null, decimals: number = 2): string {
  if (value === null) return "N/A";
  return Number(value).toFixed(decimals);
}

function formatPercentage(value: number): string {
  return `${(value * 100).toFixed(2)}%`;
}

async function main() {
  console.log(`\n📊 QUICK DATA CHECK FOR ${DATE}\n`);
  
  // Check 1: Period Coverage
  console.log(`1️⃣ CHECKING PERIOD COVERAGE...`);
  
  const periodStats = await db
    .select({
      count: count(),
      minPeriod: sql<number>`MIN(settlement_period)`,
      maxPeriod: sql<number>`MAX(settlement_period)`,
      distinctPeriods: sql<number>`COUNT(DISTINCT settlement_period)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE));
  
  const missingPeriodCount = 48 - Number(periodStats[0].distinctPeriods);
  
  console.log(`- Total periods: ${periodStats[0].distinctPeriods}/48 (${formatPercentage(periodStats[0].distinctPeriods / 48)})`);
  
  if (missingPeriodCount === 0) {
    console.log(`✅ All 48 settlement periods are present\n`);
  } else {
    console.log(`❌ Missing ${missingPeriodCount} settlement periods\n`);
  }
  
  // Check 2: Daily Summary
  console.log(`2️⃣ CHECKING DAILY SUMMARY...`);
  
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
  
  console.log(`- Total records: ${totals[0].recordCount}`);
  console.log(`- Total volume: ${formatNumber(totals[0].totalVolume)} MWh`);
  console.log(`- Total payment: £${formatNumber(totals[0].totalPayment)}`);
  
  if (dailySummary.length > 0) {
    const energyMatch = Math.abs(Number(dailySummary[0].totalCurtailedEnergy) - Number(totals[0].totalVolume)) < 0.1;
    const paymentMatch = Math.abs(Number(dailySummary[0].totalPayment) - Number(totals[0].totalPayment)) < 0.1;
    
    if (energyMatch && paymentMatch) {
      console.log(`✅ Daily summary matches the sum of all periods\n`);
    } else {
      console.log(`❌ Daily summary does not match the sum of all periods\n`);
    }
  } else {
    console.log(`❌ No daily summary found for ${DATE}\n`);
  }
  
  // Check 3: Bitcoin Calculations
  console.log(`3️⃣ CHECKING BITCOIN CALCULATIONS...`);
  
  let allComplete = true;
  
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
    
    const periodCoverage = Number(bitcoinStats[0].distinctPeriods) / 48;
    console.log(`- ${minerModel}: ${bitcoinStats[0].distinctPeriods}/48 periods (${formatPercentage(periodCoverage)})`);
    
    if (bitcoinStats[0].distinctPeriods < 48) {
      allComplete = false;
    }
  }
  
  if (allComplete) {
    console.log(`✅ All miner models have complete Bitcoin calculations\n`);
  } else {
    console.log(`❌ Some miner models are missing Bitcoin calculations\n`);
  }
  
  // Final Summary
  console.log(`SUMMARY FOR ${DATE}:`);
  const allChecks = [
    missingPeriodCount === 0,
    dailySummary.length > 0 && 
    Math.abs(Number(dailySummary[0].totalCurtailedEnergy) - Number(totals[0].totalVolume)) < 0.1 &&
    Math.abs(Number(dailySummary[0].totalPayment) - Number(totals[0].totalPayment)) < 0.1,
    allComplete
  ];
  
  const passedChecks = allChecks.filter(Boolean).length;
  console.log(`- ${passedChecks} of ${allChecks.length} checks passed (${formatPercentage(passedChecks / allChecks.length)})\n`);
  
  if (passedChecks === allChecks.length) {
    console.log(`✅ ALL CHECKS PASSED. DATA FOR ${DATE} IS COMPLETE.\n`);
  } else {
    console.log(`⚠️ SOME CHECKS FAILED. DATA MAY BE INCOMPLETE.\n`);
  }
}

main().catch(console.error);
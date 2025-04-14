/**
 * Verification Script for 2025-04-11
 * 
 * This script doesn't perform reingestion, but instead verifies the existing data
 * for April 11, 2025 in the database to show the current state.
 * 
 * Run with: npx tsx scripts/verify-april11-data.ts
 */

import { db } from '../db';
import { curtailmentRecords, dailySummaries, historicalBitcoinCalculations } from '../db/schema';
import { eq, sql } from 'drizzle-orm';
import { minerModels } from '../server/types/bitcoin';

const TARGET_DATE = '2025-04-11';
const MINER_MODELS = Object.keys(minerModels); // ['S19J_PRO', 'S9', 'M20S']

async function verifyData() {
  console.log(`\n=== Verifying Data for ${TARGET_DATE} ===`);
  
  try {
    // Step 1: Check curtailment records
    const curtailmentStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        farmCount: sql<number>`COUNT(DISTINCT ${curtailmentRecords.farmId})`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nCurtailment Records in Database:`);
    console.log(`Records: ${curtailmentStats[0]?.recordCount || 0}`);
    console.log(`Settlement Periods: ${curtailmentStats[0]?.periodCount || 0}`);
    console.log(`Farms: ${curtailmentStats[0]?.farmCount || 0}`);
    console.log(`Total Volume: ${Number(curtailmentStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Math.abs(Number(curtailmentStats[0]?.totalPayment || 0)).toFixed(2)}`);
    
    // Step 2: Check sample periods with most curtailment
    const topPeriods = await db
      .select({
        settlementPeriod: curtailmentRecords.settlementPeriod,
        recordCount: sql<number>`COUNT(*)`,
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))` as any, 'desc')
      .limit(5);
    
    console.log(`\nTop 5 Settlement Periods by Volume:`);
    for (const period of topPeriods) {
      console.log(`Period ${period.settlementPeriod}: ${Number(period.totalVolume).toFixed(2)} MWh, £${Math.abs(Number(period.totalPayment)).toFixed(2)} (${period.recordCount} records)`);
    }
    
    // Step 3: Check daily summary
    const summary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    console.log(`\nDaily Summary:`);
    if (summary.length > 0) {
      console.log(`Date: ${summary[0].summaryDate}`);
      console.log(`Total Curtailed Energy: ${summary[0].totalCurtailedEnergy} MWh`);
      console.log(`Total Payment: £${Math.abs(Number(summary[0].totalPayment)).toFixed(2)}`);
    } else {
      console.log(`No daily summary found for ${TARGET_DATE}`);
    }
    
    // Step 4: Check Bitcoin calculations if they exist
    const bitcoinStats = await db
      .select({
        minerModel: historicalBitcoinCalculations.minerModel,
        recordCount: sql<number>`COUNT(*)`,
        totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
      .groupBy(historicalBitcoinCalculations.minerModel);
    
    console.log(`\nBitcoin Calculations:`);
    if (bitcoinStats.length > 0) {
      bitcoinStats.forEach(stat => {
        console.log(`${stat.minerModel}: ${Number(stat.totalBitcoin).toFixed(8)} BTC (${stat.recordCount} records)`);
      });
    } else {
      console.log(`No Bitcoin calculations found for ${TARGET_DATE}`);
    }
    
    // Step 5: Check wind generation data
    try {
      const windGenCount = await db.execute(sql`
        SELECT COUNT(*) FROM wind_generation_data 
        WHERE settlement_date = ${TARGET_DATE}::date
      `);
      
      const windGenTotal = await db.execute(sql`
        SELECT SUM(total_wind::numeric) as total_generation
        FROM wind_generation_data 
        WHERE settlement_date = ${TARGET_DATE}::date
      `);
      
      console.log(`\nWind Generation Data:`);
      console.log(`Records: ${windGenCount.rows[0]?.count || 0}`);
      
      if (windGenCount.rows[0]?.count > 0) {
        const totalWindGen = Number(windGenTotal.rows[0]?.total_generation || 0);
        const curtailedVolume = Number(curtailmentStats[0]?.totalVolume || 0);
        const curtailmentPercentage = (curtailedVolume / (totalWindGen + curtailedVolume)) * 100;
        
        console.log(`Total Wind Generation: ${totalWindGen.toFixed(2)} MWh`);
        console.log(`Wind Farm Percentages:`);
        console.log(`Actual Generation: ${(100 - curtailmentPercentage).toFixed(2)}%`);
        console.log(`Curtailed Volume: ${curtailmentPercentage.toFixed(2)}%`);
      } else {
        console.log(`No wind generation data found for ${TARGET_DATE}`);
      }
    } catch (error) {
      console.error('Error checking wind generation data:', error);
    }
    
    console.log(`\n=== Verification Complete for ${TARGET_DATE} ===`);
    
  } catch (error) {
    console.error(`\nError during verification:`, error);
    process.exit(1);
  }
}

// Run the verification
verifyData();
/**
 * Check reconciliation status for a specific date
 * This script verifies if all curtailment records have corresponding bitcoin calculations
 */
import { db } from './db';
import { curtailmentRecords, historicalBitcoinCalculations } from './db/schema';
import { eq, sql } from 'drizzle-orm';

async function checkDateReconciliation(date: string) {
  console.log(`Checking reconciliation for date ${date}...`);
  
  try {
    // 1. Count curtailment records
    const curtailmentResult = await db.select({
      count: sql<number>`count(*)`,
      periods: sql<number>`count(distinct ${curtailmentRecords.settlementPeriod})`,
      farms: sql<number>`count(distinct ${curtailmentRecords.farmId})`,
      totalVolume: sql<number>`sum(${curtailmentRecords.volume})`,
      totalPayment: sql<number>`sum(${curtailmentRecords.payment})`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
    
    const curtailmentCount = curtailmentResult[0].count;
    const uniquePeriods = curtailmentResult[0].periods;
    const uniqueFarms = curtailmentResult[0].farms;
    const totalVolume = curtailmentResult[0].totalVolume;
    const totalPayment = curtailmentResult[0].totalPayment;
    
    console.log(`Curtailment records: ${curtailmentCount}`);
    console.log(`Unique periods: ${uniquePeriods}`);
    console.log(`Unique farms: ${uniqueFarms}`);
    console.log(`Total volume: ${totalVolume} MWh`);
    console.log(`Total payment: £${totalPayment}`);
    
    // 2. Count bitcoin calculations by miner model
    const btcCalculations = await db.select({
      minerModel: historicalBitcoinCalculations.minerModel,
      count: sql<number>`count(*)`,
      totalBitcoin: sql<number>`sum(${historicalBitcoinCalculations.bitcoinMined})`,
      periods: sql<number>`count(distinct ${historicalBitcoinCalculations.settlementPeriod})`,
      farms: sql<number>`count(distinct ${historicalBitcoinCalculations.farmId})`
    })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, date))
    .groupBy(historicalBitcoinCalculations.minerModel);
    
    console.log(`\nBitcoin Calculations:`);
    for (const calc of btcCalculations) {
      console.log(`Model: ${calc.minerModel}, Count: ${calc.count}, Bitcoin: ${calc.totalBitcoin}, Periods: ${calc.periods}, Farms: ${calc.farms}`);
    }
    
    // 3. Calculate expected number of calculations
    const totalFarmPeriods = uniqueFarms * uniquePeriods;
    const minerModels = btcCalculations.map(c => c.minerModel);
    const expectedCalculations = totalFarmPeriods * minerModels.length;
    const actualCalculations = btcCalculations.reduce((sum, c) => sum + Number(c.count), 0);
    
    console.log(`\nExpected calculations: ${expectedCalculations}`);
    console.log(`Actual calculations: ${actualCalculations}`);
    
    if (actualCalculations >= expectedCalculations) {
      console.log(`✅ Reconciliation complete! (${(actualCalculations / expectedCalculations * 100).toFixed(2)}%)`);
    } else {
      console.log(`❌ Missing ${expectedCalculations - actualCalculations} calculations (${(actualCalculations / expectedCalculations * 100).toFixed(2)}%)`);
    }
    
    // 4. Find any specific issues
    console.log(`\nChecking for specific issues...`);
    
    // Check period 16 which was suspicious
    const period16Records = await db.select({
      count: sql<number>`count(*)`,
      farms: sql<number>`count(distinct ${curtailmentRecords.farmId})`,
      totalVolume: sql<number>`sum(${curtailmentRecords.volume})`,
    })
    .from(curtailmentRecords)
    .where(sql`${curtailmentRecords.settlementDate} = ${date} AND ${curtailmentRecords.settlementPeriod} = 16`);
    
    console.log(`Period 16 records: ${period16Records[0].count}`);
    console.log(`Period 16 farms: ${period16Records[0].farms}`);
    console.log(`Period 16 volume: ${period16Records[0].totalVolume} MWh`);
    
    // Check calculations for period 16
    const period16Calcs = await db.select({
      minerModel: historicalBitcoinCalculations.minerModel,
      count: sql<number>`count(*)`,
      farms: sql<number>`count(distinct ${historicalBitcoinCalculations.farmId})`,
      totalBitcoin: sql<number>`sum(${historicalBitcoinCalculations.bitcoinMined})`
    })
    .from(historicalBitcoinCalculations)
    .where(sql`${historicalBitcoinCalculations.settlementDate} = ${date} AND ${historicalBitcoinCalculations.settlementPeriod} = 16`)
    .groupBy(historicalBitcoinCalculations.minerModel);
    
    console.log(`\nPeriod 16 Bitcoin Calculations:`);
    for (const calc of period16Calcs) {
      console.log(`Model: ${calc.minerModel}, Count: ${calc.count}, Farms: ${calc.farms}, Bitcoin: ${calc.totalBitcoin}`);
    }
    
    return {
      date,
      curtailmentCount,
      uniquePeriods,
      uniqueFarms,
      totalVolume,
      totalPayment,
      bitcoinCalculations: btcCalculations,
      expectedCalculations,
      actualCalculations,
      reconciliationPercentage: (actualCalculations / expectedCalculations * 100)
    };
  } catch (error) {
    console.error('Error during reconciliation check:', error);
    return null;
  }
}

async function main() {
  const date = '2025-03-02';
  await checkDateReconciliation(date);
}

main();
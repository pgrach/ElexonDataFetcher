/**
 * Analyze Period Distribution
 * 
 * This script analyzes the distribution of settlement periods and farms
 * in the curtailment_records table for a specific date.
 */
import { db } from './db';
import { curtailmentRecords, historicalBitcoinCalculations } from './db/schema';
import { eq, sql } from 'drizzle-orm';

async function analyzePeriodDistribution(date: string) {
  console.log(`Analyzing period distribution for ${date}...`);
  
  // Count records by period
  const periodDistribution = await db.select({
    settlementPeriod: curtailmentRecords.settlementPeriod,
    recordCount: sql<number>`count(*)`,
    farmCount: sql<number>`count(distinct ${curtailmentRecords.farmId})`,
    totalVolume: sql<number>`sum(${curtailmentRecords.volume})`,
    totalPayment: sql<number>`sum(${curtailmentRecords.payment})`
  })
  .from(curtailmentRecords)
  .where(eq(curtailmentRecords.settlementDate, date))
  .groupBy(curtailmentRecords.settlementPeriod)
  .orderBy(curtailmentRecords.settlementPeriod);
  
  console.log(`\nPeriod distribution for ${date}:`);
  console.log('Period | Records | Farms | Volume (MWh) | Payment (£)');
  console.log('-------|---------|-------|--------------|------------');
  
  let totalRecords = 0;
  let uniqueFarmIds = new Set();
  
  for (const period of periodDistribution) {
    console.log(`${period.settlementPeriod.toString().padStart(6)} | ${period.recordCount.toString().padStart(7)} | ${period.farmCount.toString().padStart(5)} | ${Number(period.totalVolume).toFixed(2).padStart(12)} | ${Number(period.totalPayment).toFixed(2).padStart(10)}`);
    totalRecords += period.recordCount;
    
    // Get farm IDs for this period to track unique farms across all periods
    const farms = await db.select({
      farmId: curtailmentRecords.farmId
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date) && eq(curtailmentRecords.settlementPeriod, period.settlementPeriod));
    
    farms.forEach(farm => uniqueFarmIds.add(farm.farmId));
  }
  
  // Count Bitcoin calculations by period
  const btcDistribution = await db.select({
    settlementPeriod: historicalBitcoinCalculations.settlementPeriod,
    recordCount: sql<number>`count(*)`,
    modelCount: sql<number>`count(distinct ${historicalBitcoinCalculations.minerModel})`,
    farmCount: sql<number>`count(distinct ${historicalBitcoinCalculations.farmId})`,
    totalBitcoin: sql<number>`sum(${historicalBitcoinCalculations.bitcoinMined})`
  })
  .from(historicalBitcoinCalculations)
  .where(eq(historicalBitcoinCalculations.settlementDate, date))
  .groupBy(historicalBitcoinCalculations.settlementPeriod)
  .orderBy(historicalBitcoinCalculations.settlementPeriod);
  
  console.log(`\nBitcoin calculation distribution for ${date}:`);
  console.log('Period | Records | Models | Farms | Bitcoin Mined');
  console.log('-------|---------|--------|-------|---------------');
  
  let totalBtcRecords = 0;
  for (const period of btcDistribution) {
    console.log(`${period.settlementPeriod.toString().padStart(6)} | ${period.recordCount.toString().padStart(7)} | ${period.modelCount.toString().padStart(6)} | ${period.farmCount.toString().padStart(5)} | ${Number(period.totalBitcoin).toFixed(8).padStart(13)}`);
    totalBtcRecords += period.recordCount;
  }
  
  // Fix any counting errors caused by string concatenation
  totalRecords = periodDistribution.reduce((sum, p) => sum + Number(p.recordCount), 0);
  totalBtcRecords = btcDistribution.reduce((sum, p) => sum + Number(p.recordCount), 0);

  console.log(`\nSummary for ${date}:`);
  console.log(`Total curtailment records: ${totalRecords}`);
  console.log(`Total unique farms: ${uniqueFarmIds.size}`);
  console.log(`Total periods: ${periodDistribution.length}`);
  console.log(`Total Bitcoin calculation records: ${totalBtcRecords}`);
  
  console.log(`\nAnalysis:`);
  const expectedCalculations = uniqueFarmIds.size * periodDistribution.length * 3;
  console.log(`Expected calculations: ${expectedCalculations} (farms × periods × 3 models)`);
  console.log(`Actual calculations: ${totalBtcRecords}`);
  console.log(`Difference: ${expectedCalculations - totalBtcRecords}`);
  console.log(`Completion percentage: ${(totalBtcRecords / expectedCalculations * 100).toFixed(2)}%`);
  
  // Calculate distribution of farms across periods
  console.log(`\nFarm distribution across periods:`);
  const farmDistribution = await db.select({
    farmId: curtailmentRecords.farmId,
    periodCount: sql<number>`count(distinct ${curtailmentRecords.settlementPeriod})`.as('period_count')
  })
  .from(curtailmentRecords)
  .where(eq(curtailmentRecords.settlementDate, date))
  .groupBy(curtailmentRecords.farmId)
  .orderBy(sql`period_count DESC`);
  
  let periodCounts: Record<number, number> = {};
  for (const farm of farmDistribution) {
    // @ts-ignore - Type issue with the period count column
    const count = farm.period_count || farm.periodCount;
    periodCounts[count] = (periodCounts[count] || 0) + 1;
  }
  
  console.log('Periods | Farm Count');
  console.log('--------|------------');
  for (const [periods, count] of Object.entries(periodCounts)) {
    console.log(`${periods.padStart(7)} | ${count.toString().padStart(10)}`);
  }
}

async function main() {
  const date = '2025-03-02';
  await analyzePeriodDistribution(date);
}

main();
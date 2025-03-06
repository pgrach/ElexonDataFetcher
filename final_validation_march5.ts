import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, sql } from "drizzle-orm";

async function validateMarch5Data() {
  const TARGET_DATE = '2025-03-05';
  
  try {
    console.log(`\n=== Final Validation for ${TARGET_DATE} ===\n`);
    
    // Get per-period stats
    const periodStats = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        recordCount: sql<string>`COUNT(*)::text`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
      
    // Verify we have 48 periods
    const periodsCount = periodStats.length;
    console.log(`Total periods: ${periodsCount} / 48 ${periodsCount === 48 ? '✅' : '❌'}`);
    
    // Get overall stats
    const overallStats = await db
      .select({
        totalRecords: sql<string>`COUNT(*)::text`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`,
        distinctFarms: sql<string>`COUNT(DISTINCT farm_id)::text`,
        minPeriod: sql<string>`MIN(settlement_period)::text`,
        maxPeriod: sql<string>`MAX(settlement_period)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const totalRecords = Number(overallStats[0].totalRecords);
    const totalVolume = Number(overallStats[0].totalVolume);
    const totalPayment = Number(overallStats[0].totalPayment);
    const distinctFarms = Number(overallStats[0].distinctFarms);
    const minPeriod = Number(overallStats[0].minPeriod);
    const maxPeriod = Number(overallStats[0].maxPeriod);
    
    console.log(`\n=== Overall Statistics ===`);
    console.log(`Total records: ${totalRecords}`);
    console.log(`Total curtailed volume: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment: £${totalPayment.toFixed(2)}`);
    console.log(`Distinct wind farms: ${distinctFarms}`);
    console.log(`Period range: ${minPeriod} - ${maxPeriod}`);
    
    // Summarize period distribution
    console.log(`\n=== Period Statistics ===`);
    console.log(`Period\tRecords\tVolume (MWh)\tPayment (£)`);
    
    let lowestRecordCount = Infinity;
    let highestRecordCount = 0;
    let lowestPeriod = 0;
    let highestPeriod = 0;
    
    periodStats.forEach(stat => {
      const recordCount = Number(stat.recordCount);
      const periodVolume = Number(stat.totalVolume);
      const periodPayment = Number(stat.totalPayment);
      
      console.log(`${stat.period}\t${recordCount}\t${periodVolume.toFixed(2)}\t${periodPayment.toFixed(2)}`);
      
      if (recordCount < lowestRecordCount) {
        lowestRecordCount = recordCount;
        lowestPeriod = stat.period;
      }
      
      if (recordCount > highestRecordCount) {
        highestRecordCount = recordCount;
        highestPeriod = stat.period;
      }
    });
    
    console.log(`\n=== Distribution Analysis ===`);
    console.log(`Period with fewest records: Period ${lowestPeriod} (${lowestRecordCount} records)`);
    console.log(`Period with most records: Period ${highestPeriod} (${highestRecordCount} records)`);
    
    // Verify the daily summary
    const dailySummary = await db
      .select({
        totalCurtailedEnergy: sql<string>`total_curtailed_energy`,
        totalPayment: sql<string>`total_payment`
      })
      .from(sql`daily_summaries`)
      .where(sql`summary_date = ${TARGET_DATE}`);
    
    if (dailySummary.length > 0) {
      const summaryVolume = Number(dailySummary[0].totalCurtailedEnergy);
      const summaryPayment = Number(dailySummary[0].totalPayment);
      
      console.log(`\n=== Daily Summary Verification ===`);
      console.log(`Summary volume: ${summaryVolume.toFixed(2)} MWh`);
      console.log(`Summary payment: £${summaryPayment.toFixed(2)}`);
      
      const volumeDiff = Math.abs(summaryVolume - totalVolume);
      const paymentDiff = Math.abs(summaryPayment - totalPayment);
      
      console.log(`Volume difference: ${volumeDiff.toFixed(2)} MWh ${volumeDiff < 0.01 ? '✅' : '❌'}`);
      console.log(`Payment difference: £${paymentDiff.toFixed(2)} ${paymentDiff < 0.01 ? '✅' : '❌'}`);
    } else {
      console.log(`\n⚠️ Daily summary not found for ${TARGET_DATE}`);
    }
    
    console.log(`\n=== Final Status ===`);
    const isComplete = periodsCount === 48 && minPeriod === 1 && maxPeriod === 48;
    console.log(`March 5, 2025 data is ${isComplete ? 'COMPLETE ✅' : 'INCOMPLETE ❌'}`);
    
  } catch (error) {
    console.error('Error validating March 5 data:', error);
  }
}

validateMarch5Data();
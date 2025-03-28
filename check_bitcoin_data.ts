import { db } from './db';
import { historicalBitcoinCalculations, bitcoinMonthlySummaries } from './db/schema';
import { eq, and, sql } from 'drizzle-orm';

async function checkBitcoinData() {
  const date1 = '2025-03-27';
  const date2 = '2025-03-28';
  
  console.log(`\n=== Checking Bitcoin Data for ${date1} ===\n`);
  
  // Check how many records we have for this date
  const recordCounts1 = await db
    .select({
      minerModel: historicalBitcoinCalculations.minerModel,
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
      totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`
    })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, date1))
    .groupBy(historicalBitcoinCalculations.minerModel);
  
  if (recordCounts1.length === 0) {
    console.log(`No Bitcoin data found for ${date1}\n`);
  } else {
    console.log(`Found Bitcoin data for ${date1}:\n`);
    
    for (const stat of recordCounts1) {
      console.log(`${stat.minerModel}:`);
      console.log(`- Records: ${stat.recordCount}`);
      console.log(`- Unique Periods: ${stat.periodCount} of 48`);
      console.log(`- Unique Farms: ${stat.farmCount}`);
      console.log(`- Total Bitcoin Mined: ${Number(stat.totalBitcoin).toFixed(8)} BTC\n`);
    }
  }
  
  console.log(`\n=== Checking Bitcoin Data for ${date2} ===\n`);
  
  // Check how many records we have for this date
  const recordCounts2 = await db
    .select({
      minerModel: historicalBitcoinCalculations.minerModel,
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
      totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`
    })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, date2))
    .groupBy(historicalBitcoinCalculations.minerModel);
  
  if (recordCounts2.length === 0) {
    console.log(`No Bitcoin data found for ${date2}\n`);
  } else {
    console.log(`Found Bitcoin data for ${date2}:\n`);
    
    for (const stat of recordCounts2) {
      console.log(`${stat.minerModel}:`);
      console.log(`- Records: ${stat.recordCount}`);
      console.log(`- Unique Periods: ${stat.periodCount} of 48`);
      console.log(`- Unique Farms: ${stat.farmCount}`);
      console.log(`- Total Bitcoin Mined: ${Number(stat.totalBitcoin).toFixed(8)} BTC\n`);
    }
  }

  // Check monthly summaries for March 2025
  console.log(`\n=== Checking Monthly Bitcoin Summary for 2025-03 ===\n`);
  
  const monthlySummaries = await db
    .select()
    .from(bitcoinMonthlySummaries)
    .where(eq(bitcoinMonthlySummaries.yearMonth, '2025-03'));
  
  if (monthlySummaries.length === 0) {
    console.log(`No monthly Bitcoin summaries found for 2025-03\n`);
  } else {
    console.log(`Found monthly Bitcoin summaries for 2025-03:\n`);
    
    for (const summary of monthlySummaries) {
      console.log(`${summary.minerModel}:`);
      console.log(`- Total Bitcoin Mined: ${Number(summary.bitcoinMined).toFixed(8)} BTC`);
      console.log(`- Updated At: ${summary.updatedAt}\n`);
    }
  }
  
  // Exit the process
  process.exit(0);
}

// Run the check
checkBitcoinData().catch(error => {
  console.error('Error checking Bitcoin data:', error);
  process.exit(1);
});
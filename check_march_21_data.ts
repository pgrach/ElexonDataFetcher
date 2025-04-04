/**
 * Check March 21, 2025 Data
 * 
 * This script checks the current state of data for March 21, 2025 in the database,
 * showing which settlement periods are present and which are missing.
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, sql } from 'drizzle-orm';

const TARGET_DATE = '2025-03-21';

async function checkData() {
  console.log(`=== Checking Data for ${TARGET_DATE} ===`);
  
  // Check curtailment records
  const curtailmentCountResult = await db
    .select({
      count: sql<number>`COUNT(*)`,
      periods: sql<number>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
      totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  if (curtailmentCountResult[0].count > 0) {
    console.log(`Found ${curtailmentCountResult[0].count} curtailment records`);
    console.log(`Settlement periods: ${curtailmentCountResult[0].periods} / 48`);
    console.log(`Total volume: ${curtailmentCountResult[0].totalVolume} MWh`);
    console.log(`Total payment: £${curtailmentCountResult[0].totalPayment}`);
    
    // Get a list of existing periods
    const periodsResult = await db
      .select({ period: curtailmentRecords.settlementPeriod })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    const existingPeriods = periodsResult.map(r => r.period);
    console.log(`\nExisting periods (${existingPeriods.length}): ${existingPeriods.join(', ')}`);
    
    // Find missing periods
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    const missingPeriods = allPeriods.filter(p => !existingPeriods.includes(p));
    
    if (missingPeriods.length > 0) {
      console.log(`\nMissing periods (${missingPeriods.length}): ${missingPeriods.join(', ')}`);
    } else {
      console.log('\nAll 48 settlement periods are present');
    }
  } else {
    console.log(`No curtailment records found for ${TARGET_DATE}`);
  }
  
  // Check daily summary
  const dailySummaryResult = await db
    .select()
    .from(dailySummaries)
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
  
  if (dailySummaryResult.length > 0) {
    console.log(`\nDaily summary for ${TARGET_DATE}:`);
    console.log(`Energy: ${dailySummaryResult[0].totalCurtailedEnergy} MWh`);
    console.log(`Payment: £${dailySummaryResult[0].totalPayment}`);
    console.log(`Last updated: ${dailySummaryResult[0].lastUpdated}`);
  } else {
    console.log(`\nNo daily summary found for ${TARGET_DATE}`);
  }
  
  // Check Bitcoin calculations
  const bitcoinCountResult = await db.execute(sql`
    SELECT miner_model, COUNT(*) as count, COUNT(DISTINCT settlement_period) as periods
    FROM historical_bitcoin_calculations
    WHERE settlement_date = ${TARGET_DATE}
    GROUP BY miner_model
  `);
  
  if (bitcoinCountResult.rows.length > 0) {
    console.log(`\nBitcoin calculations for ${TARGET_DATE}:`);
    for (const row of bitcoinCountResult.rows) {
      console.log(`${row.miner_model}: ${row.count} records, ${row.periods} periods`);
    }
  } else {
    console.log(`\nNo Bitcoin calculations found for ${TARGET_DATE}`);
  }
}

checkData().catch(console.error);
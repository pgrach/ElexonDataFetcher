/**
 * Verification Script for 2025-03-18 Data
 * 
 * This script performs a comprehensive check of the 2025-03-18 data
 * to ensure all periods have been properly processed.
 */

import { db } from './db';
import { eq, and, sql, asc } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// Set up ES Module compatible dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const TARGET_DATE = '2025-03-18';

// Main verification function
async function verifyData() {
  console.log(`=== Verification Report for ${TARGET_DATE} ===\n`);
  
  // 1. Check curtailment records
  const curtailmentStats = await db.select({
    periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
    recordCount: sql<number>`COUNT(*)`,
    totalVolume: sql<number>`ROUND(SUM(ABS(volume::numeric))::numeric, 2)`,
    totalPayment: sql<number>`ROUND(SUM(payment::numeric)::numeric, 2)`,
    periods: sql<string[]>`ARRAY_AGG(DISTINCT settlement_period ORDER BY settlement_period)`
  }).from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));

  if (curtailmentStats.length === 0 || curtailmentStats[0].recordCount === 0) {
    console.log(`No curtailment data found for ${TARGET_DATE}`);
    return;
  }

  const stats = curtailmentStats[0];
  console.log(`Curtailment Records Summary:`);
  console.log(`- ${stats.recordCount} records across ${stats.periodCount} periods`);
  console.log(`- Total curtailed energy: ${stats.totalVolume} MWh`);
  console.log(`- Total payment: £${stats.totalPayment}`);
  console.log(`- Periods with data: ${stats.periods.join(', ')}`);
  
  // 2. Check for Bitcoin calculations
  const bitcoinStats = await db.select({
    periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
    recordCount: sql<number>`COUNT(*)`,
    totalBitcoin: sql<number>`ROUND(SUM(bitcoin_mined)::numeric, 8)`,
    periods: sql<number[]>`ARRAY_AGG(DISTINCT settlement_period ORDER BY settlement_period)`
  })
  .from('historical_bitcoin_calculations')
  .where(and(
    eq('settlement_date', TARGET_DATE),
    eq('miner_model', 'S19J_PRO')
  ));

  if (bitcoinStats.length === 0 || bitcoinStats[0].recordCount === 0) {
    console.log(`\nNo Bitcoin calculations found for ${TARGET_DATE}`);
  } else {
    console.log(`\nBitcoin Calculations Summary (S19J_PRO):`);
    console.log(`- ${bitcoinStats[0].recordCount} records across ${bitcoinStats[0].periodCount} periods`);
    console.log(`- Total Bitcoin: ${bitcoinStats[0].totalBitcoin} BTC`);
    console.log(`- Periods with calculations: ${bitcoinStats[0].periods.join(', ')}`);
  }
  
  // 3. Verify reconciliation (do we have Bitcoin calculations for every curtailment record)
  console.log(`\nReconciliation Check:`);
  
  // Using a simplified approach with SQL to avoid compatibility issues with Drizzle
  const curtailedPeriods = await db.select({
    period: curtailmentRecords.settlementPeriod
  })
  .from(curtailmentRecords)
  .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
  .groupBy(curtailmentRecords.settlementPeriod);
  
  const curtailedPeriodSet = new Set(curtailedPeriods.map(r => r.period));
  const bitcoinPeriodSet = new Set(bitcoinStats[0]?.periods || []);
  
  const missingPeriods = Array.from(curtailedPeriodSet)
    .filter(period => !bitcoinPeriodSet.has(period))
    .sort((a, b) => a - b);
  
  if (missingPeriods.length === 0) {
    console.log(`✓ All periods with curtailment data have Bitcoin calculations`);
  } else {
    console.log(`✗ Missing Bitcoin calculations for periods: ${missingPeriods.join(', ')}`);
  }
  
  // 4. Check for curtailment farm counts vs. Bitcoin calculation farm counts
  console.log(`\nFarm Count Verification:`);
  
  // Get curtailed farm counts
  const curtailedFarmCounts = await db.select({
    period: curtailmentRecords.settlementPeriod,
    count: sql<number>`COUNT(DISTINCT farm_id)`
  })
  .from(curtailmentRecords)
  .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
  .groupBy(curtailmentRecords.settlementPeriod);
  
  // Get Bitcoin farm counts (using raw SQL to handle table name as string)
  const bitcoinFarmCountsResult = await db.execute(sql`
    SELECT 
      settlement_period as period, 
      COUNT(DISTINCT farm_id) as count
    FROM 
      historical_bitcoin_calculations
    WHERE 
      settlement_date = ${TARGET_DATE}
      AND miner_model = 'S19J_PRO'
    GROUP BY 
      settlement_period
  `);
  
  // Parse the result rows
  const bitcoinFarmCounts = bitcoinFarmCountsResult.map(row => ({
    period: Number(row.period),
    count: Number(row.count)
  }));
  
  // Convert to maps for easier lookup
  const curtailedFarmCountMap = new Map(
    curtailedFarmCounts.map(row => [row.period, row.count])
  );
  
  const bitcoinFarmCountMap = new Map(
    bitcoinFarmCounts.map(row => [row.period, row.count])
  );
  
  // Print the comparison table
  if (curtailedFarmCounts.length === 0) {
    console.log(`No data available for verification`);
  } else {
    console.log(`Period | Curtailed Farms | Bitcoin Farms | Status`);
    console.log(`-------|----------------|---------------|--------`);
    
    let mismatchFound = false;
    
    // Sort periods for display
    const periods = [...new Set([
      ...curtailedFarmCounts.map(row => row.period),
      ...bitcoinFarmCounts.map(row => row.period)
    ])].sort((a, b) => a - b);
    
    for (const period of periods) {
      const curtailedCount = curtailedFarmCountMap.get(period) || 0;
      const bitcoinCount = bitcoinFarmCountMap.get(period) || 0;
      const matchStatus = curtailedCount === bitcoinCount ? '✓' : '✗';
      
      console.log(`${period.toString().padStart(6)} | ${curtailedCount.toString().padStart(14)} | ${bitcoinCount.toString().padStart(13)} | ${matchStatus}`);
      
      if (matchStatus === '✗') {
        mismatchFound = true;
      }
    }
    
    if (!mismatchFound) {
      console.log(`\n✓ All periods have matching farm counts between curtailment and Bitcoin records`);
    } else {
      console.log(`\n✗ Some periods have mismatched farm counts (see table above)`);
    }
  }
  
  // 5. Verify any potentially missing periods through the API
  console.log(`\nAPI Verification Check:`);
  console.log(`Checking a sample of periods with no data...`);
  
  const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
  const periodsWithData = new Set(stats.periods);
  const periodsWithoutData = allPeriods.filter(period => !periodsWithData.has(period));
  
  // Sample 5 periods at most
  const samplesToCheck = periodsWithoutData.length <= 5 ? 
                         periodsWithoutData : 
                         periodsWithoutData.filter((_, i) => i % Math.ceil(periodsWithoutData.length / 5) === 0).slice(0, 5);
  
  console.log(`- Will check ${samplesToCheck.length} sample periods: ${samplesToCheck.join(', ')}`);
  
  for (const period of samplesToCheck) {
    try {
      console.log(`\nChecking period ${period}...`);
      
      const [bidsResponse, offersResponse] = await Promise.all([
        axios.get(`https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/stack/all/bid/${TARGET_DATE}/${period}`, {
          headers: { 'Accept': 'application/json' },
          timeout: 30000
        }),
        axios.get(`https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/stack/all/offer/${TARGET_DATE}/${period}`, {
          headers: { 'Accept': 'application/json' },
          timeout: 30000
        })
      ]);
      
      const bidsData = bidsResponse.data?.data || [];
      const offersData = offersResponse.data?.data || [];
      const allData = [...bidsData, ...offersData];
      
      const windFarmData = allData.filter(record => 
        record.volume < 0 && (record.soFlag || record.cadlFlag)
        // Note: We would check for wind farm IDs here but that requires loading BMU mappings
      );
      
      console.log(`- API returned ${allData.length} records for period ${period}`);
      console.log(`- Found ${windFarmData.length} potential curtailment records`);
      
      if (windFarmData.length > 0) {
        console.log(`⚠️ Period ${period} might have curtailment data that's not in our database!`);
      } else {
        console.log(`✓ Period ${period} confirmed to have no curtailment data`);
      }
      
    } catch (error) {
      console.error(`Error checking period ${period}: ${error.message || error}`);
    }
    
    // Brief delay to avoid rate limits
    await new Promise(resolve => setTimeout(resolve, 3000));
  }

  console.log(`\n=== Verification Complete ===`);
}

// Execute the verification
verifyData().then(() => {
  console.log('Verification script completed');
  process.exit(0);
}).catch(error => {
  console.error('Error during verification:', error);
  process.exit(1);
});
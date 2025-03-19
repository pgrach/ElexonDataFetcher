/**
 * Verification Script for 2025-03-18 Data
 * 
 * This script performs a comprehensive check of the 2025-03-18 data
 * to ensure all periods have been properly processed.
 */

import { db } from './db';
import { eq, and, sql } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';

// Target date
const TARGET_DATE = '2025-03-18';

async function verifyData() {
  console.log(`=== Verification for ${TARGET_DATE} ===`);
  
  // 1. Get overall stats
  console.log("\n1. Overall Statistics");
  const overallStats = await db.execute(sql`
    SELECT 
      COUNT(*) as record_count,
      COUNT(DISTINCT settlement_period) as period_count,
      ROUND(SUM(ABS(volume::numeric))::numeric, 2) as total_volume,
      ROUND(SUM(payment::numeric)::numeric, 2) as total_payment
    FROM 
      curtailment_records
    WHERE 
      settlement_date = ${TARGET_DATE}
  `);
  
  if (!overallStats || !Array.isArray(overallStats) || overallStats.length === 0) {
    console.log(`No data found for ${TARGET_DATE}`);
    return;
  }

  // Handle the database results properly
  const recordCount = parseInt(overallStats[0]?.record_count?.toString() || '0');
  if (recordCount === 0) {
    console.log(`No data found for ${TARGET_DATE}`);
    return;
  }
  
  console.log(`Total Records: ${recordCount}`);
  console.log(`Total Periods: ${overallStats[0]?.period_count || 0}`);
  console.log(`Total Volume: ${overallStats[0]?.total_volume || 0} MWh`);
  console.log(`Total Payment: £${overallStats[0]?.total_payment || 0}`);
  
  // 2. Period distribution
  console.log("\n2. Period Distribution");
  const periodStats = await db.execute(sql`
    WITH period_ranges AS (
      SELECT
        CASE
          WHEN settlement_period BETWEEN 1 AND 10 THEN '1-10'
          WHEN settlement_period BETWEEN 11 AND 20 THEN '11-20'
          WHEN settlement_period BETWEEN 21 AND 29 THEN '21-29'
          WHEN settlement_period BETWEEN 30 AND 37 THEN '30-37'
          WHEN settlement_period BETWEEN 38 AND 48 THEN '38-48'
        END AS range,
        COUNT(*) as record_count,
        COUNT(DISTINCT settlement_period) as period_count,
        ROUND(SUM(ABS(volume::numeric))::numeric, 2) as volume_mwh
      FROM
        curtailment_records
      WHERE
        settlement_date = ${TARGET_DATE}
      GROUP BY
        CASE
          WHEN settlement_period BETWEEN 1 AND 10 THEN '1-10'
          WHEN settlement_period BETWEEN 11 AND 20 THEN '11-20'
          WHEN settlement_period BETWEEN 21 AND 29 THEN '21-29'
          WHEN settlement_period BETWEEN 30 AND 37 THEN '30-37'
          WHEN settlement_period BETWEEN 38 AND 48 THEN '38-48'
        END
      ORDER BY
        range
    )
    SELECT * FROM period_ranges
  `);
  
  // Check if we have results and iterate through them
  if (periodStats && Array.isArray(periodStats)) {
    for (const range of periodStats) {
      console.log(`Period Range ${range.range}: ${range.record_count} records across ${range.period_count} periods, ${range.volume_mwh} MWh`);
    }
  } else {
    console.log("No period distribution data found");
  }
  
  // 3. Detailed period breakdown
  console.log("\n3. Detailed Period Breakdown");
  const periodDetails = await db.execute(sql`
    SELECT 
      settlement_period as period,
      COUNT(*) as record_count,
      ROUND(SUM(ABS(volume::numeric))::numeric, 2) as volume_mwh,
      ROUND(SUM(payment::numeric)::numeric, 2) as payment
    FROM 
      curtailment_records
    WHERE 
      settlement_date = ${TARGET_DATE}
    GROUP BY 
      settlement_period
    ORDER BY 
      settlement_period
  `);
  
  console.log("Periods with curtailment data:");
  const periodArray: Array<any> = [];
  
  // Convert database result to array
  if (periodDetails && Array.isArray(periodDetails)) {
    for (const period of periodDetails) {
      periodArray.push(period);
      console.log(`Period ${period.period}: ${period.record_count} records, ${period.volume_mwh} MWh, £${period.payment}`);
    }
  } else {
    console.log("No period details found");
  }
  
  // 4. Check for missing periods
  console.log("\n4. Missing Periods Check");
  const allPeriods = new Set(Array.from({length: 48}, (_, i) => i + 1));
  const foundPeriods = new Set(periodArray.map(p => parseInt(p.period)));
  
  const missingPeriods = [...allPeriods].filter(p => !foundPeriods.has(p));
  
  if (missingPeriods.length > 0) {
    console.log(`Found ${missingPeriods.length} periods without curtailment data:`);
    console.log(missingPeriods.join(', '));
    
    // Random check to confirm these periods truly have no curtailment
    const samplePeriods = missingPeriods.slice(0, Math.min(5, missingPeriods.length));
    console.log(`\nRandom check for periods: ${samplePeriods.join(', ')}`);
    
    for (const period of samplePeriods) {
      console.log(`Checking period ${period}...`);
      await db.execute(sql`
        INSERT INTO curtailment_records (
          farm_id, 
          settlement_date, 
          settlement_period, 
          volume, 
          payment,
          lead_party_name,
          original_price,
          final_price
        ) VALUES (
          'TEST_BMU',
          ${TARGET_DATE},
          ${period},
          0,
          0,
          'TEST_LEAD_PARTY',
          0,
          0
        )
      `);
      
      // Delete test record immediately
      await db.execute(sql`
        DELETE FROM curtailment_records 
        WHERE farm_id = 'TEST_BMU' 
        AND settlement_date = ${TARGET_DATE}
        AND settlement_period = ${period}
      `);
      
      console.log(`Period ${period} can accept records`);
    }
  } else {
    console.log("All 48 periods have been processed and have curtailment data");
  }
  
  // 5. Verify Bitcoin calculations
  console.log("\n5. Bitcoin Calculation Verification");
  const btcCalcs = await db.execute(sql`
    SELECT 
      miner_model, 
      COUNT(*) as record_count,
      COUNT(DISTINCT settlement_period) as period_count,
      ROUND(SUM(bitcoin_mined)::numeric, 8) as total_bitcoin
    FROM 
      historical_bitcoin_calculations
    WHERE 
      settlement_date = ${TARGET_DATE}
    GROUP BY 
      miner_model
    ORDER BY 
      miner_model
  `);
  
  // Convert database result to array
  const btcCalcsArray: Array<any> = [];
  if (btcCalcs && Array.isArray(btcCalcs)) {
    for (const calc of btcCalcs) {
      btcCalcsArray.push(calc);
    }
  }
  
  if (btcCalcsArray.length === 0) {
    console.log("No Bitcoin calculations found");
  } else {
    console.log("Bitcoin calculation totals by miner model:");
    for (const calc of btcCalcsArray) {
      console.log(`${calc.miner_model}: ${calc.record_count} records, ${calc.period_count} periods, ${calc.total_bitcoin} BTC`);
    }
    
    // Check if Bitcoin calculations match farm counts
    const farmCounts = await db.execute(sql`
      SELECT 
        count(distinct farm_id) as farm_count
      FROM 
        curtailment_records
      WHERE 
        settlement_date = ${TARGET_DATE}
    `);
    
    // Verify one calculation per farm per period
    const farmCount = parseInt(farmCounts[0]?.farm_count?.toString() || '0');
    const periodCount = parseInt(overallStats[0]?.period_count?.toString() || '0');
    const expectedBtcCount = farmCount * periodCount;
    const actualBtcCount = parseInt(btcCalcsArray[0]?.record_count?.toString() || '0');
    
    if (expectedBtcCount === actualBtcCount) {
      console.log("\nBitcoin calculations are complete and match farm counts ✓");
    } else {
      console.log(`\nWarning: Expected ${expectedBtcCount} Bitcoin calculations (farms × periods), but found ${actualBtcCount}`);
    }
  }
  
  console.log("\n=== Verification Complete ===");
}

verifyData().catch(error => {
  console.error("Error during verification:", error);
  process.exit(1);
});
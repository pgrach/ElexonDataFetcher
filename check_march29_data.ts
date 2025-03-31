/**
 * Simple script to check 2025-03-29 data
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

async function checkData() {
  console.log('Checking data for 2025-03-29...');
  
  // Check curtailment records count
  const countResult = await db.execute(sql`
    SELECT COUNT(*) FROM curtailment_records 
    WHERE settlement_date = '2025-03-29'
  `);
  console.log('Total curtailment records:', countResult.rows[0].count);
  
  // Check periods with data
  const periodsResult = await db.execute(sql`
    SELECT settlement_period, COUNT(*) as count
    FROM curtailment_records 
    WHERE settlement_date = '2025-03-29'
    GROUP BY settlement_period
    ORDER BY settlement_period
  `);
  
  console.log('\nPeriods with data:');
  let periodsWithData = 0;
  const periodsMap = new Map();
  
  for (const row of periodsResult.rows) {
    console.log(`  Period ${row.settlement_period}: ${row.count} records`);
    periodsMap.set(parseInt(row.settlement_period), parseInt(row.count));
    periodsWithData++;
  }
  
  console.log(`\nFound data for ${periodsWithData} unique periods`);
  
  // Check for missing periods
  const missingPeriods = [];
  for (let i = 1; i <= 48; i++) {
    if (!periodsMap.has(i)) {
      missingPeriods.push(i);
    }
  }
  
  if (missingPeriods.length > 0) {
    console.log('Missing periods:', missingPeriods.join(', '));
  } else {
    console.log('All 48 periods have data!');
  }
  
  // Get total volume and payment
  const totalsResult = await db.execute(sql`
    SELECT 
      ROUND(SUM(ABS(CAST(volume AS DECIMAL))), 2) as total_volume,
      ROUND(SUM(CAST(payment AS DECIMAL)), 2) as total_payment
    FROM curtailment_records 
    WHERE settlement_date = '2025-03-29'
  `);
  
  const volume = totalsResult.rows[0].total_volume;
  const payment = totalsResult.rows[0].total_payment;
  
  console.log(`\nTotal volume: ${volume} MWh`);
  console.log(`Total payment: £${payment}`);
  
  // Check Bitcoin calculations
  const bitcoinResult = await db.execute(sql`
    SELECT 
      miner_model,
      COUNT(*) as record_count,
      ROUND(SUM(CAST(bitcoin_mined AS DECIMAL)), 8) as total_btc
    FROM historical_bitcoin_calculations
    WHERE settlement_date = '2025-03-29'
    GROUP BY miner_model
  `);
  
  console.log('\nBitcoin calculations:');
  for (const row of bitcoinResult.rows) {
    console.log(`  ${row.miner_model}: ${row.record_count} records, ${row.total_btc} BTC`);
  }
  
  // Check Bitcoin calculations by period
  const bitcoinPeriodResult = await db.execute(sql`
    SELECT 
      miner_model,
      settlement_period,
      COUNT(*) as record_count
    FROM historical_bitcoin_calculations
    WHERE settlement_date = '2025-03-29'
    GROUP BY miner_model, settlement_period
    ORDER BY miner_model, settlement_period
  `);
  
  // Organize by miner model
  const calcsByModel = new Map();
  for (const row of bitcoinPeriodResult.rows) {
    if (!calcsByModel.has(row.miner_model)) {
      calcsByModel.set(row.miner_model, new Map());
    }
    calcsByModel.get(row.miner_model).set(parseInt(row.settlement_period), parseInt(row.record_count));
  }
  
  console.log('\nMissing Bitcoin calculations by period:');
  for (const [model, periodMap] of calcsByModel.entries()) {
    const missingForModel = [];
    for (let period = 1; period <= 48; period++) {
      if (periodMap.get(period) === undefined && periodsMap.has(period)) {
        missingForModel.push(period);
      }
    }
    
    if (missingForModel.length > 0) {
      console.log(`  ${model}: ${missingForModel.join(', ')}`);
    } else {
      console.log(`  ${model}: All periods have calculations!`);
    }
  }
}

// Run the check
checkData()
  .then(() => {
    console.log('\nCheck complete!');
    process.exit(0);
  })
  .catch(error => {
    console.error('Error:', error);
    process.exit(1);
  });
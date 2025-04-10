/**
 * Verify Data Integrity for 2025-04-03
 * 
 * This script verifies that all tables have consistent data for 2025-04-03.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

// Configuration
const TARGET_DATE = '2025-04-03';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Simple logging utility with timestamps
 */
function log(message: string): void {
  const timestamp = new Date().toISOString().substring(11, 19);
  console.log(`[${timestamp}] ${message}`);
}

async function verifyDataIntegrity(): Promise<void> {
  log(`Verifying data integrity for ${TARGET_DATE}...`);
  
  // Step 1: Check curtailment records
  const curtailmentResult = await db.execute(sql`
    SELECT COUNT(*) as record_count, SUM(ABS(volume::float)) as total_volume
    FROM curtailment_records
    WHERE settlement_date = ${TARGET_DATE}
  `);
  
  const recordCount = curtailmentResult.rows[0]?.record_count || '0';
  const totalVolume = parseFloat(curtailmentResult.rows[0]?.total_volume || '0').toFixed(2);
  
  log(`Curtailment Records: ${recordCount} records, ${totalVolume} MWh`);
  
  // Step 2: Check daily summary
  const dailySummaryResult = await db.execute(sql`
    SELECT total_curtailed_energy, total_payment
    FROM daily_summaries
    WHERE summary_date = ${TARGET_DATE}
  `);
  
  if (dailySummaryResult.rows.length > 0) {
    const totalCurtailedEnergy = parseFloat(dailySummaryResult.rows[0]?.total_curtailed_energy || '0').toFixed(2);
    const totalPayment = parseFloat(dailySummaryResult.rows[0]?.total_payment || '0').toFixed(2);
    
    log(`Daily Summary: ${totalCurtailedEnergy} MWh, £${totalPayment}`);
  } else {
    log(`Daily Summary: Not found`);
  }
  
  // Step 3: Check Bitcoin calculations
  for (const minerModel of MINER_MODELS) {
    const bitcoinCalcResult = await db.execute(sql`
      SELECT COUNT(*) as calc_count, SUM(bitcoin_mined::float) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${minerModel}
    `);
    
    const calcCount = bitcoinCalcResult.rows[0]?.calc_count || '0';
    const totalBitcoin = parseFloat(bitcoinCalcResult.rows[0]?.total_bitcoin || '0').toFixed(8);
    
    log(`Bitcoin Calculations (${minerModel}): ${calcCount} records, ${totalBitcoin} BTC`);
  }
  
  // Step 4: Check Bitcoin daily summaries
  for (const minerModel of MINER_MODELS) {
    const bitcoinDailySummaryResult = await db.execute(sql`
      SELECT bitcoin_mined
      FROM bitcoin_daily_summaries
      WHERE summary_date = ${TARGET_DATE} AND miner_model = ${minerModel}
    `);
    
    if (bitcoinDailySummaryResult.rows.length > 0) {
      const bitcoinMined = parseFloat(bitcoinDailySummaryResult.rows[0]?.bitcoin_mined || '0').toFixed(8);
      
      log(`Bitcoin Daily Summary (${minerModel}): ${bitcoinMined} BTC`);
    } else {
      log(`Bitcoin Daily Summary (${minerModel}): Not found`);
    }
  }
  
  // Step 5: Check Bitcoin monthly summary for 2025-04
  const monthlyData = await db.execute(sql`
    SELECT miner_model, bitcoin_mined
    FROM bitcoin_monthly_summaries
    WHERE year_month = '2025-04'
    ORDER BY miner_model
  `);
  
  log(`\nBitcoin Monthly Summary for 2025-04:`);
  for (const row of monthlyData.rows) {
    const minerModel = row.miner_model;
    const bitcoinMined = parseFloat(row.bitcoin_mined as string).toFixed(8);
    
    log(`  ${minerModel}: ${bitcoinMined} BTC`);
  }
  
  // Step 6: Check Bitcoin yearly summary for 2025
  const yearlyData = await db.execute(sql`
    SELECT miner_model, bitcoin_mined
    FROM bitcoin_yearly_summaries
    WHERE year = '2025' AND miner_model IN ('S19J_PRO', 'S9', 'M20S')
    ORDER BY miner_model
  `);
  
  log(`\nBitcoin Yearly Summary for 2025:`);
  for (const row of yearlyData.rows) {
    const minerModel = row.miner_model;
    const bitcoinMined = parseFloat(row.bitcoin_mined as string).toFixed(8);
    
    log(`  ${minerModel}: ${bitcoinMined} BTC`);
  }
}

// Execute the verification
verifyDataIntegrity()
  .then(() => {
    console.log('\nData verification completed successfully.');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\nData verification failed with error:', error);
    process.exit(1);
  });
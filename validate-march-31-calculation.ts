/**
 * Validate March 31 Bitcoin Calculation
 * 
 * This script validates that the Bitcoin calculation for March 31, 2025
 * is correctly aligned with the Bitcoin calculation formula.
 */

import { calculateBitcoin } from './server/utils/bitcoin';
import { db } from './db';
import { sql } from 'drizzle-orm';

const DATE_TO_VALIDATE = '2025-03-31';
const DIFFICULTY = 113757508810853;

async function validateCalculation() {
  console.log(`=== Validating Bitcoin Calculation for ${DATE_TO_VALIDATE} ===`);
  
  // Fetch daily totals
  const dailySummaryResult = await db.execute(sql`
    SELECT
      miner_model,
      bitcoin_mined
    FROM
      bitcoin_daily_summaries
    WHERE
      summary_date = ${DATE_TO_VALIDATE}
    ORDER BY
      miner_model
  `);
  
  // Fetch total energy
  const energyResult = await db.execute(sql`
    SELECT
      SUM(ABS(volume::numeric)) as total_energy_mwh
    FROM
      curtailment_records
    WHERE
      settlement_date = ${DATE_TO_VALIDATE}
  `);
  
  const totalEnergy = Number(energyResult.rows[0].total_energy_mwh);
  console.log(`Total Energy on ${DATE_TO_VALIDATE}: ${totalEnergy.toFixed(2)} MWh`);
  console.log('Difficulty:', DIFFICULTY);
  
  console.log('\nBitcoin Daily Summaries:');
  console.log('---------------------------------------');
  console.log('Miner Model    | Bitcoin Mined | BTC/MWh');
  console.log('---------------------------------------');
  
  for (const row of dailySummaryResult.rows) {
    const minerModel = row.miner_model;
    const bitcoinMined = Number(row.bitcoin_mined);
    const btcPerMWh = bitcoinMined / totalEnergy;
    
    console.log(`${minerModel.padEnd(14)} | ${bitcoinMined.toFixed(6).padEnd(13)} | ${btcPerMWh.toFixed(10)}`);
  }
  
  console.log('\nValidation against Calculation Formula (Daily):');
  console.log('---------------------------------------');
  console.log('Miner Model    | Stored BTC  | Calculated BTC | Difference (%)');
  console.log('---------------------------------------');
  
  for (const row of dailySummaryResult.rows) {
    const minerModel = row.miner_model;
    const storedBitcoin = Number(row.bitcoin_mined);
    
    // Calculate using formula
    const calculatedBitcoin = calculateBitcoin(totalEnergy, minerModel, DIFFICULTY);
    
    // Calculate difference percentage
    const difference = ((calculatedBitcoin - storedBitcoin) / storedBitcoin) * 100;
    
    console.log(`${minerModel.padEnd(14)} | ${storedBitcoin.toFixed(6).padEnd(11)} | ${calculatedBitcoin.toFixed(6).padEnd(14)} | ${difference.toFixed(2)}%`);
  }
  
  // Now validate individual hourly records
  console.log('\nValidating Individual Hourly Records:');
  console.log('-------------------------------------------');
  console.log('Random sample of hourly records for M20S:');
  
  const hourlyRecordsResult = await db.execute(sql`
    SELECT 
      hbc.id, 
      hbc.settlement_period, 
      ABS(cr.volume::numeric) as energy,
      hbc.bitcoin_mined,
      hbc.difficulty
    FROM historical_bitcoin_calculations hbc
    JOIN curtailment_records cr ON 
      hbc.settlement_date = cr.settlement_date
      AND hbc.settlement_period = cr.settlement_period
      AND hbc.farm_id = cr.farm_id
    WHERE 
      hbc.settlement_date = ${DATE_TO_VALIDATE}
      AND hbc.miner_model = 'M20S'
    ORDER BY RANDOM()
    LIMIT 5
  `);
  
  console.log('-------------------------------------------');
  console.log('Period | Energy MWh | Stored BTC | Calculated BTC | Diff (%)');
  console.log('-------------------------------------------');
  
  for (const record of hourlyRecordsResult.rows) {
    const period = record.settlement_period;
    const energy = Number(record.energy);
    const storedBtc = Number(record.bitcoin_mined);
    const difficulty = Number(record.difficulty);
    
    // Calculate with the formula
    const calculatedBtc = calculateBitcoin(energy, 'M20S', difficulty);
    
    // Calculate percentage difference
    const diffPct = ((calculatedBtc - storedBtc) / storedBtc) * 100;
    
    console.log(`${String(period).padStart(6)} | ${energy.toFixed(4).padEnd(10)} | ${storedBtc.toFixed(8).padEnd(10)} | ${calculatedBtc.toFixed(8).padEnd(14)} | ${diffPct.toFixed(2)}%`);
  }
  
  // Validate hourly sum vs daily total
  console.log('\nValidating Hourly Sum vs Daily Total:');
  
  const minerModels = dailySummaryResult.rows.map(row => row.miner_model);
  
  for (const minerModel of minerModels) {
    const hourlySum = await db.execute(sql`
      SELECT 
        SUM(bitcoin_mined::numeric) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE 
        settlement_date = ${DATE_TO_VALIDATE}
        AND miner_model = ${minerModel}
    `);
    
    const dailyTotal = dailySummaryResult.rows.find(row => row.miner_model === minerModel)?.bitcoin_mined;
    
    console.log(`${minerModel}:`);
    console.log(`  Sum of hourly records: ${Number(hourlySum.rows[0].total_bitcoin).toFixed(8)}`);
    console.log(`  Daily summary value:   ${Number(dailyTotal).toFixed(8)}`);
    console.log(`  Difference:            ${(Number(hourlySum.rows[0].total_bitcoin) - Number(dailyTotal)).toFixed(8)}`);
  }
  
  console.log('\n=== Validation completed ===');
}

// Run the validation
validateCalculation();
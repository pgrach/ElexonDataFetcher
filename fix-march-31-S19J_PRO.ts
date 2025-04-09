/**
 * Fix Bitcoin calculations for S19J_PRO on March 31, 2025
 * 
 * This script recalculates the bitcoin_mined values for the S19J_PRO miner model
 * on March 31, 2025 using the correct difficulty value.
 */

import { calculateBitcoin } from './server/utils/bitcoin';
import { db } from './db';
import { sql } from 'drizzle-orm';

const DATE_TO_UPDATE = '2025-03-31';
const CORRECT_DIFFICULTY = 113757508810853;
const MINER_MODEL = 'S19J_PRO';

async function fixMarch31S19JPRO() {
  try {
    console.log(`=== Fixing Bitcoin calculations for ${MINER_MODEL} on ${DATE_TO_UPDATE} ===`);
    
    // Verify the difficulty is correct first
    const difficultyResult = await db.execute(sql`
      SELECT DISTINCT difficulty
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${DATE_TO_UPDATE}
      AND miner_model = ${MINER_MODEL}
      LIMIT 1
    `);
    
    const currentDifficulty = difficultyResult.rows[0]?.difficulty;
    console.log(`Current difficulty for ${DATE_TO_UPDATE}: ${currentDifficulty}`);
    
    if (currentDifficulty !== String(CORRECT_DIFFICULTY)) {
      console.log(`Difficulty value for ${DATE_TO_UPDATE} needs to be corrected first.`);
      console.log(`Updating to correct difficulty: ${CORRECT_DIFFICULTY}`);
      
      await db.execute(sql`
        UPDATE historical_bitcoin_calculations
        SET difficulty = ${String(CORRECT_DIFFICULTY)}
        WHERE settlement_date = ${DATE_TO_UPDATE}
        AND miner_model = ${MINER_MODEL}
      `);
      
      console.log('Difficulty updated successfully');
    } else {
      console.log('Difficulty value is already correct');
    }
    
    // Show summary before update
    const beforeSummary = await db.execute(sql`
      SELECT 
        COUNT(*) as record_count,
        SUM(bitcoin_mined::numeric) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${DATE_TO_UPDATE}
      AND miner_model = ${MINER_MODEL}
    `);
    
    console.log(`\nBefore update - ${MINER_MODEL} on ${DATE_TO_UPDATE}:`);
    console.log(`Records: ${beforeSummary.rows[0].record_count}`);
    console.log(`Total Bitcoin: ${beforeSummary.rows[0].total_bitcoin}`);
    
    // Recalculate Bitcoin mined for each record
    console.log(`\nRecalculating Bitcoin mined values...`);
    
    // Get energy data for all records
    const records = await db.execute(sql`
      SELECT 
        hbc.id, 
        ABS(cr.volume::numeric) as energy,
        hbc.miner_model
      FROM historical_bitcoin_calculations hbc
      JOIN curtailment_records cr ON 
        hbc.settlement_date = cr.settlement_date
        AND hbc.settlement_period = cr.settlement_period
        AND hbc.farm_id = cr.farm_id
      WHERE 
        hbc.settlement_date = ${DATE_TO_UPDATE}
        AND hbc.miner_model = ${MINER_MODEL}
    `);
    
    // Process in batches of 50
    const BATCH_SIZE = 50;
    const batches = Math.ceil(records.rows.length / BATCH_SIZE);
    
    for (let i = 0; i < batches; i++) {
      const batch = records.rows.slice(i * BATCH_SIZE, (i + 1) * BATCH_SIZE);
      console.log(`Processing batch ${i+1}/${batches} (${batch.length} records)`);
      
      // Process this batch
      for (const record of batch) {
        const energy = Number(record.energy || 0);
        const newBitcoinMined = calculateBitcoin(energy, record.miner_model, CORRECT_DIFFICULTY);
        
        // Update the record
        await db.execute(sql`
          UPDATE historical_bitcoin_calculations
          SET bitcoin_mined = ${String(newBitcoinMined)}
          WHERE id = ${record.id}
        `);
      }
    }
    
    // Show summary after update
    const afterSummary = await db.execute(sql`
      SELECT 
        COUNT(*) as record_count,
        SUM(bitcoin_mined::numeric) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${DATE_TO_UPDATE}
      AND miner_model = ${MINER_MODEL}
    `);
    
    console.log(`\nAfter update - ${MINER_MODEL} on ${DATE_TO_UPDATE}:`);
    console.log(`Records: ${afterSummary.rows[0].record_count}`);
    console.log(`Total Bitcoin: ${afterSummary.rows[0].total_bitcoin}`);
    
    // Update daily summary
    console.log(`\nUpdating daily summary for ${DATE_TO_UPDATE}...`);
    
    const dailyData = await db.execute(sql`
      SELECT
        SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
        MIN(difficulty::NUMERIC) as difficulty
      FROM
        historical_bitcoin_calculations
      WHERE
        settlement_date = ${DATE_TO_UPDATE}
        AND miner_model = ${MINER_MODEL}
    `);
    
    // Delete existing summary if any
    await db.execute(sql`
      DELETE FROM bitcoin_daily_summaries
      WHERE summary_date = ${DATE_TO_UPDATE}
      AND miner_model = ${MINER_MODEL}
    `);
    
    // Use a standard Bitcoin price of 65,000 USD for value_at_mining
    // This matches the price used in other March 31 calculations
    const bitcoinPrice = 65000;
    const valueAtMining = Number(dailyData.rows[0].total_bitcoin) * bitcoinPrice;
    
    console.log(`Using Bitcoin price of $${bitcoinPrice} for value_at_mining calculation`);
    console.log(`Value at mining: $${valueAtMining.toFixed(2)}`);
    
    // Insert new summary
    await db.execute(sql`
      INSERT INTO bitcoin_daily_summaries (
        summary_date,
        miner_model,
        bitcoin_mined,
        value_at_mining,
        average_difficulty
      ) VALUES (
        ${DATE_TO_UPDATE},
        ${MINER_MODEL},
        ${dailyData.rows[0].total_bitcoin},
        ${String(valueAtMining)},
        ${dailyData.rows[0].difficulty}
      )
    `);
    
    console.log(`Updated daily summary for ${DATE_TO_UPDATE}`);
    
    // Update monthly summary for March 2025
    console.log(`\nUpdating monthly summary for 2025-03...`);
    
    const yearMonth = '2025-03';
    
    const monthlyData = await db.execute(sql`
      SELECT
        SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
        SUM(value_at_mining::NUMERIC) as total_value
      FROM
        bitcoin_daily_summaries
      WHERE
        summary_date >= '2025-03-01'
        AND summary_date <= '2025-03-31'
        AND miner_model = ${MINER_MODEL}
    `);
    
    // Delete existing monthly summary
    await db.execute(sql`
      DELETE FROM bitcoin_monthly_summaries
      WHERE year_month = ${yearMonth}
      AND miner_model = ${MINER_MODEL}
    `);
    
    // Insert new monthly summary
    await db.execute(sql`
      INSERT INTO bitcoin_monthly_summaries (
        year_month,
        miner_model,
        bitcoin_mined,
        value_at_mining
      ) VALUES (
        ${yearMonth},
        ${MINER_MODEL},
        ${monthlyData.rows[0].total_bitcoin},
        ${monthlyData.rows[0].total_value}
      )
    `);
    
    console.log(`Updated monthly summary for ${yearMonth}`);
    
    // Update yearly summary for 2025
    console.log(`\nUpdating yearly summary for 2025...`);
    
    const year = '2025';
    
    const yearlyData = await db.execute(sql`
      SELECT
        SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
        SUM(value_at_mining::NUMERIC) as total_value
      FROM
        bitcoin_monthly_summaries
      WHERE
        year_month LIKE '2025-%'
        AND miner_model = ${MINER_MODEL}
    `);
    
    // Delete existing yearly summary
    await db.execute(sql`
      DELETE FROM bitcoin_yearly_summaries
      WHERE year = ${year}
      AND miner_model = ${MINER_MODEL}
    `);
    
    // Insert new yearly summary
    await db.execute(sql`
      INSERT INTO bitcoin_yearly_summaries (
        year,
        miner_model,
        bitcoin_mined,
        value_at_mining
      ) VALUES (
        ${year},
        ${MINER_MODEL},
        ${yearlyData.rows[0].total_bitcoin},
        ${yearlyData.rows[0].total_value}
      )
    `);
    
    console.log(`Updated yearly summary for ${year}`);
    
    console.log('\n=== Fix completed successfully ===');
  } catch (error) {
    console.error('Error fixing Bitcoin calculations:', error);
  }
}

// Run the fix
fixMarch31S19JPRO();
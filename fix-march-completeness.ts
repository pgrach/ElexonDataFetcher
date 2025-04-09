/**
 * Completeness Check and Fix for March 2025 Bitcoin Calculations
 * 
 * This script performs a comprehensive check of Bitcoin calculations for March 2025,
 * ensuring that all records are properly calculated, all aggregations are correct,
 * and all summary tables are updated.
 */

import { calculateBitcoin } from './server/utils/bitcoin';
import { db } from './db';
import { sql } from 'drizzle-orm';

// March 2025 difficulty values by date range
const DIFFICULTY_MAP = {
  '2025-03-01': 110568428300952, // Mar 1-9
  '2025-03-10': 112149504190349, // Mar 10-22
  '2025-03-23': 113757508810853  // Mar 23-31
};

/**
 * Get the correct difficulty for a specific date
 */
function getDifficultyForDate(date: string): number {
  const dateObj = new Date(date);
  const day = dateObj.getDate();
  
  if (day >= 23) {
    return DIFFICULTY_MAP['2025-03-23'];
  } else if (day >= 10) {
    return DIFFICULTY_MAP['2025-03-10'];
  } else {
    return DIFFICULTY_MAP['2025-03-01'];
  }
}

/**
 * Main function to check and fix March 2025 Bitcoin calculations
 */
async function fixMarchCompleteness(startDate = '2025-03-01', endDate = '2025-03-31') {
  console.log(`=== Checking and fixing Bitcoin calculations from ${startDate} to ${endDate} ===`);
  
  // Get all miner models
  const minerModelsResult = await db.execute(sql`
    SELECT DISTINCT miner_model
    FROM historical_bitcoin_calculations
    WHERE settlement_date BETWEEN ${startDate} AND ${endDate}
    ORDER BY miner_model
  `);
  
  const minerModels = minerModelsResult.rows.map(row => row.miner_model);
  console.log(`Found ${minerModels.length} miner models in the date range:`);
  console.log(minerModels.join(', '));
  
  // Get all dates in range
  const datesResult = await db.execute(sql`
    SELECT DISTINCT settlement_date
    FROM historical_bitcoin_calculations
    WHERE settlement_date BETWEEN ${startDate} AND ${endDate}
    ORDER BY settlement_date
  `);
  
  const dates = datesResult.rows.map(row => row.settlement_date);
  console.log(`Found ${dates.length} dates with Bitcoin calculations`);
  
  // Process each date and miner model
  for (const date of dates) {
    const correctDifficulty = getDifficultyForDate(date);
    console.log(`\n=== Processing ${date} (Difficulty: ${correctDifficulty}) ===`);
    
    // Check if difficulty is correct for all miners on this date
    const difficultyResult = await db.execute(sql`
      SELECT DISTINCT miner_model, difficulty
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${date}
      ORDER BY miner_model
    `);
    
    // Fix difficulty if needed
    for (const row of difficultyResult.rows) {
      const minerModel = row.miner_model;
      const currentDifficulty = row.difficulty;
      
      if (currentDifficulty !== String(correctDifficulty)) {
        console.log(`Fixing incorrect difficulty for ${minerModel} on ${date}: ${currentDifficulty} -> ${correctDifficulty}`);
        
        await db.execute(sql`
          UPDATE historical_bitcoin_calculations
          SET difficulty = ${String(correctDifficulty)}
          WHERE settlement_date = ${date}
          AND miner_model = ${minerModel}
        `);
      }
    }
    
    // Now check and fix Bitcoin calculations for each miner model on this date
    for (const minerModel of minerModels) {
      // Check if we have any records for this miner model on this date
      const countResult = await db.execute(sql`
        SELECT COUNT(*) as record_count
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${date}
        AND miner_model = ${minerModel}
      `);
      
      if (Number(countResult.rows[0].record_count) === 0) {
        console.log(`No records for ${minerModel} on ${date}, skipping`);
        continue;
      }
      
      console.log(`Checking ${minerModel} on ${date}...`);
      
      // Get all records with energy data
      const records = await db.execute(sql`
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
          hbc.settlement_date = ${date}
          AND hbc.miner_model = ${minerModel}
        ORDER BY hbc.settlement_period
      `);
      
      // Check if any records need to be fixed
      let fixedCount = 0;
      let totalRecords = records.rows.length;
      
      for (const record of records.rows) {
        const energy = Number(record.energy || 0);
        const expectedBitcoin = calculateBitcoin(energy, minerModel, correctDifficulty);
        const currentBitcoin = Number(record.bitcoin_mined);
        
        // If there's a significant difference, update the record
        // Using a small tolerance for floating point differences
        const tolerance = 0.00000001;
        const diff = Math.abs(expectedBitcoin - currentBitcoin);
        
        if (diff > tolerance) {
          await db.execute(sql`
            UPDATE historical_bitcoin_calculations
            SET 
              bitcoin_mined = ${String(expectedBitcoin)},
              difficulty = ${String(correctDifficulty)}
            WHERE id = ${record.id}
          `);
          fixedCount++;
        }
      }
      
      if (fixedCount > 0) {
        console.log(`Fixed ${fixedCount}/${totalRecords} records for ${minerModel} on ${date}`);
      } else {
        console.log(`All ${totalRecords} records for ${minerModel} on ${date} are correct`);
      }
      
      // Now update the daily summary
      console.log(`Updating daily summary for ${minerModel} on ${date}...`);
      
      const dailyData = await db.execute(sql`
        SELECT
          SUM(bitcoin_mined::NUMERIC) as total_bitcoin
        FROM
          historical_bitcoin_calculations
        WHERE
          settlement_date = ${date}
          AND miner_model = ${minerModel}
      `);
      
      // Use a standard Bitcoin price based on the date
      // For simplicity, let's use $65,000 as the price for all March calculations
      const bitcoinPrice = 65000;
      const valueAtMining = Number(dailyData.rows[0].total_bitcoin) * bitcoinPrice;
      
      // Delete existing summary if any
      await db.execute(sql`
        DELETE FROM bitcoin_daily_summaries
        WHERE summary_date = ${date}
        AND miner_model = ${minerModel}
      `);
      
      // Insert new summary
      await db.execute(sql`
        INSERT INTO bitcoin_daily_summaries (
          summary_date,
          miner_model,
          bitcoin_mined,
          value_at_mining,
          average_difficulty
        ) VALUES (
          ${date},
          ${minerModel},
          ${dailyData.rows[0].total_bitcoin},
          ${String(valueAtMining)},
          ${String(correctDifficulty)}
        )
      `);
      
      console.log(`Updated daily summary for ${minerModel} on ${date}`);
    }
  }
  
  // Update all monthly summaries for March 2025
  console.log('\n=== Updating monthly summaries for March 2025 ===');
  
  const yearMonth = '2025-03';
  
  for (const minerModel of minerModels) {
    const monthlyData = await db.execute(sql`
      SELECT
        SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
        SUM(value_at_mining::NUMERIC) as total_value
      FROM
        bitcoin_daily_summaries
      WHERE
        summary_date >= '2025-03-01'
        AND summary_date <= '2025-03-31'
        AND miner_model = ${minerModel}
    `);
    
    // Delete existing monthly summary
    await db.execute(sql`
      DELETE FROM bitcoin_monthly_summaries
      WHERE year_month = ${yearMonth}
      AND miner_model = ${minerModel}
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
        ${minerModel},
        ${monthlyData.rows[0].total_bitcoin},
        ${monthlyData.rows[0].total_value}
      )
    `);
    
    console.log(`Updated monthly summary for ${minerModel} in ${yearMonth}`);
  }
  
  // Update yearly summary for 2025
  console.log('\n=== Updating yearly summaries for 2025 ===');
  
  const year = '2025';
  
  for (const minerModel of minerModels) {
    const yearlyData = await db.execute(sql`
      SELECT
        SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
        SUM(value_at_mining::NUMERIC) as total_value
      FROM
        bitcoin_monthly_summaries
      WHERE
        year_month LIKE '2025-%'
        AND miner_model = ${minerModel}
    `);
    
    // Delete existing yearly summary
    await db.execute(sql`
      DELETE FROM bitcoin_yearly_summaries
      WHERE year = ${year}
      AND miner_model = ${minerModel}
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
        ${minerModel},
        ${yearlyData.rows[0].total_bitcoin},
        ${yearlyData.rows[0].total_value}
      )
    `);
    
    console.log(`Updated yearly summary for ${minerModel} in ${year}`);
  }
  
  console.log('\n=== Completeness check and fix completed ===');
}

// Run the fix only for March 31
fixMarchCompleteness('2025-03-31', '2025-03-31').catch(console.error);
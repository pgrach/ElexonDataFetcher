/**
 * Fix Bitcoin calculations for March 31, 2025
 * 
 * This script recalculates the bitcoin_mined values for March 31, 2025 
 * using the correct difficulty value from DynamoDB. Unlike fix-march-31-historical-difficulty.ts,
 * which only updated the difficulty value, this script actually recalculates the Bitcoin amounts.
 */

import { calculateBitcoin } from './server/utils/bitcoin';
import { db } from './db';
import { sql } from 'drizzle-orm';

const DATE_TO_UPDATE = '2025-03-31';
const CORRECT_DIFFICULTY = 113757508810853;

async function fixMarch31Bitcoin() {
  try {
    console.log(`=== Fixing Bitcoin calculations for ${DATE_TO_UPDATE} ===`);
    
    // Verify the difficulty is correct first
    const difficultyResult = await db.execute(sql`
      SELECT DISTINCT difficulty
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${DATE_TO_UPDATE}
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
      `);
      
      console.log('Difficulty updated successfully');
    } else {
      console.log('Difficulty value is already correct');
    }
    
    // Show summary of records before update
    const beforeSummary = await db.execute(sql`
      SELECT 
        miner_model, 
        COUNT(*) as record_count,
        SUM(bitcoin_mined::numeric) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${DATE_TO_UPDATE}
      GROUP BY miner_model
    `);
    
    console.log(`\nBefore update - Bitcoin calculations for ${DATE_TO_UPDATE}:`);
    console.table(beforeSummary.rows);
    
    // OPTIMIZATION: Directly update all records using a more efficient approach
    console.log(`\nPerforming bulk recalculation of Bitcoin values...`);
    
    // For each miner model
    const miner_models = beforeSummary.rows.map(row => row.miner_model);
    
    for (const minerModel of miner_models) {
      console.log(`Processing ${minerModel}...`);
      
      // Skip the bulk update since it doesn't call the calculateBitcoin function
      // We'll do this with the precise per-record approach below
      console.log(`Skipping bulk update for ${minerModel} - will use per-record approach instead`);
    }
    
    // This is a placeholder for now - we'll use per-record calculations instead
    console.log(`Performing precise calculations for each record...`);
    
    // Process each miner model
    for (const minerModel of miner_models) {
      console.log(`Recalculating for ${minerModel}...`);
      
      // Get all records for this miner model
      const records = await db.execute(sql`
        SELECT 
          hbc.id, 
          cr.volume as energy,
          hbc.miner_model
        FROM historical_bitcoin_calculations hbc
        JOIN curtailment_records cr ON 
          hbc.settlement_date = cr.settlement_date
          AND hbc.settlement_period = cr.settlement_period
          AND hbc.farm_id = cr.farm_id
        WHERE 
          hbc.settlement_date = ${DATE_TO_UPDATE}
          AND hbc.miner_model = ${minerModel}
      `);
      
      // Process in batches of 50
      const BATCH_SIZE = 50;
      const batches = Math.ceil(records.rows.length / BATCH_SIZE);
      
      for (let i = 0; i < batches; i++) {
        const batch = records.rows.slice(i * BATCH_SIZE, (i + 1) * BATCH_SIZE);
        console.log(`Processing batch ${i+1}/${batches} (${batch.length} records)`);
        
        // Process this batch
        for (const record of batch) {
          const energy = Math.abs(Number(record.energy || 0));
          const newBitcoinMined = calculateBitcoin(energy, record.miner_model, CORRECT_DIFFICULTY);
          
          // Update the record
          await db.execute(sql`
            UPDATE historical_bitcoin_calculations
            SET bitcoin_mined = ${String(newBitcoinMined)}
            WHERE id = ${record.id}
          `);
        }
      }
    }
    
    // Show summary of records after update
    const afterSummary = await db.execute(sql`
      SELECT 
        miner_model, 
        COUNT(*) as record_count,
        SUM(bitcoin_mined::numeric) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${DATE_TO_UPDATE}
      GROUP BY miner_model
    `);
    
    console.log(`\nAfter update - Bitcoin calculations for ${DATE_TO_UPDATE}:`);
    console.table(afterSummary.rows);
    
    // Update daily summary
    console.log(`\nUpdating daily summaries for ${DATE_TO_UPDATE}...`);
    
    // Get unique miner models for this date
    const minerModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${DATE_TO_UPDATE}
    `);
    
    const minerModels = minerModelsResult.rows.map(row => row.miner_model);
    
    // Use a more efficient approach - update all daily summaries in one go
    await db.execute(sql.raw(`
      -- First delete existing summaries
      DELETE FROM bitcoin_daily_summaries
      WHERE summary_date = '${DATE_TO_UPDATE}';
      
      -- Then insert new summaries for all miner models
      INSERT INTO bitcoin_daily_summaries (
        summary_date,
        miner_model,
        bitcoin_mined,
        average_difficulty
      )
      SELECT
        '${DATE_TO_UPDATE}' as summary_date,
        miner_model,
        SUM(bitcoin_mined::NUMERIC) as bitcoin_mined,
        MIN(difficulty::NUMERIC) as average_difficulty
      FROM
        historical_bitcoin_calculations
      WHERE
        settlement_date = '${DATE_TO_UPDATE}'
      GROUP BY
        miner_model;
    `));
    
    console.log(`Updated daily summaries for ${DATE_TO_UPDATE}`);
    
    // Update monthly summary for March 2025
    console.log(`\nUpdating monthly summary for 2025-03...`);
    
    const yearMonth = '2025-03';
    
    // Use a more efficient approach - update all monthly summaries in one go
    await db.execute(sql.raw(`
      -- First delete existing summaries
      DELETE FROM bitcoin_monthly_summaries
      WHERE year_month = '${yearMonth}';
      
      -- Then insert new summaries for all miner models
      INSERT INTO bitcoin_monthly_summaries (
        year_month,
        miner_model,
        bitcoin_mined,
        average_difficulty
      )
      SELECT
        '${yearMonth}' as year_month,
        miner_model,
        SUM(bitcoin_mined::NUMERIC) as bitcoin_mined,
        AVG(average_difficulty::NUMERIC) as average_difficulty
      FROM
        bitcoin_daily_summaries
      WHERE
        summary_date >= '2025-03-01'
        AND summary_date <= '2025-03-31'
      GROUP BY
        miner_model;
    `));
    
    console.log(`Updated monthly summaries for ${yearMonth}`);
    
    // Update yearly summary for 2025
    console.log(`\nUpdating yearly summary for 2025...`);
    
    const year = '2025';
    
    // Use a more efficient approach - update all yearly summaries in one go
    await db.execute(sql.raw(`
      -- First delete existing summaries
      DELETE FROM bitcoin_yearly_summaries
      WHERE year = '${year}';
      
      -- Then insert new summaries for all miner models
      INSERT INTO bitcoin_yearly_summaries (
        year,
        miner_model,
        bitcoin_mined,
        average_difficulty
      )
      SELECT
        '${year}' as year,
        miner_model,
        SUM(bitcoin_mined::NUMERIC) as bitcoin_mined,
        AVG(average_difficulty::NUMERIC) as average_difficulty
      FROM
        bitcoin_monthly_summaries
      WHERE
        year_month LIKE '2025-%'
      GROUP BY
        miner_model;
    `));
    
    console.log(`Updated yearly summaries for ${year}`);
    
    console.log('\n=== Fix completed successfully ===');
  } catch (error) {
    console.error('Error fixing Bitcoin calculations:', error);
  }
}

// Run the fix
fixMarch31Bitcoin();
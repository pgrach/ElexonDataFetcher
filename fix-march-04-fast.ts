/**
 * Fast fix for March 04 difficulty values
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

const DATE_TO_UPDATE = '2025-03-04';
const CORRECT_DIFFICULTY = '110568428300952';
const INCORRECT_DIFFICULTY = '108105433845147';

async function fixMarchDifficultyFast() {
  console.log(`Starting fast difficulty correction for ${DATE_TO_UPDATE}...`);

  try {
    // Count remaining incorrect records
    const countResult = await db.execute(sql`
      SELECT COUNT(*) as count
      FROM historical_bitcoin_calculations 
      WHERE settlement_date = ${DATE_TO_UPDATE}
      AND difficulty = ${INCORRECT_DIFFICULTY}
    `);
    
    const count = Number(countResult.rows?.[0]?.count || 0);
    console.log(`Found ${count} records with incorrect difficulty value ${INCORRECT_DIFFICULTY}`);
    
    if (count === 0) {
      console.log(`No records to fix.`);
      return;
    }
    
    // Update all records at once
    console.log(`Updating difficulty value for all remaining records...`);
    const updateResult = await db.execute(sql`
      UPDATE historical_bitcoin_calculations
      SET difficulty = ${CORRECT_DIFFICULTY}
      WHERE settlement_date = ${DATE_TO_UPDATE}
      AND difficulty = ${INCORRECT_DIFFICULTY}
    `);
    
    console.log(`Updated difficulty for ${count} records to ${CORRECT_DIFFICULTY}`);
    
    // Now update the daily, monthly, and yearly summaries
    
    // Update daily summary for March 04
    console.log(`\nUpdating daily summary for ${DATE_TO_UPDATE}...`);
    
    // Get all miner models for this date
    const minerModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${DATE_TO_UPDATE}
    `);
    
    if (minerModelsResult.rows && minerModelsResult.rows.length > 0) {
      for (const model of minerModelsResult.rows) {
        const minerModel = model.miner_model;
        
        // Delete existing summary to ensure clean state
        await db.execute(sql`
          DELETE FROM bitcoin_daily_summaries
          WHERE summary_date = ${DATE_TO_UPDATE}
          AND miner_model = ${minerModel}
        `);
        
        // Insert new summary with the correct difficulty
        await db.execute(sql`
          INSERT INTO bitcoin_daily_summaries 
          (summary_date, miner_model, bitcoin_mined, average_difficulty, updated_at)
          SELECT 
            ${DATE_TO_UPDATE},
            ${minerModel},
            SUM(bitcoin_mined::NUMERIC),
            ${CORRECT_DIFFICULTY},
            ${new Date().toISOString()}
          FROM historical_bitcoin_calculations
          WHERE settlement_date = ${DATE_TO_UPDATE}
          AND miner_model = ${minerModel}
        `);
        
        console.log(`Updated daily summary for ${minerModel}`);
      }
    }
    
    // Update monthly summary for March 2025
    console.log(`\nUpdating monthly summary for 2025-03...`);
    const yearMonth = '2025-03';
    
    // Get all miner models for March
    const monthlyModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM historical_bitcoin_calculations
      WHERE EXTRACT(YEAR FROM settlement_date) = 2025
      AND EXTRACT(MONTH FROM settlement_date) = 3
    `);
    
    if (monthlyModelsResult.rows) {
      for (const model of monthlyModelsResult.rows) {
        const minerModel = model.miner_model;
        
        // Delete existing monthly summary
        await db.execute(sql`
          DELETE FROM bitcoin_monthly_summaries
          WHERE year_month = ${yearMonth}
          AND miner_model = ${minerModel}
        `);
        
        // Insert updated monthly summary
        await db.execute(sql`
          INSERT INTO bitcoin_monthly_summaries 
          (year_month, miner_model, bitcoin_mined, updated_at)
          SELECT 
            ${yearMonth},
            ${minerModel},
            SUM(bitcoin_mined::NUMERIC),
            ${new Date().toISOString()}
          FROM historical_bitcoin_calculations
          WHERE EXTRACT(YEAR FROM settlement_date) = 2025
          AND EXTRACT(MONTH FROM settlement_date) = 3
          AND miner_model = ${minerModel}
        `);
        
        console.log(`Updated monthly summary for ${minerModel}`);
      }
    }
    
    // Update yearly summary for 2025
    console.log(`\nUpdating yearly summary for 2025...`);
    const year = '2025';
    
    // Get all miner models with monthly summaries
    const yearlyModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM bitcoin_monthly_summaries
      WHERE year_month LIKE '2025-%'
    `);
    
    if (yearlyModelsResult.rows) {
      for (const model of yearlyModelsResult.rows) {
        const minerModel = model.miner_model;
        
        // Delete existing yearly summary
        await db.execute(sql`
          DELETE FROM bitcoin_yearly_summaries
          WHERE year = ${year}
          AND miner_model = ${minerModel}
        `);
        
        // Insert updated yearly summary
        await db.execute(sql`
          INSERT INTO bitcoin_yearly_summaries 
          (year, miner_model, bitcoin_mined, months_count, updated_at)
          SELECT 
            ${year},
            ${minerModel},
            SUM(bitcoin_mined::NUMERIC),
            COUNT(DISTINCT year_month),
            ${new Date().toISOString()}
          FROM bitcoin_monthly_summaries
          WHERE year_month LIKE '2025-%'
          AND miner_model = ${minerModel}
          GROUP BY miner_model
        `);
        
        console.log(`Updated yearly summary for ${minerModel}`);
      }
    }
    
    console.log(`\nAll updates completed successfully!`);
    
  } catch (error) {
    console.error('Error updating Bitcoin difficulty:', error);
    process.exit(1);
  }
}

// Execute the main function
fixMarchDifficultyFast().then(() => {
  console.log('Script completed successfully!');
  process.exit(0);
}).catch(error => {
  console.error('Script failed:', error);
  process.exit(1);
});
/**
 * Fast fix for March 04 difficulties with value_at_mining calculation
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

const DATE_TO_UPDATE = '2025-03-04';
const CORRECT_DIFFICULTY = '110568428300952';
const INCORRECT_DIFFICULTY = '108105433845147';

// Use NaN for value_at_mining as the existing records show NaN values

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
    
    // Verify all records have the correct difficulty
    const verifyResult = await db.execute(sql`
      SELECT COUNT(*) as count
      FROM historical_bitcoin_calculations 
      WHERE settlement_date = ${DATE_TO_UPDATE}
      AND difficulty != ${CORRECT_DIFFICULTY}
    `);
    
    const incorrectCount = Number(verifyResult.rows?.[0]?.count || 0);
    if (incorrectCount > 0) {
      console.log(`Warning: ${incorrectCount} records still have incorrect difficulty values.`);
    } else {
      console.log(`All records now have correct difficulty of ${CORRECT_DIFFICULTY}.`);
    }
    
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
        
        // Calculate total Bitcoin for this model and date
        const bitcoinResult = await db.execute(sql`
          SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
          FROM historical_bitcoin_calculations
          WHERE settlement_date = ${DATE_TO_UPDATE}
          AND miner_model = ${minerModel}
        `);
        
        if (bitcoinResult.rows && bitcoinResult.rows.length > 0) {
          const totalBitcoin = bitcoinResult.rows[0].total_bitcoin;
          if (totalBitcoin) {
            // Use NaN for value_at_mining as seen in other records
            const valueAtMining = "'NaN'";
            
            // Delete existing summary to ensure clean state
            await db.execute(sql`
              DELETE FROM bitcoin_daily_summaries
              WHERE summary_date = ${DATE_TO_UPDATE}
              AND miner_model = ${minerModel}
            `);
            
            // Insert new summary with the correct difficulty and value_at_mining
            await db.execute(sql`
              INSERT INTO bitcoin_daily_summaries 
              (summary_date, miner_model, bitcoin_mined, value_at_mining, average_difficulty, updated_at)
              VALUES (
                ${DATE_TO_UPDATE},
                ${minerModel},
                ${totalBitcoin},
                ${valueAtMining},
                ${CORRECT_DIFFICULTY},
                ${new Date().toISOString()}
              )
            `);
            
            console.log(`Updated daily summary for ${minerModel}: ${totalBitcoin} BTC, $${valueAtMining}`);
          }
        }
      }
    }
    
    // Update monthly summary for March 2025
    console.log(`\nUpdating monthly summary for 2025-03...`);
    const yearMonth = '2025-03';
    
    // Recalculate monthly summaries from daily summaries
    const monthlyModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM bitcoin_daily_summaries
      WHERE EXTRACT(YEAR FROM summary_date) = 2025
      AND EXTRACT(MONTH FROM summary_date) = 3
    `);
    
    if (monthlyModelsResult.rows) {
      for (const model of monthlyModelsResult.rows) {
        const minerModel = model.miner_model;
        
        // Get monthly totals from daily summaries
        const monthlyResult = await db.execute(sql`
          SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
          FROM bitcoin_daily_summaries
          WHERE EXTRACT(YEAR FROM summary_date) = 2025
          AND EXTRACT(MONTH FROM summary_date) = 3
          AND miner_model = ${minerModel}
        `);
        
        if (monthlyResult.rows && monthlyResult.rows.length > 0) {
          const totalBitcoin = monthlyResult.rows[0].total_bitcoin;
          
          if (totalBitcoin) {
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
              VALUES (
                ${yearMonth},
                ${minerModel},
                ${totalBitcoin},
                ${new Date().toISOString()}
              )
            `);
            
            console.log(`Updated monthly summary for ${minerModel}: ${totalBitcoin} BTC`);
          }
        }
      }
    }
    
    // Update yearly summary for 2025
    console.log(`\nUpdating yearly summary for 2025...`);
    const year = '2025';
    
    // Recalculate yearly summaries from monthly summaries
    const yearlyModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM bitcoin_monthly_summaries
      WHERE year_month LIKE '2025-%'
    `);
    
    if (yearlyModelsResult.rows) {
      for (const model of yearlyModelsResult.rows) {
        const minerModel = model.miner_model;
        
        // Get yearly totals and months count from monthly summaries
        const yearlyResult = await db.execute(sql`
          SELECT 
            SUM(bitcoin_mined::NUMERIC) as total_bitcoin,
            COUNT(*) as months_count
          FROM bitcoin_monthly_summaries
          WHERE year_month LIKE '2025-%'
          AND miner_model = ${minerModel}
          GROUP BY miner_model
        `);
        
        if (yearlyResult.rows && yearlyResult.rows.length > 0) {
          const totalBitcoin = yearlyResult.rows[0].total_bitcoin;
          const monthsCount = yearlyResult.rows[0].months_count;
          
          if (totalBitcoin) {
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
              VALUES (
                ${year},
                ${minerModel},
                ${totalBitcoin},
                ${monthsCount},
                ${new Date().toISOString()}
              )
            `);
            
            console.log(`Updated yearly summary for ${minerModel}: ${totalBitcoin} BTC`);
          }
        }
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
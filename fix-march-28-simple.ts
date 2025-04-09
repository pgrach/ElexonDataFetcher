/**
 * Fix remaining incorrect difficulty records for March 28, 2025
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

const DATE_TO_UPDATE = '2025-03-28';
const CORRECT_DIFFICULTY = '113757508810853';
const INCORRECT_DIFFICULTY = '56000000000000';

async function fixRemainingRecords() {
  console.log(`Starting targeted Bitcoin difficulty correction for ${DATE_TO_UPDATE}...`);

  try {
    // Count incorrect records
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
    
    // Update the difficulty for the incorrect records
    const updateResult = await db.execute(sql`
      UPDATE historical_bitcoin_calculations
      SET difficulty = ${CORRECT_DIFFICULTY}
      WHERE settlement_date = ${DATE_TO_UPDATE}
      AND difficulty = ${INCORRECT_DIFFICULTY}
    `);
    
    console.log(`Updated difficulty for ${count} records to ${CORRECT_DIFFICULTY}`);
    
    // Update daily summary for March 28
    console.log(`Updating daily summary for ${DATE_TO_UPDATE}...`);
    
    // Get unique miner models for this date
    const minerModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${DATE_TO_UPDATE}
    `);
    
    if (minerModelsResult.rows && minerModelsResult.rows.length > 0) {
      for (const model of minerModelsResult.rows) {
        const minerModel = model.miner_model;
        
        // Calculate the total bitcoin for this miner model on this date
        const bitcoinResult = await db.execute(sql`
          SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
          FROM historical_bitcoin_calculations
          WHERE settlement_date = ${DATE_TO_UPDATE}
          AND miner_model = ${minerModel}
        `);
        
        if (bitcoinResult.rows && bitcoinResult.rows.length > 0) {
          const totalBitcoin = bitcoinResult.rows[0].total_bitcoin;
          
          // Update or insert daily summary
          await db.execute(sql`
            DELETE FROM bitcoin_daily_summaries
            WHERE summary_date = ${DATE_TO_UPDATE}
            AND miner_model = ${minerModel}
          `);
          
          await db.execute(sql`
            INSERT INTO bitcoin_daily_summaries 
            (summary_date, miner_model, bitcoin_mined, average_difficulty, updated_at)
            VALUES (
              ${DATE_TO_UPDATE},
              ${minerModel},
              ${totalBitcoin},
              ${CORRECT_DIFFICULTY},
              ${new Date().toISOString()}
            )
          `);
          
          console.log(`Updated daily summary for ${minerModel}: ${totalBitcoin} BTC`);
        }
      }
    }
    
    // Update monthly summary for March 2025
    console.log(`Updating monthly summary for 2025-03...`);
    const yearMonth = '2025-03';
    
    // Get all miner models for March
    const monthlyMinersResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM historical_bitcoin_calculations
      WHERE EXTRACT(YEAR FROM settlement_date) = 2025
      AND EXTRACT(MONTH FROM settlement_date) = 3
    `);
    
    if (monthlyMinersResult.rows && monthlyMinersResult.rows.length > 0) {
      for (const model of monthlyMinersResult.rows) {
        const minerModel = model.miner_model;
        
        // Calculate monthly total
        const monthlyResult = await db.execute(sql`
          SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
          FROM historical_bitcoin_calculations
          WHERE EXTRACT(YEAR FROM settlement_date) = 2025
          AND EXTRACT(MONTH FROM settlement_date) = 3
          AND miner_model = ${minerModel}
        `);
        
        if (monthlyResult.rows && monthlyResult.rows.length > 0) {
          const totalBitcoin = monthlyResult.rows[0].total_bitcoin;
          
          // Update monthly summary
          await db.execute(sql`
            DELETE FROM bitcoin_monthly_summaries
            WHERE year_month = ${yearMonth}
            AND miner_model = ${minerModel}
          `);
          
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
    
    // Update yearly summary for 2025
    console.log(`Updating yearly summary for 2025...`);
    const year = '2025';
    
    // Get all miner models with monthly summaries in 2025
    const yearlyMinersResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM bitcoin_monthly_summaries
      WHERE year_month LIKE '2025-%'
    `);
    
    if (yearlyMinersResult.rows && yearlyMinersResult.rows.length > 0) {
      for (const model of yearlyMinersResult.rows) {
        const minerModel = model.miner_model;
        
        // Calculate yearly total from monthly summaries
        const yearlyResult = await db.execute(sql`
          SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin, COUNT(*) as months_count
          FROM bitcoin_monthly_summaries
          WHERE year_month LIKE '2025-%'
          AND miner_model = ${minerModel}
        `);
        
        if (yearlyResult.rows && yearlyResult.rows.length > 0) {
          const totalBitcoin = yearlyResult.rows[0].total_bitcoin;
          const monthsCount = yearlyResult.rows[0].months_count;
          
          // Update yearly summary
          await db.execute(sql`
            DELETE FROM bitcoin_yearly_summaries
            WHERE year = ${year}
            AND miner_model = ${minerModel}
          `);
          
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
    
    console.log(`All updates completed successfully!`);
    
  } catch (error) {
    console.error('Error updating Bitcoin difficulty:', error);
    process.exit(1);
  }
}

// Execute the main function
fixRemainingRecords().then(() => {
  console.log('Script completed successfully!');
  process.exit(0);
}).catch(error => {
  console.error('Script failed:', error);
  process.exit(1);
});
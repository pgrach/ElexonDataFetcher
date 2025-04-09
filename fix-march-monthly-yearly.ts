/**
 * Fix monthly and yearly summaries for March 2025
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

async function fixSummaries() {
  console.log('Starting to fix monthly and yearly summaries for March 2025...');

  try {
    // Update monthly summary for March 2025
    console.log('\nUpdating monthly summary for 2025-03...');
    const yearMonth = '2025-03';
    
    // Get all miner models from daily summaries
    const monthlyModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM bitcoin_daily_summaries
      WHERE EXTRACT(YEAR FROM summary_date) = 2025
      AND EXTRACT(MONTH FROM summary_date) = 3
    `);
    
    if (monthlyModelsResult.rows) {
      for (const model of monthlyModelsResult.rows) {
        const minerModel = model.miner_model;
        
        // Calculate monthly total from daily summaries
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
            
            // Insert new monthly summary
            await db.execute(sql`
              INSERT INTO bitcoin_monthly_summaries 
              (year_month, miner_model, bitcoin_mined, value_at_mining, updated_at)
              VALUES (
                ${yearMonth},
                ${minerModel},
                ${totalBitcoin},
                'NaN',
                ${new Date().toISOString()}
              )
            `);
            
            console.log(`Updated monthly summary for ${yearMonth} and ${minerModel}: ${totalBitcoin} BTC`);
          }
        }
      }
    }
    
    // Update yearly summary for 2025
    console.log('\nUpdating yearly summary for 2025...');
    const year = '2025';
    
    // Get all miner models from monthly summaries
    const yearlyModelsResult = await db.execute(sql`
      SELECT DISTINCT miner_model
      FROM bitcoin_monthly_summaries
      WHERE year_month LIKE '2025-%'
    `);
    
    if (yearlyModelsResult.rows) {
      for (const model of yearlyModelsResult.rows) {
        const minerModel = model.miner_model;
        
        // Calculate yearly total from monthly summaries
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
            
            // Insert new yearly summary
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
            
            console.log(`Updated yearly summary for ${year} and ${minerModel}: ${totalBitcoin} BTC`);
          }
        }
      }
    }
    
    console.log('\nAll updates completed successfully!');
    
  } catch (error) {
    console.error('Error updating summaries:', error);
    process.exit(1);
  }
}

// Execute the main function
fixSummaries().then(() => {
  console.log('Script completed successfully!');
  process.exit(0);
}).catch(error => {
  console.error('Script failed:', error);
  process.exit(1);
});
/**
 * Fix yearly summaries for 2025
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

async function fixYearlySummaries() {
  console.log('Starting to fix yearly summaries for 2025...');

  try {
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
              (year, miner_model, bitcoin_mined, value_at_mining, updated_at)
              VALUES (
                ${year},
                ${minerModel},
                ${totalBitcoin},
                'NaN',
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
fixYearlySummaries().then(() => {
  console.log('Script completed successfully!');
  process.exit(0);
}).catch(error => {
  console.error('Script failed:', error);
  process.exit(1);
});
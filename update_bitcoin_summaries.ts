/**
 * Update Bitcoin Monthly and Yearly Summaries for 2025-04-03
 * 
 * This script updates the Bitcoin monthly and yearly summaries based on the
 * daily Bitcoin summaries that have been created.
 */

import { db } from './db';
import { 
  bitcoinDailySummaries,
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries
} from './db/schema';
import { and, eq, like, sql } from 'drizzle-orm';

// Configuration
const TARGET_DATE = '2025-04-03';
const TARGET_MONTH = '2025-04';
const TARGET_YEAR = '2025';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Simple logging utility with timestamps
 */
function log(message: string): void {
  const timestamp = new Date().toISOString().substring(11, 19);
  console.log(`[${timestamp}] ${message}`);
}

/**
 * Update Bitcoin monthly summary for all miner models
 */
async function updateBitcoinMonthlySummaries(): Promise<void> {
  log(`Updating Bitcoin monthly summaries for ${TARGET_MONTH}...`);
  
  for (const minerModel of MINER_MODELS) {
    log(`Processing ${minerModel} miner model...`);
    
    // Calculate monthly Bitcoin from daily summaries using SQL directly
    const monthlyResult = await db.execute(sql`
      SELECT SUM(bitcoin_mined::float) as total_bitcoin
      FROM bitcoin_daily_summaries
      WHERE TO_CHAR(summary_date, 'YYYY-MM') = ${TARGET_MONTH}
      AND miner_model = ${minerModel}
    `);
    
    const totalBitcoin = parseFloat(monthlyResult.rows[0]?.total_bitcoin || '0');
    
    log(`Calculated ${totalBitcoin.toFixed(8)} BTC for ${minerModel} in ${TARGET_MONTH}`);
    
    // Update monthly summary
    await db.execute(sql`
      INSERT INTO bitcoin_monthly_summaries 
      (year_month, miner_model, bitcoin_mined)
      VALUES 
      (${TARGET_MONTH}, ${minerModel}, ${totalBitcoin.toString()})
      ON CONFLICT (year_month, miner_model) 
      DO UPDATE SET
        bitcoin_mined = ${totalBitcoin.toString()}
    `);
    
    log(`Updated Bitcoin monthly summary for ${minerModel}`);
  }
}

/**
 * Update Bitcoin yearly summary for all miner models
 */
async function updateBitcoinYearlySummaries(): Promise<void> {
  log(`Updating Bitcoin yearly summaries for ${TARGET_YEAR}...`);
  
  for (const minerModel of MINER_MODELS) {
    log(`Processing ${minerModel} miner model...`);
    
    // Calculate yearly Bitcoin from monthly summaries using SQL directly
    const yearlyResult = await db.execute(sql`
      SELECT SUM(bitcoin_mined::float) as total_bitcoin
      FROM bitcoin_monthly_summaries
      WHERE year_month LIKE ${TARGET_YEAR + '%'}
      AND miner_model = ${minerModel}
    `);
    
    const totalBitcoin = parseFloat(yearlyResult.rows[0]?.total_bitcoin || '0');
    
    log(`Calculated ${totalBitcoin.toFixed(8)} BTC for ${minerModel} in ${TARGET_YEAR}`);
    
    // Update yearly summary
    await db.execute(sql`
      INSERT INTO bitcoin_yearly_summaries 
      (year, miner_model, bitcoin_mined)
      VALUES 
      (${TARGET_YEAR}, ${minerModel}, ${totalBitcoin.toString()})
      ON CONFLICT (year, miner_model) 
      DO UPDATE SET
        bitcoin_mined = ${totalBitcoin.toString()}
    `);
    
    log(`Updated Bitcoin yearly summary for ${minerModel}`);
  }
}

// Execute the updates
async function updateSummaries(): Promise<void> {
  try {
    await updateBitcoinMonthlySummaries();
    await updateBitcoinYearlySummaries();
  } catch (error) {
    log(`Error updating Bitcoin summaries: ${(error as Error).message}`);
    throw error;
  }
}

updateSummaries()
  .then(() => {
    console.log('\nBitcoin summaries updated successfully.');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\nBitcoin summaries update failed with error:', error);
    process.exit(1);
  });
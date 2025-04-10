/**
 * Update Bitcoin Monthly and Yearly Summaries
 * 
 * This script updates the Bitcoin monthly and yearly summaries
 * after we have recalculated the Bitcoin values for April 3, 2025.
 */

import { db } from './db';
import { bitcoinDailySummaries, bitcoinMonthlySummaries, bitcoinYearlySummaries } from './db/schema';
import { eq, sql, like } from 'drizzle-orm';
import * as fs from 'fs';

// Configuration
const TARGET_MONTH = '2025-04';
const TARGET_YEAR = '2025';
const LOG_FILE_PATH = `./logs/update_summaries_${new Date().toISOString().replace(/:/g, '-')}.log`;
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Simple logging utility with timestamps
 */
function log(message: string): void {
  const timestamp = new Date().toISOString().substring(11, 19);
  const logMessage = `[${timestamp}] ${message}`;
  console.log(logMessage);
  
  // Append to log file
  fs.appendFileSync(LOG_FILE_PATH, logMessage + '\n');
}

/**
 * Update monthly summary for a specific miner model
 */
async function updateMonthlySummary(minerModel: string): Promise<void> {
  log(`Updating monthly summary for ${TARGET_MONTH} and model ${minerModel}...`);
  
  // Calculate total Bitcoin mined in the month
  const result = await db
    .select({
      totalBitcoin: sql<string>`SUM(${bitcoinDailySummaries.bitcoinMined}::numeric)`
    })
    .from(bitcoinDailySummaries)
    .where(sql`${bitcoinDailySummaries.summaryDate}::text LIKE ${TARGET_MONTH + '%'} AND ${bitcoinDailySummaries.minerModel} = ${minerModel}`);
  
  const totalBitcoin = parseFloat(result[0]?.totalBitcoin || '0');
  
  if (totalBitcoin <= 0) {
    log(`No Bitcoin data found for ${TARGET_MONTH} and model ${minerModel}`);
    return;
  }
  
  log(`Total Bitcoin for ${TARGET_MONTH} and model ${minerModel}: ${totalBitcoin.toFixed(8)} BTC`);
  
  // Check if monthly summary already exists
  const existingSummary = await db
    .select()
    .from(bitcoinMonthlySummaries)
    .where(sql`${bitcoinMonthlySummaries.yearMonth} = ${TARGET_MONTH} AND ${bitcoinMonthlySummaries.minerModel} = ${minerModel}`);
  
  if (existingSummary.length > 0) {
    // Update existing summary
    await db.execute(sql`
      UPDATE bitcoin_monthly_summaries
      SET bitcoin_mined = ${totalBitcoin.toString()}, updated_at = NOW()
      WHERE year_month = ${TARGET_MONTH} AND miner_model = ${minerModel}
    `);
    
    log(`Updated existing monthly summary for ${TARGET_MONTH} and model ${minerModel}`);
  } else {
    // Create new summary
    await db.execute(sql`
      INSERT INTO bitcoin_monthly_summaries (year_month, miner_model, bitcoin_mined, created_at, updated_at)
      VALUES (${TARGET_MONTH}, ${minerModel}, ${totalBitcoin.toString()}, NOW(), NOW())
    `);
    
    log(`Created new monthly summary for ${TARGET_MONTH} and model ${minerModel}`);
  }
}

/**
 * Update yearly summary for a specific miner model
 */
async function updateYearlySummary(minerModel: string): Promise<void> {
  log(`Updating yearly summary for ${TARGET_YEAR} and model ${minerModel}...`);
  
  // Calculate total Bitcoin mined in the year from monthly summaries
  const result = await db
    .select({
      totalBitcoin: sql<string>`SUM(${bitcoinMonthlySummaries.bitcoinMined}::numeric)`
    })
    .from(bitcoinMonthlySummaries)
    .where(sql`${bitcoinMonthlySummaries.yearMonth} LIKE ${TARGET_YEAR + '-%'} AND ${bitcoinMonthlySummaries.minerModel} = ${minerModel}`);
  
  const totalBitcoin = parseFloat(result[0]?.totalBitcoin || '0');
  
  if (totalBitcoin <= 0) {
    log(`No Bitcoin data found for ${TARGET_YEAR} and model ${minerModel}`);
    return;
  }
  
  log(`Total Bitcoin for ${TARGET_YEAR} and model ${minerModel}: ${totalBitcoin.toFixed(8)} BTC`);
  
  // Check if yearly summary already exists
  const existingSummary = await db
    .select()
    .from(bitcoinYearlySummaries)
    .where(sql`${bitcoinYearlySummaries.year} = ${TARGET_YEAR} AND ${bitcoinYearlySummaries.minerModel} = ${minerModel}`);
  
  if (existingSummary.length > 0) {
    // Update existing summary
    await db.execute(sql`
      UPDATE bitcoin_yearly_summaries
      SET bitcoin_mined = ${totalBitcoin.toString()}, updated_at = NOW()
      WHERE year = ${TARGET_YEAR} AND miner_model = ${minerModel}
    `);
    
    log(`Updated existing yearly summary for ${TARGET_YEAR} and model ${minerModel}`);
  } else {
    // Create new summary
    await db.execute(sql`
      INSERT INTO bitcoin_yearly_summaries (year, miner_model, bitcoin_mined, created_at, updated_at)
      VALUES (${TARGET_YEAR}, ${minerModel}, ${totalBitcoin.toString()}, NOW(), NOW())
    `);
    
    log(`Created new yearly summary for ${TARGET_YEAR} and model ${minerModel}`);
  }
}

/**
 * Run the update process
 */
async function runUpdate(): Promise<void> {
  try {
    log(`Starting summary updates for ${TARGET_MONTH} and ${TARGET_YEAR}...`);
    
    // Update monthly summaries
    for (const minerModel of MINER_MODELS) {
      await updateMonthlySummary(minerModel);
    }
    
    // Update yearly summaries
    for (const minerModel of MINER_MODELS) {
      await updateYearlySummary(minerModel);
    }
    
    log(`Summary updates completed successfully`);
  } catch (error) {
    log(`Error during summary updates: ${(error as Error).message}`);
    throw error;
  }
}

// Create logs directory if it doesn't exist
if (!fs.existsSync('./logs')) {
  fs.mkdirSync('./logs');
}

// Execute the update process
runUpdate()
  .then(() => {
    console.log('\nSummary updates completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\nSummary updates failed:', error);
    process.exit(1);
  });
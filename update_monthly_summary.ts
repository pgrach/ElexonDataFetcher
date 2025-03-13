/**
 * Update Monthly Bitcoin Summary Script
 * 
 * This script recalculates and updates the monthly Bitcoin summaries for March 2025
 * after the corrections to the 2025-03-12 curtailment data.
 * 
 * Usage:
 *   npx tsx update_monthly_summary.ts
 */

import pg from 'pg';
import { db } from './db';
import { bitcoinMonthlySummaries, bitcoinYearlySummaries } from './db/schema';
import { eq, and } from 'drizzle-orm';

// Database connection pool with optimized settings
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL,
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 15000,
  query_timeout: 20000,
  allowExitOnIdle: true
});

const minerModels = ['S19J_PRO', 'S9', 'M20S'];
const yearMonth = '2025-03';
const year = '2025';

/**
 * Update monthly Bitcoin summary for the given year-month
 */
async function updateMonthlyBitcoinSummary(yearMonth: string): Promise<void> {
  console.log(`Updating monthly Bitcoin summary for ${yearMonth}...`);
  
  for (const minerModel of minerModels) {
    console.log(`Calculating monthly Bitcoin summary for ${yearMonth} with ${minerModel}`);
    
    try {
      // Get total Bitcoin mined for this month and miner model
      const result = await db.execute(
        `SELECT SUM(bitcoin_mined) as total_bitcoin
         FROM historical_bitcoin_calculations
         WHERE DATE_TRUNC('month', settlement_date::date) = DATE_TRUNC('month', $1::date)
         AND miner_model = $2`,
        [yearMonth + '-01', minerModel]
      );
      
      const bitcoinMined = result.rows[0]?.total_bitcoin || 0;
      
      // Update the monthly summary
      await db.update(bitcoinMonthlySummaries)
        .set({ 
          bitcoinMined,
          updatedAt: new Date()
        })
        .where(
          and(
            eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
            eq(bitcoinMonthlySummaries.minerModel, minerModel)
          )
        );
      
      console.log(`Updated monthly summary for ${yearMonth}: ${bitcoinMined} BTC`);
    } catch (error) {
      console.error(`Error updating monthly summary for ${yearMonth} with ${minerModel}:`, error);
    }
  }
}

/**
 * Update yearly Bitcoin summary for the given year
 */
async function updateYearlyBitcoinSummary(year: string): Promise<void> {
  console.log(`Updating yearly Bitcoin summary for ${year}...`);
  
  for (const minerModel of minerModels) {
    console.log(`Calculating yearly Bitcoin summary for ${year} with ${minerModel}`);
    
    try {
      // Get total Bitcoin mined for this year and miner model
      const result = await db.execute(
        `SELECT SUM(bitcoin_mined) as total_bitcoin
         FROM bitcoin_monthly_summaries
         WHERE year_month LIKE $1 || '%'
         AND miner_model = $2`,
        [year, minerModel]
      );
      
      const monthlySummaries = await db.query.bitcoinMonthlySummaries.findMany({
        where: and(
          eq(bitcoinMonthlySummaries.minerModel, minerModel),
          bitcoinMonthlySummaries.yearMonth.like(`${year}%`)
        )
      });
      
      console.log(`Found ${monthlySummaries.length} monthly summaries for ${year}`);
      
      const bitcoinMined = result.rows[0]?.total_bitcoin || 0;
      
      // Update the yearly summary
      await db.update(bitcoinYearlySummaries)
        .set({ 
          bitcoinMined,
          updatedAt: new Date()
        })
        .where(
          and(
            eq(bitcoinYearlySummaries.year, year),
            eq(bitcoinYearlySummaries.minerModel, minerModel)
          )
        );
      
      console.log(`Updated yearly summary for ${year}: ${bitcoinMined} BTC with ${minerModel}`);
    } catch (error) {
      console.error(`Error updating yearly summary for ${year} with ${minerModel}:`, error);
    }
  }
}

async function main() {
  try {
    console.log('=== Updating Monthly and Yearly Bitcoin Summaries ===');
    
    // Update monthly summary for March 2025
    await updateMonthlyBitcoinSummary(yearMonth);
    
    // Update yearly summary for 2025
    await updateYearlyBitcoinSummary(year);
    
    console.log('=== Update Complete ===');
  } catch (error) {
    console.error('Error in main function:', error);
  } finally {
    // Close the database pool
    await pool.end();
    process.exit(0);
  }
}

main();
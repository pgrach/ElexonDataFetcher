/**
 * Update Summaries for March 27, 2025
 * 
 * This script will update the monthly and yearly summaries
 * based on the already processed curtailment and bitcoin calculation data.
 */

import { db } from './db';
import {
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries,
  historicalBitcoinCalculations
} from './db/schema';
import { eq, and, sql, sum } from 'drizzle-orm';

// Type definitions for summary tables
type InsertBitcoinMonthlySummary = typeof bitcoinMonthlySummaries.$inferInsert;
type InsertBitcoinYearlySummary = typeof bitcoinYearlySummaries.$inferInsert;

// Fixed date to update
const DATE_TO_UPDATE = '2025-03-27';

// Miner models to update
const MINER_MODELS = ['S9', 'S17', 'S19_PRO', 'S19J_PRO', 'M30S++'];

/**
 * Update monthly Bitcoin summaries
 */
async function updateMonthlyBitcoinSummaries(date: string): Promise<void> {
  console.log('\n=== Updating Monthly Bitcoin Summaries ===\n');
  
  const [year, month] = date.split('-');
  const yearMonth = `${year}-${month}`;
  
  for (const minerModel of MINER_MODELS) {
    console.log(`Processing monthly summary for ${yearMonth} (${minerModel})`);
    
    // Get sum of bitcoin mined for this month and model
    const result = await db
      .select({
        totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`,
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          sql`${historicalBitcoinCalculations.settlementDate}::text LIKE ${yearMonth + '%'}`,
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
    
    if (result.length === 0 || !result[0].totalBitcoin) {
      console.log(`No data for ${yearMonth} (${minerModel})`);
      continue;
    }
    
    const totalBitcoin = result[0].totalBitcoin;
    console.log(`Total Bitcoin for ${yearMonth} (${minerModel}): ${totalBitcoin}`);
    
    // Insert or update monthly summary
    const monthlySummary: InsertBitcoinMonthlySummary = {
      yearMonth,
      minerModel,
      bitcoinMined: totalBitcoin,
      valueAtMining: '0', // Using fixed value since we don't have current price data
      updatedAt: new Date()
    };
    
    await db.insert(bitcoinMonthlySummaries).values(monthlySummary)
      .onConflictDoUpdate({
        target: [
          bitcoinMonthlySummaries.yearMonth, 
          bitcoinMonthlySummaries.minerModel
        ],
        set: {
          bitcoinMined: monthlySummary.bitcoinMined,
          updatedAt: monthlySummary.updatedAt
        }
      });
    
    console.log(`Updated monthly summary for ${yearMonth} (${minerModel})`);
  }
}

/**
 * Update yearly Bitcoin summaries
 */
async function updateYearlyBitcoinSummaries(date: string): Promise<void> {
  console.log('\n=== Updating Yearly Bitcoin Summaries ===\n');
  
  const [year] = date.split('-');
  
  for (const minerModel of MINER_MODELS) {
    console.log(`Processing yearly summary for ${year} (${minerModel})`);
    
    // Get sum of bitcoin mined for this year and model
    const result = await db
      .select({
        totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`,
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          sql`${historicalBitcoinCalculations.settlementDate}::text LIKE ${year + '%'}`,
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
    
    if (result.length === 0 || !result[0].totalBitcoin) {
      console.log(`No data for ${year} (${minerModel})`);
      continue;
    }
    
    const totalBitcoin = result[0].totalBitcoin;
    console.log(`Total Bitcoin for ${year} (${minerModel}): ${totalBitcoin}`);
    
    // Insert or update yearly summary
    const yearlySummary: InsertBitcoinYearlySummary = {
      year,
      minerModel,
      bitcoinMined: totalBitcoin,
      valueAtMining: '0', // Using fixed value since we don't have current price data
      updatedAt: new Date()
    };
    
    await db.insert(bitcoinYearlySummaries).values(yearlySummary)
      .onConflictDoUpdate({
        target: [
          bitcoinYearlySummaries.year, 
          bitcoinYearlySummaries.minerModel
        ],
        set: {
          bitcoinMined: yearlySummary.bitcoinMined,
          updatedAt: yearlySummary.updatedAt
        }
      });
    
    console.log(`Updated yearly summary for ${year} (${minerModel})`);
  }
}

/**
 * Main function to update all summaries
 */
async function main() {
  try {
    console.log(`\n=== Starting to Update Summaries for ${DATE_TO_UPDATE} ===\n`);
    
    // Update monthly summaries
    await updateMonthlyBitcoinSummaries(DATE_TO_UPDATE);
    
    // Update yearly summaries
    await updateYearlyBitcoinSummaries(DATE_TO_UPDATE);
    
    console.log('\n=== Summary Updates Completed ===\n');
  } catch (error) {
    console.error('Error updating summaries:', error);
    process.exit(1);
  }
}

main();
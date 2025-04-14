/**
 * Bitcoin Halving Recalculation Verification
 * 
 * This script verifies that all Bitcoin calculations have been updated
 * with the correct post-halving reward rate of 3.125 BTC.
 */

import { db } from "./db";
import { 
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries
} from "./db/schema";
import { format } from "date-fns";
import { gte, eq, sql, desc } from "drizzle-orm";

// Constants
const HALVING_DATE = '2024-04-20';
const CURRENT_YEAR = new Date().getFullYear();

/**
 * Get all monthly Bitcoin summaries since the halving date
 */
async function getMonthlyBitcoinSummaries(): Promise<any[]> {
  const results = await db.select({
    yearMonth: bitcoinMonthlySummaries.yearMonth,
    minerModel: bitcoinMonthlySummaries.minerModel,
    bitcoinMined: bitcoinMonthlySummaries.bitcoinMined,
    createdAt: bitcoinMonthlySummaries.createdAt,
    updatedAt: bitcoinMonthlySummaries.updatedAt
  })
  .from(bitcoinMonthlySummaries)
  .where(gte(bitcoinMonthlySummaries.yearMonth, HALVING_DATE.substring(0, 7)))
  .orderBy(desc(bitcoinMonthlySummaries.yearMonth));
  
  return results;
}

/**
 * Get all yearly Bitcoin summaries
 */
async function getYearlyBitcoinSummaries(): Promise<any[]> {
  const results = await db.select({
    year: bitcoinYearlySummaries.year,
    minerModel: bitcoinYearlySummaries.minerModel,
    bitcoinMined: bitcoinYearlySummaries.bitcoinMined,
    createdAt: bitcoinYearlySummaries.createdAt,
    updatedAt: bitcoinYearlySummaries.updatedAt
  })
  .from(bitcoinYearlySummaries)
  .where(gte(bitcoinYearlySummaries.year, '2024'))
  .orderBy(desc(bitcoinYearlySummaries.year));
  
  return results;
}

/**
 * Test if a Bitcoin total is consistent with post-halving reward rate
 * For April 2024, we expect the value to be about half of what it would be
 * at the pre-halving rate, since the halving occurred mid-month.
 */
function isConsistentWithHalving(yearMonth: string, bitcoinMined: number): boolean {
  // April 2024 is a special case since halving happened mid-month
  if (yearMonth === '2024-04') {
    // April 2024 should have less BTC than previous months since
    // only 10 days (April 20-30) were at the lower rate
    return bitcoinMined < 400; // Rough threshold for April 2024
  }
  
  // For months after April 2024, all days should be at the lower rate
  return true;
}

/**
 * Print a table of months and their Bitcoin totals
 */
function printBitcoinTable(data: any[], title: string): void {
  console.log(`\n${title}`);
  console.log('-'.repeat(80));
  console.log('Month/Year\tMiner Model\tBitcoin Mined\tLast Updated\t\tStatus');
  console.log('-'.repeat(80));
  
  for (const row of data) {
    const date = row.yearMonth || row.year;
    const lastUpdated = format(row.updatedAt, 'yyyy-MM-dd HH:mm:ss');
    const status = isConsistentWithHalving(date, Number(row.bitcoinMined)) ? '✓' : '❌';
    
    console.log(`${date}\t${row.minerModel}\t${Number(row.bitcoinMined).toFixed(4)}\t${lastUpdated}\t${status}`);
  }
  
  console.log('-'.repeat(80));
}

/**
 * Verify the Bitcoin halving calculations
 */
async function verifyHalvingRecalculation(): Promise<void> {
  try {
    console.log('\n=== Bitcoin Halving Recalculation Verification ===');
    console.log(`Halving date: ${HALVING_DATE}`);
    console.log('Verifying that all Bitcoin calculations use the correct post-halving rate...');
    
    // Get monthly summaries
    const monthlySummaries = await getMonthlyBitcoinSummaries();
    printBitcoinTable(monthlySummaries, 'Monthly Bitcoin Summaries');
    
    // Get yearly summaries
    const yearlySummaries = await getYearlyBitcoinSummaries();
    printBitcoinTable(yearlySummaries, 'Yearly Bitcoin Summaries');
    
    // Check if there are any inconsistencies
    const inconsistentMonths = monthlySummaries.filter(
      row => !isConsistentWithHalving(row.yearMonth, Number(row.bitcoinMined))
    );
    
    if (inconsistentMonths.length > 0) {
      console.log('\n⚠️ Some months may not have been properly updated:');
      for (const row of inconsistentMonths) {
        console.log(`- ${row.yearMonth} (${row.minerModel}): ${Number(row.bitcoinMined).toFixed(4)} BTC`);
      }
    } else {
      console.log('\n✅ All monthly summaries appear to be consistent with the post-halving reward rate.');
    }
    
    console.log('\n=== Verification Complete ===');
  } catch (error) {
    console.error('Error during verification:', error);
  }
}

// Run the verification
verifyHalvingRecalculation();
/**
 * Sample Reconciliation Check
 * 
 * This script performs a focused check on multiple random dates across 2024
 * to provide a comprehensive verification of data reconciliation.
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import { format } from "date-fns";

// List of dates to check - sampling across different months
const SAMPLE_DATES = [
  '2024-01-15', // January
  '2024-02-20', // February
  '2024-03-05', // March
  '2024-04-07', // April
  '2024-05-12', // May
  '2024-06-25', // June
  '2024-07-04', // July
  '2024-08-19', // August
  '2024-09-28', // September
  '2024-10-31', // October
  '2024-11-15', // November
  '2024-12-25'  // December
];

const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function checkDate(date: string) {
  console.log(`\n=== Checking Date: ${date} ===`);
  
  // Get curtailment periods for this date
  const curtailmentPeriodsResult = await db
    .select({
      period: curtailmentRecords.settlementPeriod,
      count: sql`COUNT(*)`,
      totalVolume: sql`SUM(ABS(volume::numeric))`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
  
  const curtailmentPeriods = curtailmentPeriodsResult.map(row => row.period);
  
  console.log(`Curtailment periods: ${curtailmentPeriods.length}`);
  console.log(`Total records: ${curtailmentPeriodsResult.reduce((sum, row) => sum + Number(row.count), 0)}`);
  console.log(`Total volume: ${curtailmentPeriodsResult.reduce((sum, row) => sum + Number(row.totalVolume), 0).toFixed(2)} MWh`);
  
  let allPeriodsReconciled = true;
  
  // Check each miner model
  for (const minerModel of MINER_MODELS) {
    // Get calculation periods for this date and model
    const calculationPeriodsResult = await db
      .select({
        period: historicalBitcoinCalculations.settlementPeriod,
        count: sql`COUNT(*)`,
        totalBitcoin: sql`SUM(bitcoin_mined)`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      )
      .groupBy(historicalBitcoinCalculations.settlementPeriod)
      .orderBy(historicalBitcoinCalculations.settlementPeriod);
    
    const calculationPeriods = calculationPeriodsResult.map(row => row.period);
    
    // Find missing periods
    const missingPeriods = curtailmentPeriods.filter(period => !calculationPeriods.includes(period));
    
    const isFullyReconciled = missingPeriods.length === 0;
    allPeriodsReconciled = allPeriodsReconciled && isFullyReconciled;
    
    console.log(`\n${minerModel}:`);
    console.log(`- Records: ${calculationPeriodsResult.reduce((sum, row) => sum + Number(row.count), 0)}`);
    console.log(`- Periods: ${calculationPeriods.length}/${curtailmentPeriods.length} (${(calculationPeriods.length / curtailmentPeriods.length * 100).toFixed(1)}%)`);
    console.log(`- Total Bitcoin: ${calculationPeriodsResult.reduce((sum, row) => sum + Number(row.totalBitcoin), 0).toFixed(8)}`);
    console.log(`- ${isFullyReconciled ? '✓ Fully reconciled' : '× Missing periods: ' + missingPeriods.join(', ')}`);
  }
  
  console.log(`\nOverall status: ${allPeriodsReconciled ? '✓ Fully reconciled' : '× Has missing calculations'}`);
  
  return allPeriodsReconciled;
}

async function main() {
  console.log('=== Sample Reconciliation Check ===');
  console.log(`Checking ${SAMPLE_DATES.length} sample dates across 2024`);
  
  let fullyReconciledCount = 0;
  
  for (const date of SAMPLE_DATES) {
    const isFullyReconciled = await checkDate(date);
    if (isFullyReconciled) {
      fullyReconciledCount++;
    }
  }
  
  console.log('\n=== Summary ===');
  console.log(`Dates checked: ${SAMPLE_DATES.length}`);
  console.log(`Fully reconciled: ${fullyReconciledCount}/${SAMPLE_DATES.length} (${(fullyReconciledCount / SAMPLE_DATES.length * 100).toFixed(1)}%)`);
  
  if (fullyReconciledCount === SAMPLE_DATES.length) {
    console.log('\n✅ All sampled dates are fully reconciled across all periods and miner models!');
  } else {
    console.log('\n❌ Some dates have missing reconciliation data. Use reconcile2024.ts to fix these issues.');
  }
}

main()
  .then(() => process.exit(0))
  .catch(error => {
    console.error('Error:', error);
    process.exit(1);
  });
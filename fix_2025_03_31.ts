/**
 * Fix corrupted data for 2025-03-31
 * 
 * This script:
 * 1. Deletes all curtailment records for 2025-03-31
 * 2. Re-fetches data from Elexon API for all 48 periods
 * 3. Re-runs Bitcoin calculations for all miner models
 * 4. Updates monthly and yearly summaries
 */

import { processAllPeriods } from './process_all_periods';
import { processFullCascade } from './process_bitcoin_optimized';
import { db } from './db';
import { curtailmentRecords, historicalBitcoinCalculations, dailySummaries } from './db/schema';
import { eq } from 'drizzle-orm';

async function main() {
  try {
    const dateToFix = '2025-03-31';
    
    console.log(`\n=== Starting Data Fix for ${dateToFix} ===\n`);
    
    // Step 1: Delete existing curtailment records
    console.log(`Deleting existing curtailment records for ${dateToFix}...`);
    const deletedCurtailment = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, dateToFix))
      .returning();
    console.log(`Deleted ${deletedCurtailment.length} curtailment records`);
    
    // Step 2: Delete existing Bitcoin calculations
    console.log(`Deleting existing Bitcoin calculations for ${dateToFix}...`);
    const deletedCalculations = await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, dateToFix))
      .returning();
    console.log(`Deleted ${deletedCalculations.length} Bitcoin calculations`);
    
    // Step 3: Delete daily summary
    console.log(`Deleting daily summary for ${dateToFix}...`);
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.summaryDate, dateToFix));
    
    // Step 4: Process all periods to fetch fresh data from Elexon API
    console.log(`\nFetching fresh data from Elexon API...`);
    const curtailmentResult = await processAllPeriods(dateToFix);
    
    if (curtailmentResult.totalRecords === 0) {
      console.log(`No curtailment data found for ${dateToFix} in Elexon API`);
      return;
    }
    
    console.log(`\nCurtailment data fetched:`);
    console.log(`- Records: ${curtailmentResult.totalRecords}`);
    console.log(`- Periods: ${curtailmentResult.totalPeriods}/48`);
    console.log(`- Total Volume: ${curtailmentResult.totalVolume.toFixed(2)} MWh`);
    console.log(`- Total Payment: £${curtailmentResult.totalPayment.toFixed(2)}`);
    
    // Step 5: Process Bitcoin calculations and update summaries
    console.log(`\nProcessing Bitcoin calculations and updating summaries...`);
    await processFullCascade(dateToFix);
    
    console.log(`\n=== Data Fix Complete for ${dateToFix} ===\n`);
    
    // Step 6: Verify the fixed data
    const verifyRecords = await db.query.curtailmentRecords.findMany({
      where: eq(curtailmentRecords.settlementDate, dateToFix)
    });
    
    const verifyCalculations = await db.query.historicalBitcoinCalculations.findMany({
      where: eq(historicalBitcoinCalculations.settlementDate, dateToFix)
    });
    
    console.log(`Verification Results:`);
    console.log(`- Curtailment Records: ${verifyRecords.length}`);
    console.log(`- Bitcoin Calculations: ${verifyCalculations.length}`);
    
    // Group calculations by miner model for additional verification
    const calculationsByModel = verifyCalculations.reduce((acc, calc) => {
      const model = calc.minerModel;
      if (!acc[model]) {
        acc[model] = {
          count: 0,
          bitcoin: 0
        };
      }
      acc[model].count++;
      acc[model].bitcoin += Number(calc.bitcoinMined);
      return acc;
    }, {} as Record<string, { count: number, bitcoin: number }>);
    
    console.log(`\nBitcoin Calculations by Miner Model:`);
    for (const [model, stats] of Object.entries(calculationsByModel)) {
      console.log(`- ${model}: ${stats.count} records, ${stats.bitcoin.toFixed(8)} BTC`);
    }
    
  } catch (error) {
    console.error('Error fixing data:', error);
    process.exit(1);
  }
}

main();
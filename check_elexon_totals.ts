/**
 * Quick Check of Elexon Data Totals
 * 
 * This script compares database totals with the expected Elexon API totals
 * to identify any significant differences.
 */

import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";
import { processDailyCurtailment } from "./server/services/curtailment_enhanced";

const TARGET_DATE = '2025-03-24'; 

// Known totals from Elexon API
// As provided by the user
const EXPECTED_ELEXON_TOTALS = {
  volume: 23810.26, // MWh
  payment: 243596.03 // £
};

async function getDatabaseStats(date: string) {
  try {
    // Get totals from curtailment_records
    const curtailmentTotals = await db
      .select({
        recordCount: sql<number>`COUNT(*)::int`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)::int`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(ABS(payment::numeric))::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    // Get totals from daily_summaries
    const summaryTotals = await db
      .select({
        totalVolume: sql<string>`${dailySummaries.totalCurtailedEnergy}`,
        totalPayment: sql<string>`ABS(${dailySummaries.totalPayment}::numeric)::text`
      })
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date));
    
    return {
      curtailment: curtailmentTotals[0],
      summary: summaryTotals[0]
    };
  } catch (error) {
    console.error('Error getting database stats:', error);
    throw error;
  }
}

async function main() {
  try {
    console.log(`\n=== Checking Elexon Data Totals for ${TARGET_DATE} ===\n`);
    
    // Get database stats
    console.log('Fetching database stats...');
    const dbStats = await getDatabaseStats(TARGET_DATE);
    
    console.log('\nCurtailment Records Totals:');
    console.log(`- Record Count: ${dbStats.curtailment.recordCount}`);
    console.log(`- Period Count: ${dbStats.curtailment.periodCount}`);
    console.log(`- Farm Count: ${dbStats.curtailment.farmCount}`);
    console.log(`- Total Volume: ${Number(dbStats.curtailment.totalVolume).toFixed(2)} MWh`);
    console.log(`- Total Payment: £${Number(dbStats.curtailment.totalPayment).toFixed(2)}`);
    
    console.log('\nDaily Summary Totals:');
    console.log(`- Total Volume: ${Number(dbStats.summary.totalVolume).toFixed(2)} MWh`);
    console.log(`- Total Payment: £${Number(dbStats.summary.totalPayment).toFixed(2)}`);
    
    console.log('\nExpected Elexon API Totals:');
    console.log(`- Total Volume: ${EXPECTED_ELEXON_TOTALS.volume.toFixed(2)} MWh`);
    console.log(`- Total Payment: £${EXPECTED_ELEXON_TOTALS.payment.toFixed(2)}`);
    
    // Compare data
    console.log('\nComparing Data:');
    const volumeDiff = Math.abs(EXPECTED_ELEXON_TOTALS.volume - Number(dbStats.curtailment.totalVolume));
    const paymentDiff = Math.abs(EXPECTED_ELEXON_TOTALS.payment - Number(dbStats.curtailment.totalPayment));
    
    console.log(`- Volume Difference: ${volumeDiff.toFixed(2)} MWh (${(volumeDiff / EXPECTED_ELEXON_TOTALS.volume * 100).toFixed(2)}%)`);
    console.log(`- Payment Difference: £${paymentDiff.toFixed(2)} (${(paymentDiff / EXPECTED_ELEXON_TOTALS.payment * 100).toFixed(2)}%)`);
    
    if (volumeDiff > 1 || paymentDiff > 10) {
      console.log('\n⚠️ Significant differences detected between database and expected Elexon totals!');
      
      const proceed = process.argv.includes('--update');
      
      if (proceed) {
        console.log('\n=== Updating data from Elexon API ===');
        await processDailyCurtailment(TARGET_DATE);
        console.log('\n✅ Data update completed!');
        
        // Verify the update
        const updatedStats = await getDatabaseStats(TARGET_DATE);
        
        console.log('\nUpdated Curtailment Records Totals:');
        console.log(`- Record Count: ${updatedStats.curtailment.recordCount}`);
        console.log(`- Total Volume: ${Number(updatedStats.curtailment.totalVolume).toFixed(2)} MWh`);
        console.log(`- Total Payment: £${Number(updatedStats.curtailment.totalPayment).toFixed(2)}`);
        
        // Compare updated data
        const updatedVolumeDiff = Math.abs(EXPECTED_ELEXON_TOTALS.volume - Number(updatedStats.curtailment.totalVolume));
        const updatedPaymentDiff = Math.abs(EXPECTED_ELEXON_TOTALS.payment - Number(updatedStats.curtailment.totalPayment));
        
        console.log('\nUpdated Differences:');
        console.log(`- Volume Difference: ${updatedVolumeDiff.toFixed(2)} MWh (${(updatedVolumeDiff / EXPECTED_ELEXON_TOTALS.volume * 100).toFixed(2)}%)`);
        console.log(`- Payment Difference: £${updatedPaymentDiff.toFixed(2)} (${(updatedPaymentDiff / EXPECTED_ELEXON_TOTALS.payment * 100).toFixed(2)}%)`);
      } else {
        console.log('\nTo update data from Elexon API, run with --update flag');
      }
    } else {
      console.log('\n✅ Data is reasonably consistent with expected Elexon totals!');
    }
    
  } catch (error) {
    console.error('Error during verification:', error);
  }
}

main().catch(console.error);
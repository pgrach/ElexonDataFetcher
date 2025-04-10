/**
 * Multi-Date Reingestion Script
 * 
 * This script directly accesses the processDailyCurtailment function 
 * to reprocess curtailment data for multiple dates. Bitcoin calculations
 * are skipped to avoid potential constraint errors.
 * 
 * Run with: npx tsx scripts/reprocess-multiple-dates.ts <date1> <date2> ...
 * Example: npx tsx scripts/reprocess-multiple-dates.ts 2025-04-03 2025-04-04
 */

import { processDailyCurtailment } from '../server/services/curtailment';
import { db } from '../db';
import { curtailmentRecords, dailySummaries } from '../db/schema';
import { eq, sql } from 'drizzle-orm';

async function reprocessDate(date: string) {
  console.log(`\n=== Starting Direct Reingestion for ${date} ===`);
  
  try {
    // Step 1: Reingest curtailment records
    console.log(`\nProcessing curtailment data for ${date}...`);
    await processDailyCurtailment(date);
    console.log(`Successfully reingested curtailment data for ${date}`);
    
    // Verify curtailment data
    const stats = await db
      .select({
        recordCount: sql`COUNT(*)`,
        periodCount: sql`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
        totalVolume: sql`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    console.log(`\nCurtailment Data Verification:`);
    console.log(`Records: ${stats[0]?.recordCount || 0}`);
    console.log(`Settlement Periods: ${stats[0]?.periodCount || 0}`);
    console.log(`Total Volume: ${Number(stats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(stats[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Check the daily summary
    const summary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date));
    
    console.log(`\nDaily Summary:`);
    if (summary.length > 0) {
      console.log(`Date: ${summary[0].summaryDate}`);
      console.log(`Total Curtailed Energy: ${summary[0].totalCurtailedEnergy} MWh`);
      console.log(`Total Payment: £${summary[0].totalPayment}`);
      console.log(`Last Updated: ${summary[0].lastUpdated}`);
    } else {
      console.log(`No daily summary found for ${date}`);
    }
    
    console.log(`\nSkipping Bitcoin calculations to avoid constraint errors.`);
    
    console.log(`\n=== Reingestion Complete for ${date} ===`);
    console.log(`Status: Success`);
    console.log(`Completed at: ${new Date().toISOString()}`);
    
    return true;
  } catch (error) {
    console.error(`\nError during reingestion for ${date}:`, error instanceof Error ? error.message : 'Unknown error');
    return false;
  }
}

async function main() {
  const dates = process.argv.slice(2);
  
  if (dates.length === 0) {
    console.error('Error: No dates provided. Please provide at least one date in YYYY-MM-DD format.');
    console.error('Example: npx tsx scripts/reprocess-multiple-dates.ts 2025-04-03 2025-04-04');
    process.exit(1);
  }
  
  console.log(`Starting reingestion for ${dates.length} date(s): ${dates.join(', ')}`);
  
  const startTime = Date.now();
  const results: {date: string, success: boolean}[] = [];
  
  for (const date of dates) {
    const success = await reprocessDate(date);
    results.push({date, success});
  }
  
  const endTime = Date.now();
  const duration = (endTime - startTime) / 1000;
  
  console.log('\n===== REINGESTION SUMMARY =====');
  console.log(`Processed ${dates.length} date(s) in ${duration.toFixed(2)} seconds`);
  console.log('Results:');
  
  for (const result of results) {
    console.log(`- ${result.date}: ${result.success ? 'SUCCESS' : 'FAILED'}`);
  }
  
  const successCount = results.filter(r => r.success).length;
  console.log(`Success rate: ${successCount}/${dates.length} (${(successCount / dates.length * 100).toFixed(2)}%)`);
  
  process.exit(results.every(r => r.success) ? 0 : 1);
}

main().catch(error => {
  console.error('Fatal error:', error instanceof Error ? error.message : 'Unknown error');
  process.exit(1);
});
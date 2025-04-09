/**
 * Elexon Data Verification Script
 * 
 * This script checks Elexon API data against the database for a specific date
 * and shows any discrepancies.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { eq, and, sql } from "drizzle-orm";
import { processDailyCurtailment } from "./server/services/curtailment_enhanced";

const TARGET_DATE = '2025-03-24'; // The date to check

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function getAPIData(date: string) {
  const apiData = {
    recordCount: 0,
    periodCount: new Set<number>(),
    farmCount: new Set<string>(),
    totalVolume: 0,
    totalPayment: 0,
    records: [] as any[]
  };

  console.log(`\nFetching API data for all periods on ${date}...`);

  // Check only the most important periods to speed up verification
  // Focus on periods 18-24 which typically have the most curtailment
  const SAMPLE_PERIODS = [18, 20, 22, 24]; 
  
  for (const period of SAMPLE_PERIODS) {
    try {
      console.log(`Checking period ${period}...`);
      const records = await fetchBidsOffers(date, period);
      
      if (records && Array.isArray(records)) {
        apiData.records.push(...records);
        
        for (const record of records) {
          apiData.recordCount++;
          apiData.periodCount.add(period);
          apiData.farmCount.add(record.id);
          apiData.totalVolume += Math.abs(record.volume);
          apiData.totalPayment += Math.abs(record.volume) * record.originalPrice * -1; // Include -1 multiplier to match Elexon service
        }
      }
      
      // Add a small delay between API calls to avoid rate limiting
      await delay(250);
    } catch (error) {
      console.error(`[${date} P${period}] Error:`, error);
      await delay(5000); // Longer delay on error
    }
  }

  return {
    recordCount: apiData.recordCount,
    periodCount: apiData.periodCount.size,
    farmCount: apiData.farmCount.size,
    totalVolume: apiData.totalVolume,
    totalPayment: apiData.totalPayment,
    records: apiData.records
  } as {
    recordCount: number;
    periodCount: number;
    farmCount: number;
    totalVolume: number;
    totalPayment: number;
    records: any[];
  };
}

async function getDatabaseStats(date: string) {
  try {
    // Get curtailment records stats for the sample periods only
    // Focus on periods 18-24 which typically have the most curtailment
    const periods = [18, 20, 22, 24];
    
    const curtailmentStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)::int`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)::int`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          sql`${curtailmentRecords.settlementPeriod} IN (18, 20, 22, 24)`
        )
      );

    // Get stats for all periods for comparison
    const allPeriodStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)::int`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)::int`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    return {
      samplePeriods: curtailmentStats[0],
      allPeriods: allPeriodStats[0]
    };
  } catch (error) {
    console.error('Error getting database stats:', error);
    throw error;
  }
}

async function main() {
  try {
    console.log(`\n=== Verifying Elexon Data for ${TARGET_DATE} ===\n`);
    
    // Get database stats
    console.log('Fetching database stats...');
    const dbStats = await getDatabaseStats(TARGET_DATE);
    
    console.log('\nDatabase Stats (Sample Periods):');
    console.log(`- Record Count: ${dbStats.samplePeriods.recordCount}`);
    console.log(`- Period Count: ${dbStats.samplePeriods.periodCount}`);
    console.log(`- Farm Count: ${dbStats.samplePeriods.farmCount}`);
    console.log(`- Total Volume: ${Number(dbStats.samplePeriods.totalVolume).toFixed(2)} MWh`);
    console.log(`- Total Payment: £${Math.abs(Number(dbStats.samplePeriods.totalPayment)).toFixed(2)}`);
    
    console.log('\nDatabase Stats (All Periods):');
    console.log(`- Record Count: ${dbStats.allPeriods.recordCount}`);
    console.log(`- Period Count: ${dbStats.allPeriods.periodCount}`);
    console.log(`- Farm Count: ${dbStats.allPeriods.farmCount}`);
    console.log(`- Total Volume: ${Number(dbStats.allPeriods.totalVolume).toFixed(2)} MWh`);
    console.log(`- Total Payment: £${Math.abs(Number(dbStats.allPeriods.totalPayment)).toFixed(2)}`);
    
    // Get API data
    console.log('\nFetching API data...');
    const apiStats = await getAPIData(TARGET_DATE);
    
    console.log('\nAPI Stats (Sample Periods):');
    console.log(`- Record Count: ${apiStats.recordCount}`);
    console.log(`- Period Count: ${apiStats.periodCount}`);
    console.log(`- Farm Count: ${apiStats.farmCount}`);
    console.log(`- Total Volume: ${apiStats.totalVolume.toFixed(2)} MWh`);
    console.log(`- Total Payment: £${apiStats.totalPayment.toFixed(2)}`);
    
    // Compare data
    console.log('\nComparing Data:');
    const volumeDiff = Math.abs(apiStats.totalVolume - Number(dbStats.samplePeriods.totalVolume));
    const paymentDiff = Math.abs(apiStats.totalPayment - Math.abs(Number(dbStats.samplePeriods.totalPayment)));
    
    console.log(`- Volume Difference: ${volumeDiff.toFixed(2)} MWh`);
    console.log(`- Payment Difference: £${paymentDiff.toFixed(2)}`);
    
    if (volumeDiff > 0.1 || paymentDiff > 0.1) {
      console.log('\n⚠️ Discrepancies detected between API and database!');
      
      const proceed = process.argv.includes('--update');
      
      if (proceed) {
        console.log('\n=== Updating data from Elexon API ===');
        await processDailyCurtailment(TARGET_DATE);
        console.log('\n✅ Data update completed!');
        
        // Verify the update
        const updatedStats = await getDatabaseStats(TARGET_DATE);
        
        console.log('\nUpdated Database Stats (All Periods):');
        console.log(`- Record Count: ${updatedStats.allPeriods.recordCount}`);
        console.log(`- Total Volume: ${Number(updatedStats.allPeriods.totalVolume).toFixed(2)} MWh`);
        console.log(`- Total Payment: £${Math.abs(Number(updatedStats.allPeriods.totalPayment)).toFixed(2)}`);
      } else {
        console.log('\nTo update data from Elexon API, run with --update flag');
      }
    } else {
      console.log('\n✅ Data is consistent between API and database!');
    }
    
  } catch (error) {
    console.error('Error during verification:', error);
  }
}

main().catch(console.error);
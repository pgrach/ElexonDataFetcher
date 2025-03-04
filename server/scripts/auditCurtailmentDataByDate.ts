/**
 * Audit Curtailment Data Against Elexon API
 * 
 * This script compares database curtailment records with what's available
 * in the Elexon API for a specific date to identify any discrepancies.
 * 
 * Usage:
 *   npx tsx server/scripts/auditCurtailmentDataByDate.ts 2025-03-02
 */
import { db } from "@db";
import { curtailmentRecords } from "@db/schema";
import { fetchBidsOffers } from "../services/elexon";
import { eq, sql } from "drizzle-orm";
import * as fs from 'fs';

// Get date from command line arguments
const TARGET_DATE = process.argv[2] || '2025-03-02';

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function getDatabaseStats(date: string) {
  try {
    // Get curtailment records stats by period
    const periodStats = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        recordCount: sql<number>`COUNT(*)::int`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);

    // Get overall stats
    const overallStats = await db
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
      periodStats,
      overallStats: overallStats[0]
    };
  } catch (error) {
    console.error('Error getting database stats:', error);
    throw error;
  }
}

async function getAPIData(date: string, periods: number[] = []) {
  const results: Record<number, any> = {};
  const allPeriods = periods.length > 0 ? periods : Array.from({ length: 48 }, (_, i) => i + 1);
  
  console.log(`Fetching API data for ${date}, periods: ${allPeriods.length > 10 ? `${allPeriods.length} periods` : allPeriods.join(', ')}`);
  
  for (const period of allPeriods) {
    try {
      console.log(`Fetching period ${period}...`);
      const bidsOffers = await fetchBidsOffers(date, period);
      
      // Summarize the data
      const totalVolume = bidsOffers.reduce((sum, offer) => sum + Math.abs(offer.volume), 0);
      const totalPayment = bidsOffers.reduce((sum, offer) => sum + offer.volume * offer.finalPrice, 0);
      
      results[period] = {
        recordCount: bidsOffers.length,
        totalVolume: totalVolume.toFixed(2),
        totalPayment: totalPayment.toFixed(2),
        records: bidsOffers
      };
      
      console.log(`Period ${period}: ${bidsOffers.length} records, ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
      
      // Add a delay to avoid rate limiting
      await delay(500);
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
      results[period] = { error: error instanceof Error ? error.message : 'Unknown error' };
    }
  }
  
  return results;
}

async function compareDatabaseWithAPI() {
  try {
    console.log(`\n=== Auditing Curtailment Data for ${TARGET_DATE} ===\n`);
    
    // Get database stats
    console.log('Fetching database stats...');
    const dbStats = await getDatabaseStats(TARGET_DATE);
    
    console.log('\nDatabase Stats:');
    console.log(`Total Records: ${dbStats.overallStats.recordCount}`);
    console.log(`Total Periods: ${dbStats.overallStats.periodCount}/48`);
    console.log(`Total Volume: ${Number(dbStats.overallStats.totalVolume).toFixed(2)} MWh`);
    console.log(`Total Payment: £${Number(dbStats.overallStats.totalPayment).toFixed(2)}`);
    
    // Get API data for a limited set of periods to avoid excessive API calls
    // Choose 5 sample periods to audit
    const samplePeriods = [1, 12, 24, 36, 48]; // Beginning, middle, end periods
    console.log(`\nFetching API data for sample periods: ${samplePeriods.join(', ')}...`);
    const apiData = await getAPIData(TARGET_DATE, samplePeriods);
    
    // Compare database with API for the sample periods
    console.log('\nComparison Results:');
    
    let discrepanciesFound = false;
    let periodSummary = [];
    
    for (const period of samplePeriods) {
      const dbPeriod = dbStats.periodStats.find(p => p.period === period);
      const apiPeriod = apiData[period];
      
      if (!dbPeriod || !apiPeriod || apiPeriod.error) {
        console.log(`\nPeriod ${period}:`);
        console.log('  Database:', dbPeriod ? 'Found' : 'Not found');
        console.log('  API:', apiPeriod ? (apiPeriod.error ? `Error: ${apiPeriod.error}` : 'Found') : 'Not found');
        discrepanciesFound = true;
        continue;
      }
      
      const dbVolume = Number(dbPeriod.totalVolume);
      const apiVolume = Number(apiPeriod.totalVolume);
      const volumeDiff = dbVolume - apiVolume;
      
      const dbPayment = Number(dbPeriod.totalPayment);
      const apiPayment = Number(apiPeriod.totalPayment);
      const paymentDiff = dbPayment - apiPayment;
      
      const volumeDiscrepancy = Math.abs(volumeDiff) > 1; // More than 1 MWh difference
      const paymentDiscrepancy = Math.abs(paymentDiff) > 100; // More than £100 difference
      const countDiscrepancy = dbPeriod.recordCount !== apiPeriod.recordCount;
      
      const hasDiscrepancy = volumeDiscrepancy || paymentDiscrepancy || countDiscrepancy;
      if (hasDiscrepancy) discrepanciesFound = true;
      
      console.log(`\nPeriod ${period}${hasDiscrepancy ? ' ⚠️' : ' ✓'}:`);
      console.log(`  Records:  DB: ${dbPeriod.recordCount}, API: ${apiPeriod.recordCount}${countDiscrepancy ? ' ❌' : ''}`);
      console.log(`  Volume:   DB: ${dbVolume.toFixed(2)} MWh, API: ${apiVolume.toFixed(2)} MWh${volumeDiscrepancy ? ` ❌ (Diff: ${volumeDiff.toFixed(2)} MWh)` : ''}`);
      console.log(`  Payment:  DB: £${dbPayment.toFixed(2)}, API: £${apiPayment.toFixed(2)}${paymentDiscrepancy ? ` ❌ (Diff: £${paymentDiff.toFixed(2)})` : ''}`);
      
      periodSummary.push({
        period,
        hasDiscrepancy,
        db: {
          recordCount: dbPeriod.recordCount,
          volume: dbVolume.toFixed(2),
          payment: dbPayment.toFixed(2)
        },
        api: {
          recordCount: apiPeriod.recordCount,
          volume: apiVolume.toFixed(2),
          payment: apiPayment.toFixed(2)
        },
        diff: {
          recordCount: dbPeriod.recordCount - apiPeriod.recordCount,
          volume: volumeDiff.toFixed(2),
          payment: paymentDiff.toFixed(2)
        }
      });
    }
    
    // Save the comparison results to a file
    const resultsFilename = `audit_${TARGET_DATE}.json`;
    fs.writeFileSync(resultsFilename, JSON.stringify({
      date: TARGET_DATE,
      dbStats: dbStats.overallStats,
      periodSummary
    }, null, 2));
    
    console.log(`\nResults saved to ${resultsFilename}`);
    
    if (discrepanciesFound) {
      console.log('\n⚠️ Discrepancies found. Recommend running the fixMissingPeriods.ts script to update the data.');
    } else {
      console.log('\n✅ No significant discrepancies found between database and Elexon API for the audited periods.');
    }
    
  } catch (error) {
    console.error('Error comparing database with API:', error);
    process.exit(1);
  }
}

// Run the comparison
compareDatabaseWithAPI();
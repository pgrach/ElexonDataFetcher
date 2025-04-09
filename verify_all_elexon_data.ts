/**
 * Complete Elexon Data Verification Script
 * 
 * This script performs a comprehensive check of all 48 settlement periods
 * between Elexon API data and database records for a specific date.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { eq, and, sql } from "drizzle-orm";
import { processDailyCurtailment } from "./server/services/curtailment_enhanced";
import fs from "fs/promises";

const TARGET_DATE = '2025-03-24'; // The date to check

// Directory for storing comparison results
const RESULTS_DIR = './logs';

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function getDBDataForPeriod(date: string, period: number) {
  try {
    const dbRecords = await db
      .select({
        farmId: curtailmentRecords.farmId,
        volume: curtailmentRecords.volume,
        payment: curtailmentRecords.payment
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, date),
          eq(curtailmentRecords.settlementPeriod, period)
        )
      );
    
    const totalVolume = dbRecords.reduce((sum, r) => sum + Math.abs(parseFloat(r.volume)), 0);
    const totalPayment = dbRecords.reduce((sum, r) => sum + parseFloat(r.payment), 0);

    return {
      records: dbRecords,
      count: dbRecords.length,
      farms: new Set(dbRecords.map(r => r.farmId)),
      totalVolume,
      totalPayment,
    };
  } catch (error) {
    console.error(`Error getting DB data for period ${period}:`, error);
    return { records: [], count: 0, farms: new Set(), totalVolume: 0, totalPayment: 0 };
  }
}

async function getAPIDataForPeriod(date: string, period: number) {
  try {
    console.log(`Checking period ${period}...`);
    const records = await fetchBidsOffers(date, period);
    
    if (!records || !Array.isArray(records)) {
      return { records: [], count: 0, farms: new Set(), totalVolume: 0, totalPayment: 0 };
    }
    
    const totalVolume = records.reduce((sum, r) => sum + Math.abs(r.volume), 0);
    const totalPayment = records.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);
    
    return {
      records,
      count: records.length,
      farms: new Set(records.map(r => r.id)),
      totalVolume,
      totalPayment
    };
  } catch (error) {
    console.error(`Error getting API data for period ${period}:`, error);
    await delay(5000); // Longer delay on error
    return { records: [], count: 0, farms: new Set(), totalVolume: 0, totalPayment: 0 };
  }
}

async function main() {
  try {
    console.log(`\n=== Complete Verification of Elexon Data for ${TARGET_DATE} ===\n`);

    // Create results directory if it doesn't exist
    try {
      await fs.mkdir(RESULTS_DIR, { recursive: true });
    } catch (err) {
      console.error('Could not create logs directory:', err);
    }

    // Log file path
    const logFile = `${RESULTS_DIR}/elexon_verification_${TARGET_DATE}_${Date.now()}.log`;
    
    // Initialize log stream
    const logStream = await fs.open(logFile, 'w');
    
    // Helper to write to console and log file
    const log = async (message: string) => {
      console.log(message);
      await logStream.write(message + '\n');
    };
    
    // Get database totals
    const dbTotals = await db
      .select({
        recordCount: sql<number>`COUNT(*)::int`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)::int`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    await log('\nDatabase Totals:');
    await log(`- Record Count: ${dbTotals[0].recordCount}`);
    await log(`- Period Count: ${dbTotals[0].periodCount}`);
    await log(`- Farm Count: ${dbTotals[0].farmCount}`);
    await log(`- Total Volume: ${parseFloat(dbTotals[0].totalVolume).toFixed(2)} MWh`);
    await log(`- Total Payment: £${Math.abs(parseFloat(dbTotals[0].totalPayment)).toFixed(2)}`);

    // Initialize API totals
    let apiTotalRecords = 0;
    let apiTotalVolume = 0;
    let apiTotalPayment = 0;
    const apiPeriodSet = new Set<number>();
    const apiFarmSet = new Set<string>();
    
    // Track differences
    const discrepancies = [];
    const missingPeriods = [];
    const extraPeriods = [];
    
    // Check each settlement period
    for (let period = 1; period <= 48; period++) {
      await log(`\n--- Checking Period ${period} ---`);
      
      // Get database data
      const dbData = await getDBDataForPeriod(TARGET_DATE, period);
      
      // Get API data with a small delay to avoid rate limiting
      await delay(500);
      const apiData = await getAPIDataForPeriod(TARGET_DATE, period);
      
      // Update API totals
      apiTotalRecords += apiData.count;
      apiTotalVolume += apiData.totalVolume;
      apiTotalPayment += apiData.totalPayment;
      if (apiData.count > 0) apiPeriodSet.add(period);
      apiData.farms.forEach(farm => apiFarmSet.add(farm));
      
      // Check for differences
      const volumeDiff = Math.abs(apiData.totalVolume - dbData.totalVolume);
      const paymentDiff = Math.abs(apiData.totalPayment - dbData.totalPayment);
      const countDiff = apiData.count - dbData.count;
      
      await log(`DB: ${dbData.count} records, ${dbData.totalVolume.toFixed(2)} MWh, £${Math.abs(dbData.totalPayment).toFixed(2)}`);
      await log(`API: ${apiData.count} records, ${apiData.totalVolume.toFixed(2)} MWh, £${Math.abs(apiData.totalPayment).toFixed(2)}`);
      
      // Different cases
      if (apiData.count === 0 && dbData.count > 0) {
        await log(`⚠️ Period ${period} exists in DB but not in API!`);
        extraPeriods.push(period);
      } else if (apiData.count > 0 && dbData.count === 0) {
        await log(`⚠️ Period ${period} exists in API but missing from DB!`);
        missingPeriods.push(period);
        discrepancies.push({
          period,
          volumeDiff: apiData.totalVolume,
          paymentDiff: apiData.totalPayment,
          countDiff
        });
      } else if (volumeDiff > 0.01 || paymentDiff > 0.01) {
        await log(`⚠️ Discrepancy in period ${period}:`);
        await log(`  - Volume diff: ${volumeDiff.toFixed(2)} MWh`);
        await log(`  - Payment diff: £${paymentDiff.toFixed(2)}`);
        await log(`  - Record count diff: ${countDiff}`);
        
        discrepancies.push({
          period,
          volumeDiff,
          paymentDiff,
          countDiff
        });
      } else {
        await log(`✅ Period ${period} matches between DB and API`);
      }
    }
    
    // Report overall totals
    await log('\n=== Summary ===');
    await log('\nDatabase Totals:');
    await log(`- Record Count: ${dbTotals[0].recordCount}`);
    await log(`- Period Count: ${dbTotals[0].periodCount}`);
    await log(`- Farm Count: ${dbTotals[0].farmCount}`);
    await log(`- Total Volume: ${parseFloat(dbTotals[0].totalVolume).toFixed(2)} MWh`);
    await log(`- Total Payment: £${Math.abs(parseFloat(dbTotals[0].totalPayment)).toFixed(2)}`);
    
    await log('\nAPI Totals:');
    await log(`- Record Count: ${apiTotalRecords}`);
    await log(`- Period Count: ${apiPeriodSet.size}`);
    await log(`- Farm Count: ${apiFarmSet.size}`);
    await log(`- Total Volume: ${apiTotalVolume.toFixed(2)} MWh`);
    await log(`- Total Payment: £${Math.abs(apiTotalPayment).toFixed(2)}`);
    
    // Overall differences
    const totalVolumeDiff = Math.abs(apiTotalVolume - parseFloat(dbTotals[0].totalVolume));
    const totalPaymentDiff = Math.abs(apiTotalPayment - Math.abs(parseFloat(dbTotals[0].totalPayment)));
    
    await log('\nOverall Differences:');
    await log(`- Volume Difference: ${totalVolumeDiff.toFixed(2)} MWh`);
    await log(`- Payment Difference: £${totalPaymentDiff.toFixed(2)}`);
    await log(`- Record Count Difference: ${apiTotalRecords - dbTotals[0].recordCount}`);

    // Report discrepancies
    if (discrepancies.length > 0) {
      await log('\n⚠️ Discrepancies detected in the following periods:');
      for (const d of discrepancies) {
        await log(`- Period ${d.period}: ${d.volumeDiff.toFixed(2)} MWh, £${d.paymentDiff.toFixed(2)}, ${d.countDiff} records`);
      }
    }
    
    if (missingPeriods.length > 0) {
      await log(`\n⚠️ Periods in API but missing from DB: ${missingPeriods.join(', ')}`);
    }
    
    if (extraPeriods.length > 0) {
      await log(`\n⚠️ Periods in DB but not in API: ${extraPeriods.join(', ')}`);
    }
    
    if (totalVolumeDiff > 1 || totalPaymentDiff > 10) {
      await log('\n⚠️ Significant overall differences detected! Consider updating from the API.');
      
      const proceed = process.argv.includes('--update');
      
      if (proceed) {
        await log('\n=== Updating data from Elexon API ===');
        await processDailyCurtailment(TARGET_DATE);
        await log('\n✅ Data update completed!');
        
        // Verify the update
        const updatedStats = await db
          .select({
            recordCount: sql<number>`COUNT(*)::int`,
            totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
            totalPayment: sql<string>`SUM(payment::numeric)::text`
          })
          .from(curtailmentRecords)
          .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
        
        await log('\nUpdated Database Stats:');
        await log(`- Record Count: ${updatedStats[0].recordCount}`);
        await log(`- Total Volume: ${parseFloat(updatedStats[0].totalVolume).toFixed(2)} MWh`);
        await log(`- Total Payment: £${Math.abs(parseFloat(updatedStats[0].totalPayment)).toFixed(2)}`);
      } else {
        await log('\nTo update data from Elexon API, run with --update flag');
      }
    } else {
      await log('\n✅ Overall totals are reasonably consistent between DB and API.');
    }
    
    // Close log file
    await logStream.close();
    console.log(`\nComplete verification log saved to: ${logFile}`);
    
  } catch (error) {
    console.error('Error during verification:', error);
  }
}

main().catch(console.error);
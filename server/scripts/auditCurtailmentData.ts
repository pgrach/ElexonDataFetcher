import { format } from 'date-fns';
import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { fetchBidsOffers } from "../services/elexon";
import { processHistoricalCalculations } from "../services/bitcoinService";
import { processDailyCurtailment } from "../services/curtailment";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-02-11';

async function getDatabaseStats(date: string) {
  try {
    // Get curtailment records stats
    const curtailmentStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)::int`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)::int`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)::int`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));

    // Get daily summary
    const summary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date));

    return {
      curtailment: curtailmentStats[0],
      summary: summary[0]
    };
  } catch (error) {
    console.error('Error getting database stats:', error);
    throw error;
  }
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

  console.log('\nFetching API data for all periods...');

  for (let period = 1; period <= 48; period++) {
    try {
      const records = await fetchBidsOffers(date, period);
      const validRecords = records.filter(record => 
        record.volume < 0 && (record.soFlag || record.cadlFlag)
      );

      for (const record of validRecords) {
        apiData.recordCount++;
        apiData.periodCount.add(period);
        apiData.farmCount.add(record.id);
        apiData.totalVolume += Math.abs(record.volume);
        // Make sure payment is negative
        apiData.totalPayment -= Math.abs(record.volume) * record.originalPrice;
        apiData.records.push({
          ...record,
          settlementPeriod: period
        });
      }

      if (validRecords.length > 0) {
        console.log(`Period ${period}: ${validRecords.length} records`);
      }
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
    }
  }

  return {
    recordCount: apiData.recordCount,
    periodCount: apiData.periodCount.size,
    farmCount: apiData.farmCount.size,
    totalVolume: apiData.totalVolume,
    totalPayment: apiData.totalPayment,
    records: apiData.records
  };
}

async function auditCurtailmentData() {
  try {
    console.log(`\n=== Auditing Curtailment Data for ${TARGET_DATE} ===\n`);

    // Get current database stats
    console.log('Fetching current database stats...');
    const dbStats = await getDatabaseStats(TARGET_DATE);
    
    console.log('\nDatabase Current State:');
    console.log('Curtailment Records:', {
      records: dbStats.curtailment.recordCount,
      periods: dbStats.curtailment.periodCount,
      farms: dbStats.curtailment.farmCount,
      volume: Number(dbStats.curtailment.totalVolume).toFixed(2),
      payment: Number(dbStats.curtailment.totalPayment).toFixed(2)
    });

    if (dbStats.summary) {
      console.log('Daily Summary:', {
        energy: Number(dbStats.summary.totalCurtailedEnergy).toFixed(2),
        payment: Number(dbStats.summary.totalPayment).toFixed(2)
      });
    }

    // Get API data
    const apiStats = await getAPIData(TARGET_DATE);
    
    console.log('\nAPI Current State:', {
      records: apiStats.recordCount,
      periods: apiStats.periodCount,
      farms: apiStats.farmCount,
      volume: apiStats.totalVolume.toFixed(2),
      payment: apiStats.totalPayment.toFixed(2)
    });

    // Compare and report differences
    const volumeDiff = Math.abs(apiStats.totalVolume - Number(dbStats.curtailment.totalVolume));
    const paymentDiff = Math.abs(apiStats.totalPayment - Number(dbStats.curtailment.totalPayment));
    
    console.log('\nDiscrepancies Found:');
    console.log('Volume Difference:', volumeDiff.toFixed(2), 'MWh');
    console.log('Payment Difference: £', paymentDiff.toFixed(2));
    console.log('Record Count Difference:', apiStats.recordCount - dbStats.curtailment.recordCount);
    console.log('Period Count Difference:', apiStats.periodCount - dbStats.curtailment.periodCount);

    if (volumeDiff > 0.01 || paymentDiff > 0.01) {
      console.log('\nSignificant differences detected, initiating update process...');
      
      // Update curtailment records
      await processDailyCurtailment(TARGET_DATE);
      console.log('✓ Updated curtailment records');
      
      // Update bitcoin calculations
      await processHistoricalCalculations(TARGET_DATE, TARGET_DATE);
      console.log('✓ Updated historical bitcoin calculations');
      
      // Verify updates
      const updatedStats = await getDatabaseStats(TARGET_DATE);
      
      console.log('\nUpdated Database State:');
      console.log('Curtailment Records:', {
        records: updatedStats.curtailment.recordCount,
        periods: updatedStats.curtailment.periodCount,
        volume: Number(updatedStats.curtailment.totalVolume).toFixed(2),
        payment: Number(updatedStats.curtailment.totalPayment).toFixed(2)
      });
      
      if (updatedStats.summary) {
        console.log('Updated Daily Summary:', {
          energy: Number(updatedStats.summary.totalCurtailedEnergy).toFixed(2),
          payment: Number(updatedStats.summary.totalPayment).toFixed(2)
        });
      }
    } else {
      console.log('\n✓ No significant differences found, database is up to date');
    }

  } catch (error) {
    console.error('Error during curtailment audit:', error);
    process.exit(1);
  }
}

// Run audit
auditCurtailmentData();
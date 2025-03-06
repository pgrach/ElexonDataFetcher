import { format } from 'date-fns';
import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-05';

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
    records: [] as any[],
    missingRecords: [] as any[]
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
        apiData.totalPayment += Math.abs(record.volume) * record.originalPrice;
        apiData.records.push({
          ...record,
          settlementPeriod: period
        });
      }

      if (validRecords.length > 0) {
        const periodVolume = validRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
        const periodPayment = validRecords.reduce((sum, r) => sum + Math.abs(r.volume) * r.originalPrice, 0);
        console.log(`Period ${period}: ${validRecords.length} records (${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
      }
    } catch (error) {
      console.error(`Error fetching period ${period}:`, error);
    }
  }

  // Now check for records in API that aren't in the database
  const dbRecords = await db
    .select({
      farmId: curtailmentRecords.farmId,
      period: curtailmentRecords.settlementPeriod,
      volume: curtailmentRecords.volume
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));

  // Create a map of existing DB records
  const dbRecordMap = new Map();
  for (const record of dbRecords) {
    const key = `${record.farmId}-${record.period}`;
    dbRecordMap.set(key, record);
  }

  // Find API records not in DB
  for (const record of apiData.records) {
    const key = `${record.id}-${record.settlementPeriod}`;
    if (!dbRecordMap.has(key)) {
      apiData.missingRecords.push(record);
    }
  }

  return {
    date,
    recordCount: apiData.recordCount,
    periodCount: apiData.periodCount.size,
    farmCount: apiData.farmCount.size,
    totalVolume: apiData.totalVolume,
    totalPayment: apiData.totalPayment,
    records: apiData.records,
    missingRecords: apiData.missingRecords
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

    // Report missing records
    if (apiStats.missingRecords.length > 0) {
      console.log('\nMissing Records:');
      console.log(`Found ${apiStats.missingRecords.length} records in API that are not in the database`);
      
      // Group by period for better readability
      const missingByPeriod = {};
      for (const record of apiStats.missingRecords) {
        const period = record.settlementPeriod;
        if (!missingByPeriod[period]) {
          missingByPeriod[period] = [];
        }
        missingByPeriod[period].push(record);
      }
      
      // Print summary by period
      for (const period in missingByPeriod) {
        const records = missingByPeriod[period];
        const volume = records.reduce((sum, r) => sum + Math.abs(r.volume), 0);
        const payment = records.reduce((sum, r) => sum + Math.abs(r.volume) * r.originalPrice, 0);
        
        console.log(`Period ${period}: ${records.length} missing records (${volume.toFixed(2)} MWh, £${payment.toFixed(2)})`);
        
        // Print details for first few records in each period
        const samplesToShow = Math.min(3, records.length);
        for (let i = 0; i < samplesToShow; i++) {
          const r = records[i];
          console.log(`  - ${r.id}: ${Math.abs(r.volume).toFixed(2)} MWh, £${(Math.abs(r.volume) * r.originalPrice).toFixed(2)}`);
        }
        if (records.length > samplesToShow) {
          console.log(`  - ... and ${records.length - samplesToShow} more`);
        }
      }
    } else {
      console.log('\n✓ All API records are present in the database');
    }

  } catch (error) {
    console.error('Error during curtailment audit:', error);
    process.exit(1);
  }
}

// Run audit
auditCurtailmentData();
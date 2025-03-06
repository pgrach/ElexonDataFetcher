import { format } from 'date-fns';
import { db } from "./db";
import { curtailmentRecords, dailySummaries } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-05';
// Only check a sample of periods to avoid timeout
const PERIODS_TO_CHECK = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45];

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

    // Get counts by period
    const periodStats = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        recordCount: sql<number>`COUNT(*)::int`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);

    // Get daily summary
    const summary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, date));

    return {
      curtailment: curtailmentStats[0],
      periodStats,
      summary: summary[0]
    };
  } catch (error) {
    console.error('Error getting database stats:', error);
    throw error;
  }
}

async function getAPIData(date: string, periodsToCheck: number[]) {
  const apiData = {
    recordCount: 0,
    periodCount: new Set<number>(),
    farmCount: new Set<string>(),
    totalVolume: 0,
    totalPayment: 0,
    records: [] as any[],
    missingRecords: [] as any[],
    periodStats: {} as Record<number, {
      recordCount: number,
      totalVolume: number,
      totalPayment: number
    }>
  };

  console.log('\nFetching API data for selected periods...');

  for (const period of periodsToCheck) {
    try {
      // Initialize period stats
      apiData.periodStats[period] = {
        recordCount: 0,
        totalVolume: 0,
        totalPayment: 0
      };

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
        
        // Update period stats
        apiData.periodStats[period].recordCount++;
        apiData.periodStats[period].totalVolume += Math.abs(record.volume);
        apiData.periodStats[period].totalPayment += Math.abs(record.volume) * record.originalPrice;
        
        apiData.records.push({
          ...record,
          settlementPeriod: period
        });
      }

      if (validRecords.length > 0) {
        const periodVolume = validRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
        const periodPayment = validRecords.reduce((sum, r) => sum + Math.abs(r.volume) * r.originalPrice, 0);
        console.log(`Period ${period}: ${validRecords.length} records (${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
      } else {
        console.log(`Period ${period}: No records found`);
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
    .where(eq(curtailmentRecords.settlementDate, date))
    .where(sql`settlement_period = ANY(${periodsToCheck})`);

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
    missingRecords: apiData.missingRecords,
    periodStats: apiData.periodStats
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

    // Get API data for sample periods
    const apiStats = await getAPIData(TARGET_DATE, PERIODS_TO_CHECK);

    console.log('\nAPI Sample Data (for selected periods):', {
      checkedPeriods: PERIODS_TO_CHECK.length,
      records: apiStats.recordCount,
      periods: apiStats.periodCount.size,
      farms: apiStats.farmCount.size,
      volume: apiStats.totalVolume.toFixed(2),
      payment: apiStats.totalPayment.toFixed(2)
    });

    // Compare each period
    console.log('\nPeriod-by-Period Comparison:');
    for (const period of PERIODS_TO_CHECK) {
      const apiPeriodStat = apiStats.periodStats[period];
      
      // Find matching DB stat for this period
      const dbPeriodStat = dbStats.periodStats.find(p => p.period === period);
      
      if (apiPeriodStat && dbPeriodStat) {
        const apiRecords = apiPeriodStat.recordCount;
        const dbRecords = dbPeriodStat.recordCount;
        const recordDiff = apiRecords - dbRecords;
        
        const apiVolume = apiPeriodStat.totalVolume;
        const dbVolume = Number(dbPeriodStat.totalVolume);
        const volumeDiff = apiVolume - dbVolume;
        
        console.log(`Period ${period}:`);
        console.log(`  - API: ${apiRecords} records, ${apiVolume.toFixed(2)} MWh`);
        console.log(`  - DB:  ${dbRecords} records, ${dbVolume.toFixed(2)} MWh`);
        
        if (recordDiff !== 0 || Math.abs(volumeDiff) > 0.01) {
          console.log(`  - DIFFERENCE: ${recordDiff} records, ${volumeDiff.toFixed(2)} MWh`);
        } else {
          console.log(`  - ✓ No significant differences`);
        }
      } else if (apiPeriodStat && !dbPeriodStat) {
        console.log(`Period ${period}:`);
        console.log(`  - API: ${apiPeriodStat.recordCount} records, ${apiPeriodStat.totalVolume.toFixed(2)} MWh`);
        console.log(`  - DB:  No records found`);
        console.log(`  - MISSING: All ${apiPeriodStat.recordCount} records missing from DB`);
      } else if (!apiPeriodStat && dbPeriodStat) {
        console.log(`Period ${period}:`);
        console.log(`  - API: No records found`);
        console.log(`  - DB:  ${dbPeriodStat.recordCount} records, ${Number(dbPeriodStat.totalVolume).toFixed(2)} MWh`);
        console.log(`  - EXTRA: All ${dbPeriodStat.recordCount} records in DB not found in API`);
      } else {
        console.log(`Period ${period}: No records in API or DB`);
      }
    }

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
      console.log('\n✓ All sampled API records are present in the database');
    }

  } catch (error) {
    console.error('Error during curtailment audit:', error);
    process.exit(1);
  }
}

// Run audit
auditCurtailmentData();
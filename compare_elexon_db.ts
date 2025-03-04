import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { fetchBidsOffers } from "./server/services/elexon";
import { sql, eq } from "drizzle-orm";

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function getDatabaseStats(date: string) {
  try {
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

    return curtailmentStats[0];
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

  // For quicker testing, just get a few periods
  const periods = [1, 2, 3, 4, 5, 15, 20, 25, 30, 35, 40, 45, 48];
  
  for (const period of periods) {
    try {
      await delay(250); // Rate limiting
      const records = await fetchBidsOffers(date, period);

      if (records && Array.isArray(records)) {
        const validRecords = records.filter(record =>
          record.volume < 0 && (record.soFlag || record.cadlFlag)
        );

        if (validRecords.length > 0) {
          console.log(`[${date} P${period}] Records: ${validRecords.length} (${validRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0).toFixed(2)} MWh, £${validRecords.reduce((sum, r) => sum + Math.abs(r.volume * r.originalPrice), 0).toFixed(2)})`);
        }

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
      }
    } catch (error) {
      console.error(`[${date} P${period}] Error:`, error);
      await delay(500); // Double delay on error
    }
  }

  // Estimate total based on sampled periods
  const sampledPeriodsCount = periods.length;
  const totalPeriodsCount = 48;
  const estimationFactor = totalPeriodsCount / sampledPeriodsCount;
  
  const estimatedTotalVolume = apiData.totalVolume * estimationFactor;
  const estimatedTotalPayment = apiData.totalPayment * estimationFactor;

  return {
    date,
    recordCount: apiData.recordCount,
    periodCount: apiData.periodCount.size,
    farmCount: apiData.farmCount.size,
    sampleVolume: apiData.totalVolume,
    samplePayment: apiData.totalPayment,
    estimatedTotalVolume: estimatedTotalVolume,
    estimatedTotalPayment: estimatedTotalPayment,
    sampledPeriodsCount,
    totalPeriodsCount,
    estimationFactor
  };
}

async function compareData(date: string) {
  try {
    console.log(`\nComparing data for ${date}...`);

    const dbStats = await getDatabaseStats(date);
    const apiStats = await getAPIData(date);

    console.log('\nDatabase stats:', {
      records: dbStats.recordCount,
      periods: dbStats.periodCount,
      farms: dbStats.farmCount,
      volume: Number(dbStats.totalVolume).toFixed(2),
      payment: Number(dbStats.totalPayment).toFixed(2)
    });

    console.log('\nAPI stats (sampled):', {
      sampledPeriods: apiStats.sampledPeriodsCount,
      records: apiStats.recordCount,
      periods: apiStats.periodCount,
      farms: apiStats.farmCount,
      sampleVolume: apiStats.sampleVolume.toFixed(2),
      samplePayment: apiStats.samplePayment.toFixed(2)
    });

    console.log('\nAPI stats (estimated total):', {
      estimationFactor: apiStats.estimationFactor.toFixed(2),
      estimatedVolume: apiStats.estimatedTotalVolume.toFixed(2),
      estimatedPayment: apiStats.estimatedTotalPayment.toFixed(2)
    });

    // Calculate differences
    const volumeDiff = Math.abs(apiStats.estimatedTotalVolume - Number(dbStats.totalVolume));
    const paymentDiff = Math.abs(apiStats.estimatedTotalPayment - Number(dbStats.totalPayment));
    const volumeDiffPercent = (volumeDiff / Number(dbStats.totalVolume)) * 100;
    const paymentDiffPercent = (paymentDiff / Math.abs(Number(dbStats.totalPayment))) * 100;

    console.log('\nDifferences:', {
      volumeDiff: volumeDiff.toFixed(2),
      volumeDiffPercent: volumeDiffPercent.toFixed(2) + '%',
      paymentDiff: paymentDiff.toFixed(2),
      paymentDiffPercent: paymentDiffPercent.toFixed(2) + '%'
    });

    return {
      date,
      dbStats,
      apiStats,
      differences: {
        volume: volumeDiff,
        volumePercent: volumeDiffPercent,
        payment: paymentDiff,
        paymentPercent: paymentDiffPercent
      }
    };
  } catch (error) {
    console.error(`Error comparing data for ${date}:`, error);
    return {
      date,
      error: true
    };
  }
}

// Check for date argument or default to 2025-03-02
const dateToCheck = process.argv[2] || '2025-03-02';

compareData(dateToCheck)
  .then(result => {
    console.log('\nComparison complete');
    process.exit(0);
  })
  .catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
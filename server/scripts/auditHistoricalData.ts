import { format, eachDayOfInterval, parseISO } from 'date-fns';
import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { fetchBidsOffers } from "../services/elexon";
import { sql, eq } from "drizzle-orm";
import pLimit from 'p-limit';

// Allow date range to be passed as arguments
const args = process.argv.slice(2);
const START_DATE = args[0] || '2022-01-01';
const END_DATE = args[1] || '2022-01-31';  // Default to one month
const BATCH_SIZE = 5; // Number of days to process concurrently
const limit = pLimit(BATCH_SIZE);
const API_RATE_LIMIT = 250; // ms between API calls

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

  for (let period = 1; period <= 48; period++) {
    try {
      await delay(API_RATE_LIMIT); // Rate limiting
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
      await delay(API_RATE_LIMIT * 2); // Double delay on error
    }
  }

  return {
    date,
    recordCount: apiData.recordCount,
    periodCount: apiData.periodCount.size,
    farmCount: apiData.farmCount.size,
    totalVolume: apiData.totalVolume,
    totalPayment: apiData.totalPayment,
    records: apiData.records
  };
}

async function auditDate(date: string) {
  try {
    console.log(`\nAuditing ${date}...`);

    const dbStats = await getDatabaseStats(date);
    const apiStats = await getAPIData(date);

    // If there's no API data and no DB data, this is valid
    if (apiStats.recordCount === 0 && 
        (!dbStats.curtailment.recordCount || dbStats.curtailment.recordCount === 0)) {
      console.log(`[${date}] No curtailment events found in API or database - this is valid`);
      return {
        date,
        hasMissingData: false,
        dbStats: {
          volume: 0,
          payment: 0
        },
        apiStats: {
          volume: 0,
          payment: 0
        }
      };
    }

    const volumeDiff = Math.abs(apiStats.totalVolume - Number(dbStats.curtailment.totalVolume || 0));
    const paymentDiff = Math.abs(apiStats.totalPayment - Number(dbStats.curtailment.totalPayment || 0));

    const hasMissingData =
      (!dbStats.curtailment.recordCount && apiStats.recordCount > 0) ||
      (dbStats.curtailment.recordCount === 0 && apiStats.recordCount > 0) ||
      volumeDiff > 0.01 ||
      paymentDiff > 0.01;

    if (hasMissingData) {
      console.log(`[${date}] Discrepancy found:`);
      console.log('Database:', {
        records: dbStats.curtailment.recordCount,
        volume: Number(dbStats.curtailment.totalVolume || 0).toFixed(2),
        payment: Number(dbStats.curtailment.totalPayment || 0).toFixed(2)
      });
      console.log('API:', {
        records: apiStats.recordCount,
        volume: apiStats.totalVolume.toFixed(2),
        payment: apiStats.totalPayment.toFixed(2)
      });
    }

    return {
      date,
      hasMissingData,
      dbStats: {
        volume: Number(dbStats.curtailment.totalVolume || 0),
        payment: Number(dbStats.curtailment.totalPayment || 0)
      },
      apiStats: {
        volume: apiStats.totalVolume,
        payment: apiStats.totalPayment
      }
    };
  } catch (error) {
    console.error(`Error auditing ${date}:`, error);
    return {
      date,
      hasMissingData: true,
      error: true
    };
  }
}

async function auditHistoricalData() {
  try {
    console.log(`\n=== Starting Historical Data Audit (${START_DATE} to ${END_DATE}) ===\n`);

    const dates = eachDayOfInterval({
      start: parseISO(START_DATE),
      end: parseISO(END_DATE)
    }).map(date => format(date, 'yyyy-MM-dd'));

    let missingDates: string[] = [];
    let monthlyStats: { [key: string]: { missing: number, total: number } } = {};

    // Process dates in smaller batches
    for (let i = 0; i < dates.length; i += BATCH_SIZE) {
      const batchDates = dates.slice(i, i + BATCH_SIZE);
      const results = await Promise.all(
        batchDates.map(date => limit(() => auditDate(date)))
      );

      // Analyze batch results
      results.forEach(result => {
        if (result.hasMissingData) {
          missingDates.push(result.date);
          const month = result.date.substring(0, 7);
          monthlyStats[month] = monthlyStats[month] || { missing: 0, total: 0 };
          monthlyStats[month].missing++;
        }
        const month = result.date.substring(0, 7);
        monthlyStats[month] = monthlyStats[month] || { missing: 0, total: 0 };
        monthlyStats[month].total++;
      });

      // Print progress
      const progress = ((i + BATCH_SIZE) / dates.length * 100).toFixed(1);
      console.log(`\nProgress: ${progress}% (${i + BATCH_SIZE}/${dates.length} days)`);

      // Add delay between batches
      if (i + BATCH_SIZE < dates.length) {
        await delay(API_RATE_LIMIT * 2);
      }
    }

    // Print monthly summary
    console.log('\n=== Monthly Summary ===');
    Object.entries(monthlyStats)
      .sort(([a], [b]) => a.localeCompare(b))
      .forEach(([month, stats]) => {
        console.log(`${month}: ${stats.missing}/${stats.total} days with missing/incorrect data`);
      });

    console.log('\n=== Missing/Incorrect Dates ===');
    console.log(missingDates.join(', '));

    return {
      totalDays: dates.length,
      missingDays: missingDates.length,
      missingDates,
      monthlyStats
    };
  } catch (error) {
    console.error('Error during historical audit:', error);
    throw error;
  }
}

// Modified run section
if (import.meta.url === `file://${process.argv[1]}`) {
  if (!START_DATE.match(/^\d{4}-\d{2}-\d{2}$/) || !END_DATE.match(/^\d{4}-\d{2}-\d{2}$/)) {
    console.error('Please provide dates in YYYY-MM-DD format');
    console.error('Example: npm run audit-historical 2022-01-01 2022-01-31');
    process.exit(1);
  }

  console.log(`Auditing data from ${START_DATE} to ${END_DATE}`);

  auditHistoricalData()
    .then(results => {
      console.log('\n=== Audit Complete ===');
      console.log(`Found ${results.missingDays} days with missing/incorrect data out of ${results.totalDays} total days`);
      console.log('\nMissing dates:', results.missingDates.join(', '));
      process.exit(0);
    })
    .catch(error => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

export { auditHistoricalData };
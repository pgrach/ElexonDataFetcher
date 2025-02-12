import { format, eachDayOfInterval, startOfMonth, endOfMonth } from 'date-fns';
import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { fetchBidsOffers } from "../services/elexon";
import { processHistoricalCalculations } from "../services/bitcoinService";
import { processDailyCurtailment } from "../services/curtailment";
import { eq, sql, between } from "drizzle-orm";
import pLimit from 'p-limit';

const CURRENT_YEAR = '2024';
const SAMPLE_PERIODS = [1, 12, 24, 36, 48]; // Sample periods throughout the day
const MAX_CONCURRENT_DAYS = 5;

async function getDatabaseStats(startDate: string, endDate: string) {
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
      .where(
        between(curtailmentRecords.settlementDate, startDate, endDate)
      );

    // Get daily summaries for the period
    const summaries = await db
      .select()
      .from(dailySummaries)
      .where(
        between(dailySummaries.summaryDate, startDate, endDate)
      );

    return {
      curtailment: curtailmentStats[0],
      summaries
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

  console.log(`\nFetching API data for ${date}...`);

  for (const period of SAMPLE_PERIODS) {
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
    totalPayment: apiData.totalPayment
  };
}

async function processMissingData(date: string): Promise<void> {
  try {
    console.log(`Processing missing data for ${date}`);
    await processDailyCurtailment(date);
    await processHistoricalCalculations(date, date);
    console.log(`Completed processing for ${date}`);
  } catch (error) {
    console.error(`Error processing ${date}:`, error);
    throw error;
  }
}

async function auditMonth(yearMonth: string) {
  const startDate = startOfMonth(new Date(yearMonth));
  const endDate = endOfMonth(startDate);
  const monthDays = eachDayOfInterval({ start: startDate, end: endDate });

  console.log(`\n=== Auditing ${format(startDate, 'MMMM yyyy')} ===\n`);

  // Get monthly database stats
  const dbStats = await getDatabaseStats(
    format(startDate, 'yyyy-MM-dd'),
    format(endDate, 'yyyy-MM-dd')
  );

  console.log('Database Current State:', {
    records: dbStats.curtailment.recordCount,
    totalVolume: Number(dbStats.curtailment.totalVolume || 0).toFixed(2),
    totalPayment: Number(dbStats.curtailment.totalPayment || 0).toFixed(2),
    daysWithData: dbStats.summaries.length
  });

  // Check each day in parallel with concurrency limit
  const limit = pLimit(MAX_CONCURRENT_DAYS);
  const daysNeedingProcess = [];

  await Promise.all(monthDays.map(day => 
    limit(async () => {
      const dateStr = format(day, 'yyyy-MM-dd');
      const apiData = await getAPIData(dateStr);
      const dbDay = dbStats.summaries.find(s => s.summaryDate === dateStr);

      if (!dbDay || 
          Math.abs(apiData.totalVolume - Number(dbDay.totalCurtailedEnergy)) > 0.1 ||
          Math.abs(apiData.totalPayment - Number(dbDay.totalPayment)) > 0.1) {
        console.log(`\nDiscrepancy found for ${dateStr}:`, {
          api: {
            volume: apiData.totalVolume.toFixed(2),
            payment: apiData.totalPayment.toFixed(2)
          },
          db: dbDay ? {
            volume: Number(dbDay.totalCurtailedEnergy).toFixed(2),
            payment: Number(dbDay.totalPayment).toFixed(2)
          } : 'No data'
        });
        daysNeedingProcess.push(dateStr);
      }
    })
  ));

  if (daysNeedingProcess.length > 0) {
    console.log(`\nProcessing ${daysNeedingProcess.length} days with missing/incorrect data...`);

    for (const date of daysNeedingProcess) {
      try {
        await processMissingData(date);
      } catch (error) {
        console.error(`Failed to process ${date}:`, error);
      }
    }

    // Verify updates
    const updatedStats = await getDatabaseStats(
      format(startDate, 'yyyy-MM-dd'),
      format(endDate, 'yyyy-MM-dd')
    );

    console.log('\nUpdated Database State:', {
      records: updatedStats.curtailment.recordCount,
      totalVolume: Number(updatedStats.curtailment.totalVolume).toFixed(2),
      totalPayment: Number(updatedStats.curtailment.totalPayment).toFixed(2),
      daysWithData: updatedStats.summaries.length
    });
  } else {
    console.log('\n✓ No discrepancies found for this month');
  }
}

async function auditYear() {
  console.log(`\n=== Starting ${CURRENT_YEAR} Monthly Audit ===\n`);

  for (let month = 1; month <= 12; month++) {
    const yearMonth = `${CURRENT_YEAR}-${month.toString().padStart(2, '0')}`;
    await auditMonth(yearMonth);
  }
}

// Run audit
auditYear().catch(error => {
  console.error('Error during year audit:', error);
  process.exit(1);
});
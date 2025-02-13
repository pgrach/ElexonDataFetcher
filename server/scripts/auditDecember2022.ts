import { format, eachDayOfInterval, parseISO } from 'date-fns';
import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { fetchBidsOffers } from "../services/elexon";
import { processDailyCurtailment } from "../services/curtailment";
import { sql, eq } from "drizzle-orm";
import pLimit from 'p-limit';

const START_DATE = '2022-12-01';
const END_DATE = '2022-12-31';
const BATCH_SIZE = 3; // Process 3 days concurrently to avoid rate limits
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
    totalPayment: 0
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
          
          for (const record of validRecords) {
            apiData.recordCount++;
            apiData.periodCount.add(period);
            apiData.farmCount.add(record.id);
            apiData.totalVolume += Math.abs(record.volume);
            apiData.totalPayment += Math.abs(record.volume) * record.originalPrice;
          }
        }
      }
    } catch (error) {
      console.error(`[${date} P${period}] Error:`, error);
      await delay(API_RATE_LIMIT * 2); // Double delay on error
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

async function auditDate(date: string) {
  try {
    console.log(`\nAuditing ${date}...`);
    
    const dbStats = await getDatabaseStats(date);
    const apiStats = await getAPIData(date);

    // Compare totals with a small tolerance for floating point differences
    const volumeDiff = Math.abs(apiStats.totalVolume - Number(dbStats.curtailment.totalVolume || 0));
    const paymentDiff = Math.abs(apiStats.totalPayment - Number(dbStats.curtailment.totalPayment || 0));
    const hasMissingData = volumeDiff > 0.01 || paymentDiff > 0.01;

    console.log(`[${date}] Database:`, {
      records: dbStats.curtailment.recordCount,
      periods: dbStats.curtailment.periodCount,
      volume: Number(dbStats.curtailment.totalVolume || 0).toFixed(2),
      payment: Number(dbStats.curtailment.totalPayment || 0).toFixed(2)
    });

    console.log(`[${date}] API:`, {
      records: apiStats.recordCount,
      periods: apiStats.periodCount,
      volume: apiStats.totalVolume.toFixed(2),
      payment: apiStats.totalPayment.toFixed(2)
    });

    if (hasMissingData) {
      console.log(`[${date}] ⚠️ Discrepancies found - reprocessing...`);
      await processDailyCurtailment(date);
      
      // Verify the update
      const updatedStats = await getDatabaseStats(date);
      console.log(`[${date}] ✓ Updated:`, {
        volume: Number(updatedStats.curtailment.totalVolume || 0).toFixed(2),
        payment: Number(updatedStats.curtailment.totalPayment || 0).toFixed(2)
      });
    } else {
      console.log(`[${date}] ✓ Data is accurate`);
    }

    return {
      date,
      hasMissingData,
      volumeDiff,
      paymentDiff
    };
  } catch (error) {
    console.error(`Error processing ${date}:`, error);
    return {
      date,
      error: true
    };
  }
}

async function auditDecember2022() {
  try {
    console.log(`\n=== Starting December 2022 Data Audit ===\n`);

    const dates = eachDayOfInterval({
      start: parseISO(START_DATE),
      end: parseISO(END_DATE)
    }).map(date => format(date, 'yyyy-MM-dd'));

    let reprocessedDates: string[] = [];
    let errorDates: string[] = [];
    
    // Process dates in smaller batches
    for (let i = 0; i < dates.length; i += BATCH_SIZE) {
      const batchDates = dates.slice(i, i + BATCH_SIZE);
      const results = await Promise.all(
        batchDates.map(date => limit(() => auditDate(date)))
      );

      // Analyze batch results
      results.forEach(result => {
        if (result.error) {
          errorDates.push(result.date);
        } else if (result.hasMissingData) {
          reprocessedDates.push(result.date);
        }
      });

      // Print progress
      const progress = ((i + BATCH_SIZE) / dates.length * 100).toFixed(1);
      console.log(`\nProgress: ${progress}% (${i + BATCH_SIZE}/${dates.length} days)`);
      
      // Add delay between batches
      if (i + BATCH_SIZE < dates.length) {
        await delay(API_RATE_LIMIT * 2);
      }
    }

    // Print summary
    console.log('\n=== Audit Summary ===');
    console.log(`Total days processed: ${dates.length}`);
    console.log(`Days reprocessed: ${reprocessedDates.length}`);
    console.log(`Days with errors: ${errorDates.length}`);
    
    if (reprocessedDates.length > 0) {
      console.log('\nReprocessed dates:', reprocessedDates.join(', '));
    }
    if (errorDates.length > 0) {
      console.log('\nError dates:', errorDates.join(', '));
    }

    return {
      totalDays: dates.length,
      reprocessedDays: reprocessedDates.length,
      errorDays: errorDates.length,
      reprocessedDates,
      errorDates
    };
  } catch (error) {
    console.error('Error during December 2022 audit:', error);
    throw error;
  }
}

// Run the audit if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  auditDecember2022()
    .then(results => {
      console.log('\n=== Audit Complete ===');
      process.exit(0);
    })
    .catch(error => {
      console.error('Fatal error:', error);
      process.exit(1);
    });
}

export { auditDecember2022 };

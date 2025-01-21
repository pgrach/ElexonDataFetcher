import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries, curtailmentRecords } from "@db/schema";
import { eq, sql } from "drizzle-orm";

const datesToReprocess = [
  '2025-01-06',
  '2025-01-07',
  '2025-01-08'
];

async function reIngestDates() {
  try {
    for (const date of datesToReprocess) {
      console.log(`\nReprocessing ${date}...`);

      // Clear existing data
      await db.delete(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, date));
      await db.delete(dailySummaries).where(eq(dailySummaries.summaryDate, date));

      // Re-ingest with updated logic
      await processDailyCurtailment(date);

      // Verify results
      const totals = await db
        .select({
          totalVolume: sql<string>`SUM(${curtailmentRecords.volume}::numeric)`,
          totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`,
          recordCount: sql`COUNT(*)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date));

      // Get period-by-period breakdown
      const periodBreakdown = await db
        .select({
          period: curtailmentRecords.settlementPeriod,
          recordCount: sql`COUNT(*)`,
          periodVolume: sql`SUM(${curtailmentRecords.volume}::numeric)`,
          periodPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date))
        .groupBy(curtailmentRecords.settlementPeriod)
        .orderBy(curtailmentRecords.settlementPeriod);

      console.log(`\nCompleted ${date}:`);
      console.log(`Total Volume: ${totals[0].totalVolume} MWh`);
      console.log(`Total Payment: £${totals[0].totalPayment}`);
      console.log(`Total Records: ${totals[0].recordCount}`);

      console.log('\nPeriod-by-Period Breakdown:');
      periodBreakdown.forEach(p => {
        console.log(`Period ${p.period}: ${p.recordCount} records, ${p.periodVolume} MWh, £${p.periodPayment}`);
      });

      // Add delay between dates to respect rate limits
      if (datesToReprocess.indexOf(date) < datesToReprocess.length - 1) {
        console.log('\nWaiting 60 seconds before next date...');
        await new Promise(resolve => setTimeout(resolve, 60000));
      }
    }

    console.log('\nRe-ingestion completed successfully');
  } catch (error) {
    console.error('Error during re-ingestion:', error);
    process.exit(1);
  }
}

reIngestDates();
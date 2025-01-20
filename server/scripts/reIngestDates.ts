import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries, curtailmentRecords } from "@db/schema";
import { eq, sql } from "drizzle-orm";

// Process a single date to avoid timeouts
const dateToReprocess = '2025-01-10';

async function reIngestDate() {
  try {
    console.log(`\nReprocessing ${dateToReprocess}...`);

    // Clear existing data
    await db.delete(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, dateToReprocess));
    await db.delete(dailySummaries).where(eq(dailySummaries.summaryDate, dateToReprocess));

    // Re-ingest with updated logic
    await processDailyCurtailment(dateToReprocess);

    // Verify results
    const totals = await db
      .select({
        totalVolume: sql<string>`SUM(${curtailmentRecords.volume}::numeric)`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`,
        recordCount: sql`COUNT(*)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, dateToReprocess));

    // Get period-by-period breakdown
    const periodBreakdown = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        recordCount: sql`COUNT(*)`,
        periodVolume: sql`SUM(${curtailmentRecords.volume}::numeric)`,
        periodPayment: sql`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, dateToReprocess))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);

    console.log(`\nCompleted ${dateToReprocess}:`);
    console.log(`Total Volume: ${totals[0].totalVolume} MWh`);
    console.log(`Total Payment: £${totals[0].totalPayment}`);
    console.log(`Total Records: ${totals[0].recordCount}`);

    console.log('\nPeriod-by-Period Breakdown:');
    periodBreakdown.forEach(p => {
      console.log(`Period ${p.period}: ${p.recordCount} records, ${p.periodVolume} MWh, £${p.periodPayment}`);
    });

  } catch (error) {
    console.error('Error during re-ingestion:', error);
    process.exit(1);
  }
}

reIngestDate();
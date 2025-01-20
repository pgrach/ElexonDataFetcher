import { db } from "@db";
import { dailySummaries, curtailmentRecords } from "@db/schema";
import { eq, sql } from "drizzle-orm";
import { processDailyCurtailment } from "../services/curtailment";

async function verifyJan1stData() {
  try {
    console.log('Starting verification of January 1st, 2025 data...');

    // Clear existing data for Jan 1st
    await db.delete(dailySummaries).where(eq(dailySummaries.summaryDate, '2025-01-01'));
    await db.delete(curtailmentRecords).where(eq(curtailmentRecords.settlementDate, '2025-01-01'));

    console.log('\nProcessing data for 2025-01-01');
    await processDailyCurtailment('2025-01-01');

    // Verify the results
    const recordTotals = await db
      .select({
        recordCount: sql`COUNT(*)`,
        totalVolume: sql`SUM(${curtailmentRecords.volume})`,
        totalPayment: sql`SUM(${curtailmentRecords.payment})`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, '2025-01-01'));

    console.log('\nVerification Results:');
    console.log('Total Records:', recordTotals[0]?.recordCount || 0);
    console.log('Total Volume:', recordTotals[0]?.totalVolume || 0, 'MWh');
    console.log('Total Payment:', recordTotals[0]?.totalPayment || 0, 'GBP');

    // Get daily summary for comparison
    const dailySummary = await db.query.dailySummaries.findFirst({
      where: eq(dailySummaries.summaryDate, '2025-01-01')
    });

    if (dailySummary) {
      console.log('\nDaily Summary:');
      console.log('Total Volume:', dailySummary.totalCurtailedEnergy, 'MWh');
      console.log('Total Payment:', dailySummary.totalPayment, 'GBP');
    }

    // Get period-by-period breakdown
    const periodBreakdown = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        recordCount: sql`COUNT(*)`,
        periodVolume: sql`SUM(${curtailmentRecords.volume})`,
        periodPayment: sql`SUM(${curtailmentRecords.payment})`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, '2025-01-01'))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);

    console.log('\nPeriod-by-Period Breakdown:');
    periodBreakdown.forEach(p => {
      console.log(`Period ${p.period}: ${p.recordCount} records, ${p.periodVolume} MWh, £${p.periodPayment}`);
    });

  } catch (error) {
    console.error('Verification failed:', error);
    process.exit(1);
  }
}

// Run the verification
verifyJan1stData();
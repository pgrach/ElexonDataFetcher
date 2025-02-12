import { db } from "@db";
import { monthlySummaries, dailySummaries, curtailmentRecords } from "@db/schema";
import { format, eachMonthOfInterval, parseISO } from "date-fns";
import { sql, eq } from "drizzle-orm";

const START_DATE = '2023-01';
const END_DATE = '2025-02';

async function reprocessMonthSummary(yearMonth: string): Promise<void> {
  try {
    console.log(`\nProcessing ${yearMonth}...`);

    // Calculate monthly totals from daily_summaries
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${dailySummaries.totalCurtailedEnergy}::numeric))`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(
        sql`DATE_TRUNC('month', ${dailySummaries.summaryDate}::date) = 
            DATE_TRUNC('month', ${yearMonth + '-01'}::date)`
      );

    if (!monthlyTotals[0]?.totalCurtailedEnergy) {
      console.log(`No daily data found for ${yearMonth}`);
      return;
    }

    const totalPayment = Number(monthlyTotals[0].totalPayment);
    // Ensure payment is stored as negative value (matching the curtailment records)
    const adjustedPayment = totalPayment > 0 ? -totalPayment : totalPayment;

    // Insert or update monthly summary
    await db.insert(monthlySummaries).values({
      yearMonth,
      totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
      totalPayment: adjustedPayment.toString(),
      updatedAt: new Date()
    }).onConflictDoUpdate({
      target: [monthlySummaries.yearMonth],
      set: {
        totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
        totalPayment: adjustedPayment.toString(),
        updatedAt: new Date()
      }
    });

    // Verify against curtailment records
    const curtailmentTotals = await db
      .select({
        totalVolume: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(sql`DATE_TRUNC('month', ${curtailmentRecords.settlementDate}::date) = DATE_TRUNC('month', ${yearMonth + '-01'}::date)`);

    console.log(`${yearMonth} processed:`, {
      energy: {
        summary: Number(monthlyTotals[0].totalCurtailedEnergy).toFixed(2),
        curtailment: curtailmentTotals[0]?.totalVolume ? Number(curtailmentTotals[0].totalVolume).toFixed(2) : 'N/A'
      },
      payment: {
        summary: Number(adjustedPayment).toFixed(2),
        curtailment: curtailmentTotals[0]?.totalPayment ? Number(curtailmentTotals[0].totalPayment).toFixed(2) : 'N/A'
      }
    });

  } catch (error) {
    console.error(`Error processing ${yearMonth}:`, error);
    throw error;
  }
}

async function reprocessMonthlySummaries() {
  try {
    console.log(`\n=== Starting Monthly Summaries Reprocessing ===`);
    console.log(`Range: ${START_DATE} to ${END_DATE}\n`);

    // Generate list of months to process
    const months = eachMonthOfInterval({
      start: parseISO(`${START_DATE}-01`),
      end: parseISO(`${END_DATE}-01`)
    }).map(date => format(date, 'yyyy-MM'));

    console.log(`Will process ${months.length} months:`, months.join(', '));

    // Process each month
    for (const month of months) {
      await reprocessMonthSummary(month);
    }

    // Verify final results
    const verificationQuery = await db
      .select({
        yearMonth: monthlySummaries.yearMonth,
        totalCurtailedEnergy: monthlySummaries.totalCurtailedEnergy,
        totalPayment: monthlySummaries.totalPayment
      })
      .from(monthlySummaries)
      .where(
        sql`${monthlySummaries.yearMonth} >= ${START_DATE}
            AND ${monthlySummaries.yearMonth} <= ${END_DATE}`
      )
      .orderBy(monthlySummaries.yearMonth);

    console.log('\nVerification Results:');
    let totalEnergy = 0;
    let totalPayment = 0;

    verificationQuery.forEach(record => {
      console.log(`${record.yearMonth}: ${Number(record.totalCurtailedEnergy).toFixed(2)} MWh, £${Number(record.totalPayment).toFixed(2)}`);
      totalEnergy += Number(record.totalCurtailedEnergy);
      totalPayment += Number(record.totalPayment);
    });

    console.log('\nTotal Results:', {
      months: verificationQuery.length,
      totalEnergy: totalEnergy.toFixed(2),
      totalPayment: totalPayment.toFixed(2),
      avgEnergyPerMonth: (totalEnergy / verificationQuery.length).toFixed(2)
    });

    console.log('\n=== Monthly Summaries Reprocessing Complete ===');

  } catch (error) {
    console.error('Error during reprocessing:', error);
    process.exit(1);
  }
}

// Run the reprocessing
if (import.meta.url === `file://${process.argv[1]}`) {
  reprocessMonthlySummaries()
    .catch(error => {
      console.error('Script failed:', error);
      process.exit(1);
    });
}
import { db } from "@db";
import { monthlySummaries, dailySummaries } from "@db/schema";
import { sql } from "drizzle-orm";
import { format } from "date-fns";

async function generateMonthlySummaries() {
  try {
    console.log('\n=== Starting Monthly Summaries Generation ===');

    // Get all unique months from daily_summaries
    const months = await db
      .select({
        yearMonth: sql<string>`DISTINCT TO_CHAR(${dailySummaries.summaryDate}, 'YYYY-MM')`
      })
      .from(dailySummaries)
      .orderBy(sql`TO_CHAR(${dailySummaries.summaryDate}, 'YYYY-MM')`);

    console.log(`Found ${months.length} unique months to process`);

    for (const { yearMonth } of months) {
      try {
        // Calculate monthly totals from daily_summaries using absolute values for payments
        const monthlyTotals = await db
          .select({
            totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
            totalPayment: sql<string>`SUM(ABS(${dailySummaries.totalPayment}::numeric))`
          })
          .from(dailySummaries)
          .where(sql`TO_CHAR(${dailySummaries.summaryDate}, 'YYYY-MM') = ${yearMonth}`);

        const totals = monthlyTotals[0];

        if (!totals.totalCurtailedEnergy || !totals.totalPayment) {
          console.log(`No data found for ${yearMonth}, skipping...`);
          continue;
        }

        // Insert or update monthly summary with absolute payment values
        await db.insert(monthlySummaries).values({
          yearMonth,
          totalCurtailedEnergy: totals.totalCurtailedEnergy,
          totalPayment: totals.totalPayment,
          updatedAt: new Date()
        }).onConflictDoUpdate({
          target: [monthlySummaries.yearMonth],
          set: {
            totalCurtailedEnergy: totals.totalCurtailedEnergy,
            totalPayment: totals.totalPayment,
            updatedAt: new Date()
          }
        });

        console.log(`✓ Processed ${yearMonth}:`, {
          totalCurtailedEnergy: Number(totals.totalCurtailedEnergy).toFixed(2),
          totalPayment: Number(totals.totalPayment).toFixed(2)
        });

      } catch (error) {
        console.error(`Error processing ${yearMonth}:`, error);
      }
    }

    console.log('\n=== Monthly Summaries Generation Completed ===');

    // Verify the results
    const summaries = await db.query.monthlySummaries.findMany({
      orderBy: (monthlySummaries, { asc }) => [asc(monthlySummaries.yearMonth)]
    });

    console.log('\nGenerated Monthly Summaries:');
    summaries.forEach(summary => {
      // Always display positive payment values
      console.log(`${summary.yearMonth}: ${Number(summary.totalCurtailedEnergy).toFixed(2)} MWh, £${Math.abs(Number(summary.totalPayment)).toFixed(2)}`);
    });

  } catch (error) {
    console.error('Fatal error during monthly summaries generation:', error);
    process.exit(1);
  }
}

// Run the generation
generateMonthlySummaries();

export { generateMonthlySummaries };
import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { eq, sql } from "drizzle-orm";
import { format, addDays, parseISO } from "date-fns";

async function reprocessDateRange(startDate: string, endDate?: string) {
  try {
    console.log(`\n=== Starting Data Re-processing from ${startDate}${endDate ? ` to ${endDate}` : ''} ===`);

    let currentDate = parseISO(startDate);
    const lastDate = endDate ? parseISO(endDate) : currentDate;

    while (currentDate <= lastDate) {
      const dateStr = format(currentDate, 'yyyy-MM-dd');

      try {
        console.log(`\nReprocessing ${dateStr}...`);

        // Clear existing records for the date
        await db.delete(curtailmentRecords)
          .where(eq(curtailmentRecords.settlementDate, dateStr));

        await db.delete(dailySummaries)
          .where(eq(dailySummaries.summaryDate, dateStr));

        // Process the day with fixed payment calculations
        await processDailyCurtailment(dateStr);

        // Verify the reprocessed data
        const [curtailmentTotal, summaryTotal] = await Promise.all([
          db.select({
            totalPayment: sql<string>`ABS(SUM(${curtailmentRecords.payment}::numeric))`
          })
          .from(curtailmentRecords)
          .where(eq(curtailmentRecords.settlementDate, dateStr)),

          db.query.dailySummaries.findFirst({
            where: eq(dailySummaries.summaryDate, dateStr)
          })
        ]);

        console.log(`Verification for ${dateStr}:`);
        console.log(`- Curtailment records total: £${Number(curtailmentTotal[0]?.totalPayment || 0).toFixed(2)}`);
        console.log(`- Daily summary total: £${summaryTotal ? Number(summaryTotal.totalPayment).toFixed(2) : 0}`);

      } catch (error) {
        console.error(`Error processing ${dateStr}:`, error);
      }

      // Move to next day
      currentDate = addDays(currentDate, 1);
    }

    console.log(`\n=== Data Re-processing Complete ===\n`);

  } catch (error) {
    console.error('Fatal error during re-processing:', error);
    process.exit(1);
  }
}

const args = process.argv.slice(2);
const startDate = args[0];
const endDate = args[1];

if (!startDate || !startDate.match(/^\d{4}-\d{2}-\d{2}$/)) {
  console.error('Please provide start date in YYYY-MM-DD format');
  console.error('Example for single day: npm run reprocess-day 2024-12-31');
  console.error('Example for date range: npm run reprocess-day 2024-12-01 2024-12-31');
  process.exit(1);
}

if (endDate && !endDate.match(/^\d{4}-\d{2}-\d{2}$/)) {
  console.error('End date must be in YYYY-MM-DD format');
  process.exit(1);
}

reprocessDateRange(startDate, endDate);
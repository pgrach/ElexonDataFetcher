import { eachDayOfInterval, format, parseISO } from "date-fns";
import { processDailyCurtailment } from "../services/curtailment";
import { db } from "@db";
import { dailySummaries } from "@db/schema";
import { eq } from "drizzle-orm";

async function ingestJan2025ToPresent() {
  try {
    const startDate = parseISO("2025-01-01");
    const endDate = new Date();
    const days = eachDayOfInterval({ start: startDate, end: endDate });

    console.log(`Starting data ingestion from ${format(startDate, 'yyyy-MM-dd')} to ${format(endDate, 'yyyy-MM-dd')}`);

    for (const day of days) {
      const dateStr = format(day, 'yyyy-MM-dd');

      // Check if we already have data for this date
      const existingData = await db.query.dailySummaries.findFirst({
        where: eq(dailySummaries.summaryDate, dateStr)
      });

      if (existingData) {
        console.log(`Data already exists for ${dateStr}, skipping...`);
        continue;
      }

      console.log(`Processing data for ${dateStr}`);

      try {
        await processDailyCurtailment(dateStr);
        console.log(`Successfully processed ${dateStr}`);
      } catch (error) {
        console.error(`Error processing ${dateStr}:`, error);
        // Continue with next day even if one fails
      }

      // Add longer delay between days to respect rate limits
      await new Promise(resolve => setTimeout(resolve, 5000));
    }

    console.log('Data ingestion completed successfully');
  } catch (error) {
    console.error('Fatal error during ingestion:', error);
    process.exit(1);
  }
}

// Run the ingestion
ingestJan2025ToPresent();

export { ingestJan2025ToPresent };
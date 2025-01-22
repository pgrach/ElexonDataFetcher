import { ingestMonthlyData } from "./ingestMonthlyData";
import { db } from "@db";
import { ingestionProgress } from "@db/schema";
import { eq } from "drizzle-orm";

async function delay(ms: number) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function processAllMonths() {
  const months = [
    "01", "02", "03", "04", "05", "06",
    "07", "08", "09", "10", "11", "12"
  ];

  console.log("=== Starting 2023 Data Processing ===\n");

  for (const month of months) {
    try {
      // Check if month has already been processed successfully
      const progress = await db.query.ingestionProgress.findFirst({
        where: eq(ingestionProgress.lastProcessedDate, `2023-${month}-01`)
      });

      if (progress?.status === 'completed') {
        console.log(`\nSkipping 2023-${month} - Already processed successfully`);
        continue;
      }

      console.log(`\nProcessing month: 2023-${month}`);
      console.log("=".repeat(40));

      // Process each month
      await ingestMonthlyData(`2023-${month}`);

      // Record successful processing
      await db.insert(ingestionProgress).values({
        lastProcessedDate: `2023-${month}-01`,
        status: 'completed',
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [ingestionProgress.lastProcessedDate],
        set: {
          status: 'completed',
          updatedAt: new Date()
        }
      });

      // Add a significant delay between months to avoid rate limiting
      console.log(`\nCompleted 2023-${month}. Waiting before next month...`);
      await delay(60000); // 60 second delay between months

    } catch (error) {
      console.error(`Error processing month 2023-${month}:`, error);

      // Record failed processing
      await db.insert(ingestionProgress).values({
        lastProcessedDate: `2023-${month}-01`,
        status: 'failed',
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
        updatedAt: new Date()
      }).onConflictDoUpdate({
        target: [ingestionProgress.lastProcessedDate],
        set: {
          status: 'failed',
          errorMessage: error instanceof Error ? error.message : 'Unknown error',
          updatedAt: new Date()
        }
      });

      // Longer cooldown on error before trying next month
      console.log(`\nWaiting 2 minutes before proceeding to next month...`);
      await delay(120000); // 2 minute cooldown on error
    }
  }

  console.log("\n=== 2023 Data Processing Complete ===");
}

// Start processing with error handling
processAllMonths().catch(error => {
  console.error('Fatal error in processAllMonths:', error);
  process.exit(1);
});
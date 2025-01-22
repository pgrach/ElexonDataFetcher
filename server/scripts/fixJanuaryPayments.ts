import { db } from "@db";
import { curtailmentRecords, dailySummaries, monthlySummaries } from "@db/schema";
import { processDailyCurtailment } from "../services/curtailment";
import { sql, eq } from "drizzle-orm";

async function fixJanuaryPayments() {
  const JANUARY_2025 = '2025-01';
  
  try {
    console.log('\n=== Starting January 2025 Payment Fix ===');

    // Clear existing January records
    await db.delete(curtailmentRecords)
      .where(sql`date_trunc('month', settlement_date::date) = date_trunc('month', ${'2025-01-01'}::date)`);
    
    await db.delete(dailySummaries)
      .where(sql`date_trunc('month', summary_date::date) = date_trunc('month', ${'2025-01-01'}::date)`);
    
    await db.delete(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, JANUARY_2025));

    // Process all days in January
    for (let day = 1; day <= 31; day++) {
      const date = `2025-01-${day.toString().padStart(2, '0')}`;
      try {
        await processDailyCurtailment(date);
        console.log(`✓ Processed ${date}`);
      } catch (error) {
        console.error(`Error processing ${date}:`, error);
      }
      
      // Add a small delay between days to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    // Verify the fix
    const fixedRecords = await db.select({
      summaryDate: dailySummaries.summaryDate,
      totalCurtailedEnergy: dailySummaries.totalCurtailedEnergy,
      totalPayment: dailySummaries.totalPayment
    })
    .from(dailySummaries)
    .where(sql`date_trunc('month', summary_date::date) = date_trunc('month', ${'2025-01-01'}::date)`)
    .orderBy(dailySummaries.summaryDate);

    console.log('\n=== January 2025 Daily Summaries After Fix ===');
    fixedRecords.forEach(record => {
      console.log(`${record.summaryDate}: ${Number(record.totalCurtailedEnergy).toFixed(2)} MWh, £${Number(record.totalPayment).toFixed(2)}`);
    });

  } catch (error) {
    console.error('Fatal error during payment fix:', error);
    process.exit(1);
  }
}

// Run the fix
fixJanuaryPayments();

/**
 * Update Daily Summary for 2025-03-31
 * 
 * This script updates the daily summary for 2025-03-31 based on 
 * the corrected curtailment records.
 */

import { db } from './db';
import { dailySummaries, curtailmentRecords } from './db/schema';
import { count, eq, sql, sum } from 'drizzle-orm';

const DATE_TO_UPDATE = '2025-03-31';

/**
 * Update the daily summary for a specific date
 */
async function updateDailySummary(date: string): Promise<void> {
  try {
    console.log(`Updating daily summary for ${date}...`);
    
    // Get summary data from curtailment records
    const result = await db.select({
      totalRecords: count(),
      uniquePeriods: sql<number>`COUNT(DISTINCT settlement_period)`,
      totalVolume: sql<number>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<number>`SUM(payment::numeric)`
    }).from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .limit(1);
    
    if (!result || result.length === 0) {
      console.error(`No curtailment records found for ${date}`);
      return;
    }
    
    const { totalRecords, uniquePeriods, totalVolume, totalPayment } = result[0];
    
    console.log(`Summary data: ${totalRecords} records, ${uniquePeriods} periods, ${totalVolume} MWh, £${totalPayment}`);
    
    // Check if summary already exists
    const existingSummary = await db.select().from(dailySummaries)
      .where(eq(dailySummaries.date, date));
    
    if (existingSummary && existingSummary.length > 0) {
      // Update existing summary
      console.log(`Updating existing summary for ${date}`);
      
      await db.update(dailySummaries)
        .set({
          records: totalRecords.toString(),
          periods: uniquePeriods.toString(),
          volume: totalVolume ? totalVolume.toString() : '0',
          payment: totalPayment ? totalPayment.toString() : '0',
          updatedAt: new Date().toISOString()
        })
        .where(eq(dailySummaries.date, date));
    } else {
      // Insert new summary
      console.log(`Creating new summary for ${date}`);
      
      await db.insert(dailySummaries).values({
        date,
        records: totalRecords.toString(),
        periods: uniquePeriods.toString(),
        volume: totalVolume ? totalVolume.toString() : '0',
        payment: totalPayment ? totalPayment.toString() : '0',
        updatedAt: new Date().toISOString()
      });
    }
    
    console.log(`Summary for ${date} updated successfully`);
  } catch (error) {
    console.error(`Error updating daily summary for ${date}:`, error);
    throw error;
  }
}

/**
 * Main function
 */
async function main() {
  try {
    console.log(`\n=== Updating Daily Summary for ${DATE_TO_UPDATE} ===\n`);
    
    await updateDailySummary(DATE_TO_UPDATE);
    
    console.log(`\n=== Update Complete ===\n`);
  } catch (error) {
    console.error('Error in main process:', error);
  }
}

// Run the main function
main();
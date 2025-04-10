/**
 * Update Daily Summary Script for 2025-04-03
 * 
 * This script updates the daily summary for 2025-04-03
 * based on the curtailment records that have been inserted.
 */

import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { eq, sql } from 'drizzle-orm';

// Configuration
const TARGET_DATE = '2025-04-03';

/**
 * Simple logging utility with timestamps
 */
function log(message: string): void {
  const timestamp = new Date().toISOString().substring(11, 19);
  console.log(`[${timestamp}] ${message}`);
}

/**
 * Update daily summary from curtailment records
 */
async function updateDailySummary(): Promise<void> {
  log(`Updating daily summary for ${TARGET_DATE}...`);
  
  try {
    // Get records count
    const recordCount = await db
      .select({ count: sql<number>`COUNT(*)` })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    log(`Found ${recordCount[0].count} curtailment records for ${TARGET_DATE}`);
    
    if (recordCount[0].count === 0) {
      log(`No curtailment records found for ${TARGET_DATE}, skipping summary update`);
      return;
    }
    
    // Calculate totals from curtailment records
    const totals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}::numeric))`,
        totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const totalCurtailedEnergy = parseFloat(totals[0]?.totalCurtailedEnergy || '0');
    const totalPayment = parseFloat(totals[0]?.totalPayment || '0');
    
    log(`Calculated totals: ${totalCurtailedEnergy.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
    
    // Update existing summary
    await db.update(dailySummaries)
      .set({
        totalCurtailedEnergy: totalCurtailedEnergy.toString(),
        totalPayment: totalPayment.toString(),
        lastUpdated: new Date()
      })
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    log(`Updated daily summary for ${TARGET_DATE}`);
    
    // Get updated summary
    const updatedSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    if (updatedSummary.length > 0) {
      log(`Updated summary: ${parseFloat(updatedSummary[0].totalCurtailedEnergy?.toString() || '0').toFixed(2)} MWh, £${parseFloat(updatedSummary[0].totalPayment?.toString() || '0').toFixed(2)}`);
    }
  } catch (error) {
    log(`Error updating daily summary: ${(error as Error).message}`);
    throw error;
  }
}

// Execute the update
updateDailySummary()
  .then(() => {
    console.log('\nDaily summary update completed successfully.');
    process.exit(0);
  })
  .catch((error) => {
    console.error('\nDaily summary update failed with error:', error);
    process.exit(1);
  });
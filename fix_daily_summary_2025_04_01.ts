/**
 * Fix Daily Summary for 2025-04-01
 * 
 * This script corrects the energy and payment values in the daily_summaries table
 * to match the totals in the curtailment_records table for 2025-04-01.
 */

import { db } from "./db";
import { dailySummaries, curtailmentRecords } from "./db/schema";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-04-01';

async function fixDailySummary(): Promise<void> {
  try {
    console.log(`\n=== Fixing Daily Summary for ${TARGET_DATE} ===\n`);
    
    // Get current daily summary values
    const currentSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    if (currentSummary.length > 0) {
      console.log('Current daily summary values:');
      console.log(`- Energy: ${currentSummary[0].totalCurtailedEnergy} MWh`);
      console.log(`- Payment: £${currentSummary[0].totalPayment}`);
      
      // Save existing wind generation data to preserve it
      const totalWindGeneration = currentSummary[0].totalWindGeneration;
      const windOnshoreGeneration = currentSummary[0].windOnshoreGeneration;
      const windOffshoreGeneration = currentSummary[0].windOffshoreGeneration;
      
      // Calculate correct totals from curtailment_records
      const dbTotals = await db
        .select({
          totalCurtailedEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume})::numeric)`,
          totalPayment: sql<string>`SUM(${curtailmentRecords.payment}::numeric)`
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
      if (dbTotals[0] && dbTotals[0].totalCurtailedEnergy) {
        const correctedEnergy = parseFloat(dbTotals[0].totalCurtailedEnergy);
        const correctedPayment = parseFloat(dbTotals[0].totalPayment);
        
        console.log('\nCalculated totals from curtailment_records:');
        console.log(`- Energy: ${correctedEnergy.toFixed(2)} MWh`);
        console.log(`- Payment: £${correctedPayment.toFixed(2)}`);
        
        // Update daily summary with correct values while preserving wind data
        await db
          .update(dailySummaries)
          .set({
            totalCurtailedEnergy: correctedEnergy.toString(),
            totalPayment: correctedPayment.toString(),
            totalWindGeneration: totalWindGeneration,
            windOnshoreGeneration: windOnshoreGeneration,
            windOffshoreGeneration: windOffshoreGeneration,
            lastUpdated: new Date()
          })
          .where(eq(dailySummaries.summaryDate, TARGET_DATE));
        
        // Verify the update
        const updatedSummary = await db
          .select()
          .from(dailySummaries)
          .where(eq(dailySummaries.summaryDate, TARGET_DATE));
        
        console.log('\nUpdated daily summary values:');
        console.log(`- Energy: ${updatedSummary[0].totalCurtailedEnergy} MWh`);
        console.log(`- Payment: £${updatedSummary[0].totalPayment}`);
        console.log(`- Total Wind Generation: ${updatedSummary[0].totalWindGeneration} MW`);
        console.log(`- Onshore Wind: ${updatedSummary[0].windOnshoreGeneration} MW`);
        console.log(`- Offshore Wind: ${updatedSummary[0].windOffshoreGeneration} MW`);
        console.log(`- Last Updated: ${updatedSummary[0].lastUpdated}`);
        
        console.log('\n✓ Daily summary fixed successfully!');
      } else {
        console.error('No curtailment records found for this date');
      }
    } else {
      console.error('No daily summary found for this date');
    }
  } catch (error) {
    console.error('Error fixing daily summary:', error);
    throw error;
  }
}

async function main(): Promise<void> {
  try {
    await fixDailySummary();
    process.exit(0);
  } catch (error) {
    console.error('Script failed:', error);
    process.exit(1);
  }
}

main();
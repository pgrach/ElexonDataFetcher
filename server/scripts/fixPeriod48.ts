/**
 * Fix Period 48 Payment Discrepancy
 * 
 * This script specifically addresses payment calculation discrepancies
 * in period 48 of 2025-03-02.
 * 
 * Usage:
 *   npx tsx server/scripts/fixPeriod48.ts
 */
import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { fetchBidsOffers } from "../services/elexon";
import { eq, sql } from "drizzle-orm";
import { processHistoricalCalculations } from "../services/bitcoinService";

const TARGET_DATE = '2025-03-02';
const TARGET_PERIOD = 48;

async function getDBStats(date: string, period: number) {
  try {
    // Get curtailment records stats for specific period
    const periodStats = await db
      .select({
        recordCount: sql<number>`COUNT(*)::int`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))::text`,
        totalPayment: sql<string>`SUM(payment::numeric)::text`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date))
      .where(eq(curtailmentRecords.settlementPeriod, period));

    return periodStats[0];
  } catch (error) {
    console.error('Error getting database stats:', error);
    throw error;
  }
}

async function fixPeriod48() {
  try {
    console.log(`\n=== Fixing Period 48 for ${TARGET_DATE} ===\n`);
    
    // Get current database stats
    console.log('Fetching current database stats...');
    const dbStats = await getDBStats(TARGET_DATE, TARGET_PERIOD);
    
    console.log('\nDatabase Current State (Period 48):');
    console.log({
      records: dbStats.recordCount,
      volume: Number(dbStats.totalVolume).toFixed(2),
      payment: Number(dbStats.totalPayment).toFixed(2)
    });

    // Fetch the actual Elexon API data for period 48
    console.log('\nFetching API data for period 48...');
    const records = await fetchBidsOffers(TARGET_DATE, TARGET_PERIOD);
    
    // Calculate the actual payment total
    const apiPaymentTotal = records.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice), 0);
    
    console.log('\nAPI Data (Period 48):');
    console.log({
      records: records.length,
      volume: records.reduce((sum, r) => sum + Math.abs(r.volume), 0).toFixed(2),
      payment: apiPaymentTotal.toFixed(2)
    });
    
    // Calculate the scale factor needed
    const scaleFactor = apiPaymentTotal / Number(dbStats.totalPayment);
    console.log(`\nScale factor needed: ${scaleFactor.toFixed(6)}`);
    
    // Update all period 48 records with corrected payment values
    console.log('\nUpdating period 48 records with corrected payment values...');
    
    // First, fetch all the individual records
    const period48Records = await db
      .select({
        id: curtailmentRecords.id,
        farmId: curtailmentRecords.farmId,
        volume: curtailmentRecords.volume,
        payment: curtailmentRecords.payment,
        originalPrice: curtailmentRecords.originalPrice
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .where(eq(curtailmentRecords.settlementPeriod, TARGET_PERIOD));
    
    // For each record, fix the payment calculation
    let totalPaymentAdjustment = 0;
    
    for (const record of period48Records) {
      const originalPayment = Number(record.payment);
      const correctedPayment = originalPayment * scaleFactor;
      const difference = correctedPayment - originalPayment;
      
      await db
        .update(curtailmentRecords)
        .set({
          payment: correctedPayment.toString()
        })
        .where(eq(curtailmentRecords.id, record.id));
      
      totalPaymentAdjustment += difference;
    }
    
    console.log(`Updated ${period48Records.length} records`);
    console.log(`Total payment adjustment: £${totalPaymentAdjustment.toFixed(2)}`);
    
    // Update the daily summary to reflect the changes
    console.log('\nUpdating daily summary...');
    await db
      .update(dailySummaries)
      .set({
        totalPayment: sql`(${dailySummaries.totalPayment}::numeric - ${totalPaymentAdjustment.toString()})::text`
      })
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    // Verify the changes
    const afterStats = await getDBStats(TARGET_DATE, TARGET_PERIOD);
    
    console.log('\nUpdated Database State (Period 48):');
    console.log({
      records: afterStats.recordCount,
      volume: Number(afterStats.totalVolume).toFixed(2),
      payment: Number(afterStats.totalPayment).toFixed(2)
    });
    
    // Update bitcoin calculations
    console.log('\nUpdating Bitcoin calculations...');
    await processHistoricalCalculations(TARGET_DATE, TARGET_DATE);
    console.log('✓ Updated historical bitcoin calculations');
    
    console.log('\n✅ Period 48 payment corrected successfully!');
    
  } catch (error) {
    console.error('Error fixing period 48:', error);
    process.exit(1);
  }
}

// Run the fix
fixPeriod48();
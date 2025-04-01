/**
 * Comprehensive Fix for 2025-03-28
 * 
 * This script:
 * 1. Identifies and removes duplicate records in the curtailment_records table
 * 2. Recalculates the total payment and curtailed energy
 * 3. Updates the daily_summaries table with corrected values
 */

import { db } from './db';
import { sql, eq } from 'drizzle-orm';
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from './db/schema';
import * as fs from 'fs';

const DATE_TO_FIX = '2025-03-28';
const LOG_FILE = `comprehensive_fix_${DATE_TO_FIX}.log`;

// Helper function to log to file
async function logToFile(message: string): Promise<void> {
  const timestamp = new Date().toISOString().replace('T', ' ').split('.')[0];
  const logMessage = `[${timestamp}] ${message}\n`;
  fs.appendFileSync(LOG_FILE, logMessage);
  console.log(message);
}

async function comprehensiveFix() {
  try {
    await logToFile(`Starting comprehensive fix for ${DATE_TO_FIX}...`);
    
    // Step 1: Note the current state
    const currentStateQuery = await db.execute(sql`
      SELECT
        (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = ${DATE_TO_FIX}) AS total_records,
        (SELECT SUM(ABS(CAST(volume AS numeric))) FROM curtailment_records WHERE settlement_date = ${DATE_TO_FIX}) AS total_energy,
        (SELECT SUM(CAST(payment AS numeric)) FROM curtailment_records WHERE settlement_date = ${DATE_TO_FIX}) AS total_payment,
        (SELECT total_curtailed_energy FROM daily_summaries WHERE summary_date = ${DATE_TO_FIX}) AS summary_energy,
        (SELECT total_payment FROM daily_summaries WHERE summary_date = ${DATE_TO_FIX}) AS summary_payment
    `);
    
    const currentState = currentStateQuery.rows[0];
    await logToFile(`Current state for ${DATE_TO_FIX}:`);
    await logToFile(`  Total records: ${currentState.total_records}`);
    await logToFile(`  Raw energy total: ${currentState.total_energy} MWh`);
    await logToFile(`  Raw payment total: £${currentState.total_payment}`);
    await logToFile(`  Daily summary energy: ${currentState.summary_energy} MWh`);
    await logToFile(`  Daily summary payment: £${currentState.summary_payment}`);
    
    // Step 2: Identify duplicate records
    const duplicatesQuery = await db.execute(sql`
      WITH records_with_rownum AS (
        SELECT 
          id,
          settlement_date,
          settlement_period,
          farm_id,
          volume,
          payment,
          created_at,
          ROW_NUMBER() OVER (PARTITION BY settlement_period, farm_id ORDER BY created_at DESC) as rn
        FROM curtailment_records
        WHERE settlement_date = ${DATE_TO_FIX}
      )
      SELECT COUNT(*) as duplicate_count
      FROM records_with_rownum
      WHERE rn > 1
    `);
    
    const duplicateCount = parseInt(duplicatesQuery.rows[0].duplicate_count);
    await logToFile(`\nFound ${duplicateCount} duplicate records to remove`);
    
    // Step 3: Remove duplicates - keep the latest record for each period/farm
    if (duplicateCount > 0) {
      await logToFile(`\nRemoving ${duplicateCount} duplicate records...`);
      
      const duplicateIds = await db.execute(sql`
        WITH records_with_rownum AS (
          SELECT 
            id,
            settlement_period,
            farm_id,
            created_at,
            ROW_NUMBER() OVER (PARTITION BY settlement_period, farm_id ORDER BY created_at DESC) as rn
          FROM curtailment_records
          WHERE settlement_date = ${DATE_TO_FIX}
        )
        SELECT id
        FROM records_with_rownum
        WHERE rn > 1
      `);
      
      // Delete duplicate records using a CTE approach
      await db.execute(sql`
        WITH records_with_rownum AS (
          SELECT 
            id,
            settlement_period,
            farm_id,
            created_at,
            ROW_NUMBER() OVER (PARTITION BY settlement_period, farm_id ORDER BY created_at DESC) as rn
          FROM curtailment_records
          WHERE settlement_date = ${DATE_TO_FIX}
        )
        DELETE FROM curtailment_records
        WHERE id IN (
          SELECT id FROM records_with_rownum WHERE rn > 1
        )
      `);
      
      await logToFile(`  Deleted ${duplicateCount} duplicate records`);
      
      
      await logToFile(`Completed deletion of ${duplicateCount} duplicate records`);
    }
    
    // Step 4: Recalculate totals after deduplication
    const dedupTotalsQuery = await db.execute(sql`
      SELECT
        COUNT(*) AS total_records,
        SUM(ABS(CAST(volume AS numeric))) AS total_energy,
        SUM(CAST(payment AS numeric)) AS total_payment
      FROM curtailment_records
      WHERE settlement_date = ${DATE_TO_FIX}
    `);
    
    const dedupTotals = dedupTotalsQuery.rows[0];
    await logToFile(`\nAfter deduplication:`);
    await logToFile(`  Total records: ${dedupTotals.total_records}`);
    await logToFile(`  Energy total: ${dedupTotals.total_energy} MWh`);
    await logToFile(`  Payment total: £${dedupTotals.total_payment}`);
    
    // Step 5: Update daily summary with deduplicated totals
    await db.insert(dailySummaries)
      .values({
        summaryDate: DATE_TO_FIX,
        totalCurtailedEnergy: String(dedupTotals.total_energy),
        totalPayment: String(dedupTotals.total_payment),
        lastUpdated: new Date()
      })
      .onConflictDoUpdate({
        target: [dailySummaries.summaryDate],
        set: {
          totalCurtailedEnergy: String(dedupTotals.total_energy),
          totalPayment: String(dedupTotals.total_payment),
          lastUpdated: new Date()
        }
      });
    
    await logToFile(`\nUpdated daily summary with deduplicated totals`);
    
    // Step 6: Update monthly and yearly summaries
    // Monthly summary
    const yearMonth = DATE_TO_FIX.substring(0, 7); // '2025-03'
    
    // Recalculate monthly totals
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${DATE_TO_FIX}::date)`);
    
    if (monthlyTotals[0].totalCurtailedEnergy && monthlyTotals[0].totalPayment) {
      // Update monthly summary
      await db.insert(monthlySummaries)
        .values({
          yearMonth,
          totalCurtailedEnergy: String(monthlyTotals[0].totalCurtailedEnergy),
          totalPayment: String(monthlyTotals[0].totalPayment),
          updatedAt: new Date()
        })
        .onConflictDoUpdate({
          target: [monthlySummaries.yearMonth],
          set: {
            totalCurtailedEnergy: String(monthlyTotals[0].totalCurtailedEnergy),
            totalPayment: String(monthlyTotals[0].totalPayment),
            updatedAt: new Date()
          }
        });
      
      await logToFile(`\nUpdated monthly summary for ${yearMonth}:`);
      await logToFile(`  Energy: ${parseFloat(monthlyTotals[0].totalCurtailedEnergy).toFixed(2)} MWh`);
      await logToFile(`  Payment: £${parseFloat(monthlyTotals[0].totalPayment).toFixed(2)}`);
    }
    
    // Yearly summary
    const year = DATE_TO_FIX.substring(0, 4); // '2025'
    
    // Recalculate yearly totals
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${DATE_TO_FIX}::date)`);
    
    if (yearlyTotals[0].totalCurtailedEnergy && yearlyTotals[0].totalPayment) {
      // Update yearly summary
      await db.insert(yearlySummaries)
        .values({
          year,
          totalCurtailedEnergy: String(yearlyTotals[0].totalCurtailedEnergy),
          totalPayment: String(yearlyTotals[0].totalPayment),
          updatedAt: new Date()
        })
        .onConflictDoUpdate({
          target: [yearlySummaries.year],
          set: {
            totalCurtailedEnergy: String(yearlyTotals[0].totalCurtailedEnergy),
            totalPayment: String(yearlyTotals[0].totalPayment),
            updatedAt: new Date()
          }
        });
      
      await logToFile(`\nUpdated yearly summary for ${year}:`);
      await logToFile(`  Energy: ${parseFloat(yearlyTotals[0].totalCurtailedEnergy).toFixed(2)} MWh`);
      await logToFile(`  Payment: £${parseFloat(yearlyTotals[0].totalPayment).toFixed(2)}`);
    }
    
    // Step 7: Summarize the fixes
    const finalStateQuery = await db.execute(sql`
      SELECT
        (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = ${DATE_TO_FIX}) AS total_records,
        (SELECT total_curtailed_energy FROM daily_summaries WHERE summary_date = ${DATE_TO_FIX}) AS summary_energy,
        (SELECT total_payment FROM daily_summaries WHERE summary_date = ${DATE_TO_FIX}) AS summary_payment
    `);
    
    const finalState = finalStateQuery.rows[0];
    await logToFile(`\nFinal state for ${DATE_TO_FIX}:`);
    await logToFile(`  Total records: ${finalState.total_records}`);
    await logToFile(`  Daily summary energy: ${String(finalState.summary_energy)} MWh`);
    await logToFile(`  Daily summary payment: £${String(finalState.summary_payment)}`);
    
    // Step 8: Note expected value from Elexon API
    await logToFile(`\nExpected Elexon API payment: £3,784,089.62`);
    const actualPayment = Math.abs(parseFloat(String(finalState.summary_payment)));
    const percentageOfExpected = ((actualPayment / 3784089.62) * 100).toFixed(2);
    await logToFile(`Current payment after fix: £${actualPayment.toFixed(2)} (${percentageOfExpected}% of expected)`);
    
    // Check if there's still a significant discrepancy
    if (parseFloat(percentageOfExpected) < 95) {
      await logToFile(`\nNOTE: There's still a significant discrepancy with the expected Elexon API total.`);
      await logToFile(`This suggests we may be missing records or there could be calculation differences.`);
      await logToFile(`Consider re-fetching the full data from Elexon API for this date.`);
    }
    
    await logToFile(`\nFix completed successfully`);
    
  } catch (error) {
    await logToFile(`Error during fix: ${error}`);
    console.error('Error:', error);
  }
}

// Run the fix
comprehensiveFix().then(() => {
  console.log('Script completed');
  process.exit(0);
}).catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});
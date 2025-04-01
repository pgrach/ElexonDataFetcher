/**
 * Fix Duplicates and Daily Summary for 2025-03-28
 * 
 * This script:
 * 1. Identifies and removes duplicate records for the same farm/period
 * 2. Keeps only the latest record based on created_at timestamp
 * 3. Recalculates the total payment and curtailed energy
 * 4. Updates the daily_summaries table with correct values
 */

import { db } from './db';
import { sql, eq } from 'drizzle-orm';
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from './db/schema';
import * as fs from 'fs';

const DATE_TO_FIX = '2025-03-28';
const LOG_FILE = `fix_duplicates_${DATE_TO_FIX}.log`;

// Helper function to log to file
async function logToFile(message: string): Promise<void> {
  const timestamp = new Date().toISOString().replace('T', ' ').split('.')[0];
  const logMessage = `[${timestamp}] ${message}\n`;
  fs.appendFileSync(LOG_FILE, logMessage);
  console.log(message);
}

async function fixDuplicatesAndSummary() {
  try {
    await logToFile(`Starting fix for ${DATE_TO_FIX}...`);
    
    // 1. First, let's identify all the duplicate combinations
    const duplicateQuery = await db.execute(sql`
      SELECT 
        settlement_date, 
        settlement_period, 
        farm_id, 
        COUNT(*) as record_count
      FROM curtailment_records
      WHERE settlement_date = ${DATE_TO_FIX}
      GROUP BY settlement_date, settlement_period, farm_id
      HAVING COUNT(*) > 1
      ORDER BY record_count DESC
    `);
    
    const duplicateCombos = duplicateQuery.rows;
    await logToFile(`Found ${duplicateCombos.length} farm/period combinations with duplicates`);
    
    // Calculate total duplicate records
    let totalDuplicateRecords = 0;
    duplicateCombos.forEach(combo => {
      totalDuplicateRecords += parseInt(combo.record_count) - 1; // -1 because we want to keep one record
    });
    await logToFile(`Total number of duplicate records to remove: ${totalDuplicateRecords}`);
    
    // 2. For each duplicate combination, keep only the most recent record
    let processedCombos = 0;
    let removedRecords = 0;
    
    for (const combo of duplicateCombos) {
      const period = combo.settlement_period;
      const farmId = combo.farm_id;
      
      // Get all records for this combination
      const records = await db.execute(sql`
        SELECT id, created_at, volume, payment
        FROM curtailment_records
        WHERE settlement_date = ${DATE_TO_FIX}
        AND settlement_period = ${period}
        AND farm_id = ${farmId}
        ORDER BY created_at DESC
      `);
      
      // Keep the first record (most recent by created_at) and delete the rest
      const recordsToKeep = records.rows[0];
      const recordsToDelete = records.rows.slice(1);
      
      // Log the records for this combination
      await logToFile(`Processing ${farmId} Period ${period}: Found ${records.rows.length} records`);
      await logToFile(`  Keeping record ID ${recordsToKeep.id} with payment ${recordsToKeep.payment}`);
      
      // Delete the duplicate records
      for (const record of recordsToDelete) {
        await db.execute(sql`
          DELETE FROM curtailment_records
          WHERE id = ${record.id}
        `);
        removedRecords++;
        await logToFile(`  Deleted record ID ${record.id} with payment ${record.payment}`);
      }
      
      processedCombos++;
      if (processedCombos % 100 === 0) {
        await logToFile(`Processed ${processedCombos}/${duplicateCombos.length} combinations, removed ${removedRecords} records so far...`);
      }
    }
    
    await logToFile(`\nCompleted deduplication: Processed ${processedCombos} combinations, removed ${removedRecords} records`);
    
    // 3. Now recalculate the totals from the cleaned-up table
    await logToFile('\nRecalculating totals from clean data...');
    
    // Get the actual totals from the curtailment_records table after deduplication
    const totalsResult = await db.execute(sql`
      SELECT 
        SUM(ABS(CAST(volume AS numeric))) as total_curtailed_energy,
        SUM(CAST(payment AS numeric)) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${DATE_TO_FIX}
    `);
    
    if (!totalsResult.rows || totalsResult.rows.length === 0) {
      await logToFile(`No curtailment records found for ${DATE_TO_FIX}`);
      return;
    }
    
    const totalCurtailedEnergy = parseFloat(totalsResult.rows[0].total_curtailed_energy || '0');
    const totalPayment = parseFloat(totalsResult.rows[0].total_payment || '0');
    
    // Get current values in the daily_summaries table
    const currentSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, DATE_TO_FIX));
      
    await logToFile('\nCurrent values in daily_summaries:');
    if (currentSummary.length > 0) {
      await logToFile(`  Energy: ${currentSummary[0].totalCurtailedEnergy} MWh`);
      await logToFile(`  Payment: £${currentSummary[0].totalPayment}`);
    } else {
      await logToFile('  No existing summary found');
    }
    
    await logToFile('\nRecalculated values from curtailment_records after deduplication:');
    await logToFile(`  Energy: ${totalCurtailedEnergy.toFixed(2)} MWh`);
    await logToFile(`  Payment: £${totalPayment.toFixed(2)}`);
    
    // 4. Update the daily_summaries table with the corrected values
    await db.insert(dailySummaries)
      .values({
        summaryDate: DATE_TO_FIX,
        totalCurtailedEnergy: totalCurtailedEnergy.toString(),
        totalPayment: totalPayment.toString(),
        lastUpdated: new Date()
      })
      .onConflictDoUpdate({
        target: [dailySummaries.summaryDate],
        set: {
          totalCurtailedEnergy: totalCurtailedEnergy.toString(),
          totalPayment: totalPayment.toString(),
          lastUpdated: new Date()
        }
      });
    
    await logToFile('\nDaily summary updated successfully');
    
    // 5. Now update the monthly summary for March 2025
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
      const monthlyEnergy = parseFloat(monthlyTotals[0].totalCurtailedEnergy);
      const monthlyPayment = parseFloat(monthlyTotals[0].totalPayment);
      
      // Update monthly summary
      await db.insert(monthlySummaries)
        .values({
          yearMonth,
          totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
          totalPayment: monthlyTotals[0].totalPayment,
          updatedAt: new Date()
        })
        .onConflictDoUpdate({
          target: [monthlySummaries.yearMonth],
          set: {
            totalCurtailedEnergy: monthlyTotals[0].totalCurtailedEnergy,
            totalPayment: monthlyTotals[0].totalPayment,
            updatedAt: new Date()
          }
        });
      
      await logToFile(`\nMonthly summary for ${yearMonth} updated:`);
      await logToFile(`  Energy: ${monthlyEnergy.toFixed(2)} MWh`);
      await logToFile(`  Payment: £${monthlyPayment.toFixed(2)}`);
    }
    
    // 6. Update yearly summary for 2025
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
      const yearlyEnergy = parseFloat(yearlyTotals[0].totalCurtailedEnergy);
      const yearlyPayment = parseFloat(yearlyTotals[0].totalPayment);
      
      // Update yearly summary
      await db.insert(yearlySummaries)
        .values({
          year,
          totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
          totalPayment: yearlyTotals[0].totalPayment,
          updatedAt: new Date()
        })
        .onConflictDoUpdate({
          target: [yearlySummaries.year],
          set: {
            totalCurtailedEnergy: yearlyTotals[0].totalCurtailedEnergy,
            totalPayment: yearlyTotals[0].totalPayment,
            updatedAt: new Date()
          }
        });
      
      await logToFile(`\nYearly summary for ${year} updated:`);
      await logToFile(`  Energy: ${yearlyEnergy.toFixed(2)} MWh`);
      await logToFile(`  Payment: £${yearlyPayment.toFixed(2)}`);
    }
    
    await logToFile('\nFix completed successfully');
    
  } catch (error) {
    await logToFile(`Error fixing daily summary: ${error}`);
  }
}

// Run the fix
fixDuplicatesAndSummary().then(() => {
  console.log('Script completed');
  process.exit(0);
}).catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});
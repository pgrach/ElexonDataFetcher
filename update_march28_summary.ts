/**
 * Update March 28, 2025 Summary
 * 
 * This script:
 * 1. Calculates totals from the curtailment_records table for 2025-03-28
 * 2. Updates the daily_summaries table with these totals
 * 3. Updates the monthly and yearly summaries accordingly
 */

import { db } from './db';
import { sql, eq } from 'drizzle-orm';
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from './db/schema';
import * as fs from 'fs';

// Configuration
const DATE_TO_UPDATE = '2025-03-28';
const LOG_FILE = `update_summary_${DATE_TO_UPDATE}.log`;

// Helper function to log to file
function log(message: string, level: "info" | "error" | "warning" | "success" = "info"): void {
  const timestamp = new Date().toISOString().replace('T', ' ').split('.')[0];
  const logMessage = `[${timestamp}] ${message}\n`;
  fs.appendFileSync(LOG_FILE, logMessage);
  
  // Also log to console with colors for better visibility
  const colors = {
    info: '\x1b[36m', // Cyan
    error: '\x1b[31m', // Red
    warning: '\x1b[33m', // Yellow
    success: '\x1b[32m', // Green
    reset: '\x1b[0m' // Reset
  };
  
  console.log(`${colors[level]}${message}${colors.reset}`);
}

async function updateSummaries(): Promise<void> {
  try {
    log(`Updating summaries for ${DATE_TO_UPDATE}`, "info");
    
    // Recalculate totals from the curtailment_records table
    const totalQuery = await db.execute(sql`
      SELECT
        SUM(ABS(CAST(volume AS numeric))) as total_curtailed_energy,
        SUM(CAST(payment AS numeric)) as total_payment,
        COUNT(*) as record_count
      FROM curtailment_records
      WHERE settlement_date = ${DATE_TO_UPDATE}
    `);
    
    if (!totalQuery.rows[0]) {
      log(`No records found for ${DATE_TO_UPDATE}`, "error");
      return;
    }
    
    const totalCurtailedEnergy = totalQuery.rows[0].total_curtailed_energy || '0';
    const totalPayment = totalQuery.rows[0].total_payment || '0';
    const recordCount = totalQuery.rows[0].record_count || '0';
    
    log(`Found ${recordCount} records for ${DATE_TO_UPDATE}:`, "info");
    log(`Energy: ${parseFloat(totalCurtailedEnergy).toFixed(2)} MWh`, "info");
    log(`Payment: £${parseFloat(totalPayment).toFixed(2)}`, "info");
    
    // Update the daily summary
    await db.insert(dailySummaries)
      .values({
        summaryDate: DATE_TO_UPDATE,
        totalCurtailedEnergy: totalCurtailedEnergy,
        totalPayment: totalPayment,
        lastUpdated: new Date()
      })
      .onConflictDoUpdate({
        target: [dailySummaries.summaryDate],
        set: {
          totalCurtailedEnergy: totalCurtailedEnergy,
          totalPayment: totalPayment,
          lastUpdated: new Date()
        }
      });
    
    log(`Updated daily summary for ${DATE_TO_UPDATE}`, "success");
    
    // Update monthly summary for the month containing DATE_TO_UPDATE
    const yearMonth = DATE_TO_UPDATE.substring(0, 7); // '2025-03'
    
    // Recalculate monthly totals from all daily summaries in this month
    const monthlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('month', ${dailySummaries.summaryDate}::date) = date_trunc('month', ${DATE_TO_UPDATE}::date)`);
    
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
      
      log(`Updated monthly summary for ${yearMonth}:`, "success");
      log(`Energy: ${parseFloat(monthlyTotals[0].totalCurtailedEnergy).toFixed(2)} MWh`, "success");
      log(`Payment: £${parseFloat(monthlyTotals[0].totalPayment).toFixed(2)}`, "success");
    }
    
    // Update yearly summary for the year containing DATE_TO_UPDATE
    const year = DATE_TO_UPDATE.substring(0, 4); // '2025'
    
    // Recalculate yearly totals from all daily summaries in this year
    const yearlyTotals = await db
      .select({
        totalCurtailedEnergy: sql<string>`SUM(${dailySummaries.totalCurtailedEnergy}::numeric)`,
        totalPayment: sql<string>`SUM(${dailySummaries.totalPayment}::numeric)`
      })
      .from(dailySummaries)
      .where(sql`date_trunc('year', ${dailySummaries.summaryDate}::date) = date_trunc('year', ${DATE_TO_UPDATE}::date)`);
    
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
      
      log(`Updated yearly summary for ${year}:`, "success");
      log(`Energy: ${parseFloat(yearlyTotals[0].totalCurtailedEnergy).toFixed(2)} MWh`, "success");
      log(`Payment: £${parseFloat(yearlyTotals[0].totalPayment).toFixed(2)}`, "success");
    }
    
    // Compare to expected value from Elexon API
    const expectedElexonTotal = 3784089.62;
    const currentPayment = Math.abs(parseFloat(totalPayment));
    const percentageOfExpected = (currentPayment / expectedElexonTotal) * 100;
    
    log(`\nExpected Elexon API payment: £${expectedElexonTotal.toFixed(2)}`, "info");
    log(`Current payment: £${currentPayment.toFixed(2)} (${percentageOfExpected.toFixed(2)}% of expected)`, 
      percentageOfExpected >= 95 ? "success" : "warning");
    
  } catch (error) {
    log(`Error updating summaries: ${error}`, "error");
    throw error;
  }
}

// Run the update
(async () => {
  log(`Starting summary update for ${DATE_TO_UPDATE}\n`, "info");
  
  try {
    await updateSummaries();
    log(`\nSummary update completed successfully!`, "success");
  } catch (error) {
    log(`\nSummary update failed: ${error}`, "error");
    process.exit(1);
  }
})();
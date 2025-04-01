/**
 * Fix Daily Summary for 2025-03-28
 * 
 * This script recalculates the total payment and curtailed energy for 2025-03-28
 * based on the raw curtailment_records table and updates the daily_summaries table.
 */

import { db } from './db';
import { sql, eq } from 'drizzle-orm';
import { curtailmentRecords, dailySummaries, monthlySummaries, yearlySummaries } from './db/schema';

const DATE_TO_FIX = '2025-03-28';

async function fixDailySummary() {
  try {
    console.log(`Fixing daily summary for ${DATE_TO_FIX}`);
    
    // Get the actual totals from the curtailment_records table
    const totalsResult = await db.execute(sql`
      SELECT 
        SUM(ABS(CAST(volume AS numeric))) as total_curtailed_energy,
        SUM(CAST(payment AS numeric)) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${DATE_TO_FIX}
    `);
    
    if (!totalsResult.rows || totalsResult.rows.length === 0) {
      console.error(`No curtailment records found for ${DATE_TO_FIX}`);
      return;
    }
    
    const totalCurtailedEnergy = parseFloat(totalsResult.rows[0].total_curtailed_energy || '0');
    const totalPayment = parseFloat(totalsResult.rows[0].total_payment || '0');
    
    console.log('Current values in daily_summaries:');
    const currentSummary = await db
      .select()
      .from(dailySummaries)
      .where(eq(dailySummaries.summaryDate, DATE_TO_FIX));
      
    if (currentSummary.length > 0) {
      console.log(`  Energy: ${currentSummary[0].totalCurtailedEnergy} MWh`);
      console.log(`  Payment: £${currentSummary[0].totalPayment}`);
    } else {
      console.log('  No existing summary found');
    }
    
    console.log('\nRecalculated values from curtailment_records:');
    console.log(`  Energy: ${totalCurtailedEnergy.toFixed(2)} MWh`);
    console.log(`  Payment: £${totalPayment.toFixed(2)}`);
    
    // Update the daily_summaries table
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
    
    console.log('\nDaily summary updated successfully');
    
    // Now update the monthly summary for March 2025
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
      
      console.log(`\nMonthly summary for ${yearMonth} updated:`);
      console.log(`  Energy: ${monthlyEnergy.toFixed(2)} MWh`);
      console.log(`  Payment: £${monthlyPayment.toFixed(2)}`);
    }
    
    // Update yearly summary for 2025
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
      
      console.log(`\nYearly summary for ${year} updated:`);
      console.log(`  Energy: ${yearlyEnergy.toFixed(2)} MWh`);
      console.log(`  Payment: £${yearlyPayment.toFixed(2)}`);
    }
    
    console.log('\nFix completed successfully');
    
  } catch (error) {
    console.error('Error fixing daily summary:', error);
  }
}

// Run the fix
fixDailySummary().then(() => {
  console.log('Script completed');
  process.exit(0);
}).catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});
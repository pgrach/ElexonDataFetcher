/**
 * Update Summary Tables for March 22, 2025
 * 
 * This script calculates and updates the daily, monthly, and yearly summaries
 * based on the curtailment records for March 22, 2025 that have been ingested.
 */

import { db } from "./db";
import { dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import { calculateDailyBitcoinMiningPotential } from "./server/services/bitcoin";
import { Logger } from "./server/utils/logger";

const logger = new Logger("update_march_22_summaries");
const TARGET_DATE = "2025-03-22";
const TARGET_MONTH = "2025-03";
const TARGET_YEAR = "2025";

// Update daily, monthly, and yearly summaries
async function updateSummaries(): Promise<void> {
  try {
    logger.info(`=== Updating Summaries for March 22, 2025 ===`);
    
    // Calculate raw totals from curtailment_records
    const [rawTotals] = await db.execute(sql`
      SELECT 
        ABS(SUM(CAST(volume AS NUMERIC))) as total_volume,
        SUM(CAST(payment AS NUMERIC)) as total_payment 
      FROM curtailment_records 
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const totalVolume = parseFloat(rawTotals.total_volume);
    const totalPayment = parseFloat(rawTotals.total_payment);
    
    logger.info(`Raw totals from database:`);
    logger.info(`- Energy: ${totalVolume.toFixed(2)} MWh`);
    logger.info(`- Payment: ${totalPayment.toFixed(2)}`);
    
    // 1. Update daily summary
    await db.delete(dailySummaries)
      .where(eq(dailySummaries.date, TARGET_DATE));
    
    await db.insert(dailySummaries).values({
      date: TARGET_DATE,
      totalCurtailedEnergy: totalVolume.toString(),
      totalPayment: totalPayment.toString(),
      leadParty: null
    });
    
    logger.success(`Daily summary updated for ${TARGET_DATE}:`);
    logger.info(`- Energy: ${totalVolume.toFixed(2)} MWh`);
    logger.info(`- Payment: £${totalPayment.toFixed(2)}`);
    
    // 2. Update monthly summary by recalculating from all days in the month
    const [monthlyTotals] = await db.execute(sql`
      SELECT 
        SUM(CAST(total_curtailed_energy AS NUMERIC)) as monthly_volume,
        SUM(CAST(total_payment AS NUMERIC)) as monthly_payment 
      FROM daily_summaries 
      WHERE date >= ${TARGET_MONTH + '-01'} AND date <= ${TARGET_MONTH + '-31'}
    `);
    
    const monthlyVolume = parseFloat(monthlyTotals.monthly_volume);
    const monthlyPayment = parseFloat(monthlyTotals.monthly_payment);
    
    await db.delete(monthlySummaries)
      .where(eq(monthlySummaries.yearMonth, TARGET_MONTH));
    
    await db.insert(monthlySummaries).values({
      yearMonth: TARGET_MONTH,
      totalCurtailedEnergy: monthlyVolume.toString(),
      totalPayment: monthlyPayment.toString()
    });
    
    logger.success(`Monthly summary updated for ${TARGET_MONTH}:`);
    logger.info(`- Energy: ${monthlyVolume.toFixed(2)} MWh`);
    logger.info(`- Payment: £${monthlyPayment.toFixed(2)}`);
    
    // 3. Update yearly summary from monthly totals
    const [yearlyTotals] = await db.execute(sql`
      SELECT 
        SUM(CAST(total_curtailed_energy AS NUMERIC)) as yearly_volume,
        SUM(CAST(total_payment AS NUMERIC)) as yearly_payment 
      FROM monthly_summaries 
      WHERE year_month LIKE ${TARGET_YEAR + '%'}
    `);
    
    const yearlyVolume = parseFloat(yearlyTotals.yearly_volume);
    const yearlyPayment = parseFloat(yearlyTotals.yearly_payment);
    
    await db.delete(yearlySummaries)
      .where(eq(yearlySummaries.year, TARGET_YEAR));
    
    await db.insert(yearlySummaries).values({
      year: TARGET_YEAR,
      totalCurtailedEnergy: yearlyVolume.toString(),
      totalPayment: yearlyPayment.toString()
    });
    
    logger.success(`Yearly summary updated for ${TARGET_YEAR}:`);
    logger.info(`- Energy: ${yearlyVolume.toFixed(2)} MWh`);
    logger.info(`- Payment: £${yearlyPayment.toFixed(2)}`);
  } catch (error) {
    logger.error(`Error updating summaries: ${error}`);
    throw error;
  }
}

// Update Bitcoin mining calculations
async function updateBitcoinCalculations(): Promise<void> {
  try {
    logger.info(`Updating Bitcoin calculations for ${TARGET_DATE}...`);
    
    // Clear existing Bitcoin calculations
    await db.query(
      `DELETE FROM historical_bitcoin_calculations 
       WHERE settlement_date = $1`,
      [TARGET_DATE]
    );
    
    // Fetch all unique farm IDs for the target date
    const farmResult = await db.query(
      `SELECT DISTINCT farm_id FROM curtailment_records WHERE settlement_date = $1`,
      [TARGET_DATE]
    );
    
    const farmIds = farmResult.rows.map((row: any) => row.farm_id);
    logger.info(`Found ${farmIds.length} unique farm IDs`);
    
    // Calculate Bitcoin potential for S19J_PRO model
    const minerModel = "S19J_PRO";
    
    // Create a record of all settlements and their Bitcoin mining potential
    await calculateDailyBitcoinMiningPotential(TARGET_DATE, minerModel);
    
    // Verify the calculations were completed
    const verifyResult = await db.query(
      `SELECT COUNT(*) as record_count FROM historical_bitcoin_calculations 
       WHERE settlement_date = $1 AND miner_model = $2`,
      [TARGET_DATE, minerModel]
    );
    
    const recordCount = parseInt(verifyResult.rows[0].record_count);
    logger.success(`Created ${recordCount} Bitcoin calculation records for ${TARGET_DATE}`);
    
    // Get the total Bitcoin mined for verification
    const totalResult = await db.query(
      `SELECT SUM(bitcoin_mined) as total_btc FROM historical_bitcoin_calculations 
       WHERE settlement_date = $1 AND miner_model = $2`,
      [TARGET_DATE, minerModel]
    );
    
    const totalBtc = parseFloat(totalResult.rows[0].total_btc);
    logger.success(`Total Bitcoin mining potential: ${totalBtc.toFixed(8)} BTC`);
  } catch (error) {
    logger.error(`Error updating Bitcoin calculations: ${error}`);
    throw error;
  }
}

// Main function
async function main(): Promise<void> {
  try {
    await updateSummaries();
    await updateBitcoinCalculations();
    
    logger.success(`Successfully updated all summary tables for ${TARGET_DATE}`);
    
    // Final verification
    const verifyResult = await db.query(
      `SELECT COUNT(*) as period_count FROM 
       (SELECT DISTINCT settlement_period FROM curtailment_records 
        WHERE settlement_date = $1) as periods`,
      [TARGET_DATE]
    );
    
    const periodCount = parseInt(verifyResult.rows[0].period_count);
    
    if (periodCount === 48) {
      logger.success(`VERIFICATION PASSED: All 48 settlement periods are present for ${TARGET_DATE}`);
    } else {
      logger.warning(`VERIFICATION WARNING: Only ${periodCount}/48 settlement periods present for ${TARGET_DATE}`);
      
      // Show which periods are missing
      const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
      const existingPeriodsResult = await db.query(
        `SELECT DISTINCT settlement_period FROM curtailment_records WHERE settlement_date = $1`,
        [TARGET_DATE]
      );
      
      const existingPeriods = new Set(existingPeriodsResult.rows.map((row: any) => row.settlement_period));
      const missingPeriods = allPeriods.filter(p => !existingPeriods.has(p));
      
      if (missingPeriods.length > 0) {
        logger.warning(`Missing periods: ${missingPeriods.join(', ')}`);
        logger.info(`To complete the data, run staged_reingest_march_22.ts with these periods`);
      }
    }
  } catch (error) {
    logger.error(`Error in main function: ${error}`);
    process.exit(1);
  } finally {
    await db.end();
    process.exit(0);
  }
}

// Run the script
main();
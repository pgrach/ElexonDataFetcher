/**
 * Update Summary Tables for March 22, 2025
 * 
 * This script calculates and updates the daily, monthly, and yearly summaries
 * based on the curtailment records for March 22, 2025 that have been ingested.
 */

import { db } from "./db";
import { dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { sql, eq } from "drizzle-orm";
import { processSingleDay } from "./server/services/bitcoinService";

const TARGET_DATE = "2025-03-22";
const YEAR_MONTH = "2025-03";
const YEAR = "2025";

// Update summaries at all levels (daily, monthly, yearly)
async function updateSummaries(): Promise<void> {
  console.log(`Updating summary tables for ${TARGET_DATE}...`);
  
  try {
    // Step 1: Calculate daily totals from curtailment_records
    const dailyResults = await db.query(`
      SELECT 
        SUM(ABS(CAST(volume AS DECIMAL))) AS total_volume,
        SUM(ABS(CAST(payment AS DECIMAL))) AS total_payment,
        COUNT(*) AS record_count
      FROM curtailment_records
      WHERE settlement_date = $1
    `, [TARGET_DATE]);
    
    if (dailyResults.rows.length === 0 || !dailyResults.rows[0].total_volume) {
      console.warn(`No curtailment records found for ${TARGET_DATE}`);
      return;
    }
    
    const { total_volume, total_payment, record_count } = dailyResults.rows[0];
    console.log(`Daily totals: ${total_volume} MWh, £${total_payment}, ${record_count} records`);
    
    // Step 2: Update or insert daily summary
    const existingDaily = await db.query(`
      SELECT * FROM daily_summaries WHERE date = $1
    `, [TARGET_DATE]);
    
    if (existingDaily.rows.length > 0) {
      // Update existing record
      await db.query(`
        UPDATE daily_summaries
        SET 
          curtailed_energy = $1,
          curtailment_payment = $2,
          record_count = $3,
          last_updated = CURRENT_TIMESTAMP
        WHERE date = $4
      `, [total_volume, total_payment, record_count, TARGET_DATE]);
      console.log(`Updated existing daily summary for ${TARGET_DATE}`);
    } else {
      // Insert new record
      await db.query(`
        INSERT INTO daily_summaries (
          date,
          curtailed_energy,
          curtailment_payment,
          record_count
        ) VALUES ($1, $2, $3, $4)
      `, [TARGET_DATE, total_volume, total_payment, record_count]);
      console.log(`Created new daily summary for ${TARGET_DATE}`);
    }
    
    // Step 3: Update monthly summary
    const monthlyResults = await db.query(`
      SELECT 
        SUM(curtailed_energy) AS total_volume,
        SUM(curtailment_payment) AS total_payment,
        SUM(record_count) AS record_count
      FROM daily_summaries
      WHERE date LIKE $1
    `, [YEAR_MONTH + '%']);
    
    if (monthlyResults.rows.length > 0 && monthlyResults.rows[0].total_volume) {
      const { total_volume, total_payment, record_count } = monthlyResults.rows[0];
      console.log(`Monthly totals for ${YEAR_MONTH}: ${total_volume} MWh, £${total_payment}, ${record_count} records`);
      
      // Update or insert monthly summary
      const existingMonthly = await db.query(`
        SELECT * FROM monthly_summaries WHERE year_month = $1
      `, [YEAR_MONTH]);
      
      if (existingMonthly.rows.length > 0) {
        await db.query(`
          UPDATE monthly_summaries
          SET 
            curtailed_energy = $1,
            curtailment_payment = $2,
            record_count = $3,
            last_updated = CURRENT_TIMESTAMP
          WHERE year_month = $4
        `, [total_volume, total_payment, record_count, YEAR_MONTH]);
        console.log(`Updated existing monthly summary for ${YEAR_MONTH}`);
      } else {
        await db.query(`
          INSERT INTO monthly_summaries (
            year_month,
            curtailed_energy,
            curtailment_payment,
            record_count
          ) VALUES ($1, $2, $3, $4)
        `, [YEAR_MONTH, total_volume, total_payment, record_count]);
        console.log(`Created new monthly summary for ${YEAR_MONTH}`);
      }
    }
    
    // Step 4: Update yearly summary
    const yearlyResults = await db.query(`
      SELECT 
        SUM(curtailed_energy) AS total_volume,
        SUM(curtailment_payment) AS total_payment,
        SUM(record_count) AS record_count
      FROM monthly_summaries
      WHERE year_month LIKE $1
    `, [YEAR + '%']);
    
    if (yearlyResults.rows.length > 0 && yearlyResults.rows[0].total_volume) {
      const { total_volume, total_payment, record_count } = yearlyResults.rows[0];
      console.log(`Yearly totals for ${YEAR}: ${total_volume} MWh, £${total_payment}, ${record_count} records`);
      
      // Update or insert yearly summary
      const existingYearly = await db.query(`
        SELECT * FROM yearly_summaries WHERE year = $1
      `, [YEAR]);
      
      if (existingYearly.rows.length > 0) {
        await db.query(`
          UPDATE yearly_summaries
          SET 
            curtailed_energy = $1,
            curtailment_payment = $2,
            record_count = $3,
            last_updated = CURRENT_TIMESTAMP
          WHERE year = $4
        `, [total_volume, total_payment, record_count, YEAR]);
        console.log(`Updated existing yearly summary for ${YEAR}`);
      } else {
        await db.query(`
          INSERT INTO yearly_summaries (
            year,
            curtailed_energy,
            curtailment_payment,
            record_count
          ) VALUES ($1, $2, $3, $4)
        `, [YEAR, total_volume, total_payment, record_count]);
        console.log(`Created new yearly summary for ${YEAR}`);
      }
    }
    
    console.log('All summary tables updated successfully');
  } catch (error) {
    console.error('Error updating summaries:', error);
    throw error;
  }
}

// Update Bitcoin calculations
async function updateBitcoinCalculations(): Promise<void> {
  console.log(`Updating Bitcoin calculations for ${TARGET_DATE}...`);
  
  try {
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      await processSingleDay(TARGET_DATE, minerModel);
      console.log(`- Processed ${minerModel}`);
    }
    
    console.log('Bitcoin calculations updated successfully');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
    throw error;
  }
}

// Main function to control execution flow
async function main(): Promise<void> {
  try {
    console.log(`=== Starting summary updates for ${TARGET_DATE} ===`);
    
    // Step 1: Update summary tables
    await updateSummaries();
    
    // Step 2: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    console.log(`\n=== Completed all updates for ${TARGET_DATE} ===`);
    console.log(`Daily, monthly, and yearly summaries have been updated.`);
    console.log(`Bitcoin mining calculations have been updated for all miner models.`);
  } catch (error) {
    console.error('Error during update process:', error);
    process.exit(1);
  }
}

// Run the main function
main()
  .then(() => {
    console.log('All summaries and calculations updated successfully.');
    process.exit(0);
  })
  .catch(error => {
    console.error('Unhandled error:', error);
    process.exit(1);
  });
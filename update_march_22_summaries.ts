/**
 * Update Summary Tables for March 22, 2025
 * 
 * This script calculates and updates the daily, monthly, and yearly summaries
 * based on the curtailment records for March 22, 2025 that have been ingested.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import fs from 'fs';
import path from 'path';

// Configuration
const TARGET_DATE = '2025-03-22';

// Create log directory if it doesn't exist
const LOG_DIR = path.join(__dirname, 'logs');
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR);
}

// Set up logging
const logFile = path.join(LOG_DIR, `update_summaries_march_22.log`);
const logStream = fs.createWriteStream(logFile, { flags: 'a' });

function log(message: string): void {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] ${message}`;
  console.log(formattedMessage);
  logStream.write(formattedMessage + '\n');
}

async function updateSummaries(): Promise<void> {
  try {
    log(`=== Updating Summaries for ${TARGET_DATE} ===`);
    
    // Calculate totals from curtailment records
    const totalsResult = await db.execute(sql`
      SELECT 
        SUM(curtailed_volume) as total_energy,
        SUM(payment) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const totalEnergy = parseFloat(totalsResult.rows[0].total_energy as string) || 0;
    const totalPayment = parseFloat(totalsResult.rows[0].total_payment as string) || 0;
    
    log(`Raw totals from database:`);
    log(`- Energy: ${totalEnergy.toFixed(2)} MWh`);
    log(`- Payment: ${totalPayment.toFixed(2)}`);
    
    // Update or insert daily summary
    await db.execute(sql`
      INSERT INTO daily_summaries (
        date,
        total_curtailed_energy,
        total_payment
      ) VALUES (
        ${TARGET_DATE},
        ${totalEnergy},
        ${totalPayment}
      )
      ON CONFLICT (date) DO UPDATE SET
        total_curtailed_energy = ${totalEnergy},
        total_payment = ${totalPayment}
    `);
    
    log(`Daily summary updated for ${TARGET_DATE}:`);
    log(`- Energy: ${totalEnergy.toFixed(2)} MWh`);
    log(`- Payment: £${totalPayment.toFixed(2)}`);
    
    // Extract year and month for monthly/yearly summaries
    const [year, month] = TARGET_DATE.split('-');
    const yearMonth = `${year}-${month}`;
    
    // Update monthly summary
    const monthlyResult = await db.execute(sql`
      SELECT 
        SUM(total_curtailed_energy) as monthly_energy,
        SUM(total_payment) as monthly_payment
      FROM daily_summaries
      WHERE to_char(date, 'YYYY-MM') = ${yearMonth}
    `);
    
    const monthlyEnergy = parseFloat(monthlyResult.rows[0].monthly_energy) || 0;
    const monthlyPayment = parseFloat(monthlyResult.rows[0].monthly_payment) || 0;
    
    await db.execute(sql`
      INSERT INTO monthly_summaries (
        year_month,
        total_curtailed_energy,
        total_payment
      ) VALUES (
        ${yearMonth},
        ${monthlyEnergy},
        ${monthlyPayment}
      )
      ON CONFLICT (year_month) DO UPDATE SET
        total_curtailed_energy = ${monthlyEnergy},
        total_payment = ${monthlyPayment}
    `);
    
    log(`Monthly summary updated for ${yearMonth}:`);
    log(`- Energy: ${monthlyEnergy.toFixed(2)} MWh`);
    log(`- Payment: £${monthlyPayment.toFixed(2)}`);
    
    // Update yearly summary
    const yearlyResult = await db.execute(sql`
      SELECT 
        SUM(total_curtailed_energy) as yearly_energy,
        SUM(total_payment) as yearly_payment
      FROM monthly_summaries
      WHERE to_char(year_month, 'YYYY') = ${year}
    `);
    
    const yearlyEnergy = parseFloat(yearlyResult.rows[0].yearly_energy) || 0;
    const yearlyPayment = parseFloat(yearlyResult.rows[0].yearly_payment) || 0;
    
    await db.execute(sql`
      INSERT INTO yearly_summaries (
        year,
        total_curtailed_energy,
        total_payment
      ) VALUES (
        ${year},
        ${yearlyEnergy},
        ${yearlyPayment}
      )
      ON CONFLICT (year) DO UPDATE SET
        total_curtailed_energy = ${yearlyEnergy},
        total_payment = ${yearlyPayment}
    `);
    
    log(`Yearly summary updated for ${year}:`);
    log(`- Energy: ${yearlyEnergy.toFixed(2)} MWh`);
    log(`- Payment: £${yearlyPayment.toFixed(2)}`);
    
  } catch (error) {
    log(`Error updating summaries: ${error}`);
    throw error;
  }
}

async function updateBitcoinCalculations(): Promise<void> {
  try {
    log(`Updating Bitcoin calculations for ${TARGET_DATE}...`);
    
    // Get the latest difficulty and price
    const minerstatResult = await db.execute(sql`
      SELECT 
        difficulty,
        price_gbp
      FROM minerstat_data
      ORDER BY timestamp DESC
      LIMIT 1
    `);
    
    const difficulty = parseFloat(minerstatResult.rows[0].difficulty);
    const priceBTC = parseFloat(minerstatResult.rows[0].price_gbp);
    
    log(`Using Bitcoin difficulty: ${difficulty}, Price: £${priceBTC}`);
    
    // Delete existing calculations for this date
    await db.execute(sql`
      DELETE FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    // Calculate Bitcoin mining potential for each farm and period
    await db.execute(sql`
      WITH curtailment_with_farm_data AS (
        SELECT 
          c.settlement_date,
          c.settlement_period,
          c.farm_id,
          c.curtailed_volume,
          f.capacity_mw,
          'S19J_PRO' as miner_model
        FROM curtailment_records c
        JOIN wind_farms f ON c.farm_id = f.id
        WHERE c.settlement_date = ${TARGET_DATE}
      )
      INSERT INTO historical_bitcoin_calculations (
        settlement_date,
        settlement_period,
        farm_id,
        miner_model,
        bitcoin_mined,
        difficulty,
        calculated_at
      )
      SELECT 
        settlement_date,
        settlement_period,
        farm_id,
        miner_model,
        -- Bitcoin mining calculation based on curtailed energy and difficulty
        (curtailed_volume * 1000 * 0.9 * 1 / NULLIF(difficulty / 1e12, 0)) as bitcoin_mined,
        ${difficulty},
        NOW()
      FROM curtailment_with_farm_data
    `);
    
    // Get total Bitcoin mined for the date
    const bitcoinResult = await db.execute(sql`
      SELECT SUM(bitcoin_mined) as total_bitcoin_mined
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const totalBitcoinMined = parseFloat(bitcoinResult.rows[0].total_bitcoin_mined) || 0;
    const totalBitcoinValue = totalBitcoinMined * priceBTC;
    
    log(`Total Bitcoin mining calculations updated for ${TARGET_DATE}:`);
    log(`- Bitcoin mined: ${totalBitcoinMined.toFixed(6)} BTC`);
    log(`- Value at current price: £${totalBitcoinValue.toFixed(2)}`);
    
  } catch (error) {
    log(`Error updating Bitcoin calculations: ${error}`);
    throw error;
  }
}

async function main(): Promise<void> {
  try {
    // Update summary tables
    await updateSummaries();
    
    // Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // Verify final results
    const finalResult = await db.execute(sql`
      SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT settlement_period) as period_count,
        SUM(curtailed_volume) as total_volume,
        SUM(payment) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const recordCount = parseInt(finalResult.rows[0].record_count);
    const periodCount = parseInt(finalResult.rows[0].period_count);
    const totalVolume = parseFloat(finalResult.rows[0].total_volume) || 0;
    const totalPayment = parseFloat(finalResult.rows[0].total_payment) || 0;
    
    log('========================================');
    log(`Final Results for ${TARGET_DATE}:`);
    log(`- Settlement Periods: ${periodCount}/48`);
    log(`- Records: ${recordCount}`);
    log(`- Total Volume: ${totalVolume.toFixed(2)} MWh`);
    log(`- Total Payment: £${Math.abs(totalPayment).toFixed(2)}`);
    
    if (periodCount === 48) {
      log('SUCCESS: All 48 settlement periods successfully processed');
    } else {
      log(`WARNING: Only ${periodCount} out of 48 settlement periods were processed`);
      
      // Get missing periods
      const periodsResult = await db.execute(sql`
        SELECT settlement_period
        FROM curtailment_records
        WHERE settlement_date = ${TARGET_DATE}
        GROUP BY settlement_period
        ORDER BY settlement_period
      `);
      
      const existingPeriods = new Set(periodsResult.rows.map(r => parseInt(r.settlement_period)));
      const missingPeriods = Array.from({ length: 48 }, (_, i) => i + 1).filter(p => !existingPeriods.has(p));
      
      log(`Missing periods: ${missingPeriods.join(', ')}`);
    }
    
  } catch (error) {
    log(`Error in main function: ${error}`);
    throw error;
  } finally {
    logStream.end();
  }
}

main()
  .then(() => {
    log('Summary update process finished');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
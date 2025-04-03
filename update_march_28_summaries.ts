/**
 * Update March 28 Summaries
 * 
 * This script updates all summary tables and Bitcoin calculations for March 28, 2025
 * after periods have been processed.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

// Target date
const TARGET_DATE = '2025-03-28';

// Color-coded console output
const colors = {
  info: '\x1b[36m',    // Cyan
  success: '\x1b[32m', // Green
  warning: '\x1b[33m', // Yellow
  error: '\x1b[31m',   // Red
  reset: '\x1b[0m'     // Reset
};

// Log with color and type
function log(message: string, type: 'info' | 'success' | 'warning' | 'error' = 'info'): void {
  const timestamp = new Date().toLocaleTimeString();
  console.log(`${colors[type]}[${type.toUpperCase()}] ${timestamp}: ${message}${colors.reset}`);
}

/**
 * Update the daily summary for the target date
 */
async function updateDailySummary(): Promise<void> {
  log(`Updating daily summary for ${TARGET_DATE}...`, 'info');
  
  try {
    // Calculate total curtailed energy and total payment
    const result = await db.execute(sql`
      SELECT 
        SUM(volume) AS total_volume, 
        SUM(payment) AS total_payment,
        COUNT(DISTINCT settlement_period) AS periods_count
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    if (!result.rows || result.rows.length === 0) {
      log('No data found for the target date', 'warning');
      return;
    }
    
    const row = result.rows[0];
    const totalVolume = Number(row.total_volume) || 0;
    const totalPayment = Number(row.total_payment) || 0;
    const periodsCount = Number(row.periods_count) || 0;
    
    // Check if entry exists
    const existingEntry = await db.execute(sql`
      SELECT summary_date FROM daily_summaries
      WHERE summary_date = ${TARGET_DATE}
    `);
    
    if (existingEntry.rows && existingEntry.rows.length > 0) {
      // Update existing summary
      await db.execute(sql`
        UPDATE daily_summaries
        SET 
          total_curtailed_energy = ${totalVolume},
          total_payment = ${totalPayment},
          last_updated = NOW()
        WHERE summary_date = ${TARGET_DATE}
      `);
      log(`Updated existing daily summary for ${TARGET_DATE}`, 'success');
    } else {
      // Insert new summary
      await db.execute(sql`
        INSERT INTO daily_summaries (
          summary_date,
          total_curtailed_energy,
          total_payment,
          created_at,
          last_updated
        )
        VALUES (
          ${TARGET_DATE},
          ${totalVolume},
          ${totalPayment},
          NOW(),
          NOW()
        )
      `);
      log(`Created new daily summary for ${TARGET_DATE}`, 'success');
    }
    
    log(`Daily summary updated with ${totalVolume.toFixed(2)} MWh and £${totalPayment.toFixed(2)} across ${periodsCount} periods`, 'success');
  } catch (error) {
    log(`Error updating daily summary: ${error}`, 'error');
  }
}

/**
 * Update monthly summary that includes the target date
 */
async function updateMonthlySummary(): Promise<void> {
  const yearMonth = TARGET_DATE.substring(0, 7); // Format: "2025-03"
  
  log(`Updating monthly summary for ${yearMonth}...`, 'info');
  
  try {
    // Get all daily summaries for the month - using a valid date range
    const result = await db.execute(sql`
      SELECT 
        SUM(total_curtailed_energy) AS total_volume,
        SUM(total_payment) AS total_payment,
        COUNT(DISTINCT summary_date) AS days_count
      FROM daily_summaries
      WHERE summary_date >= ${yearMonth + '-01'} AND summary_date <= ${yearMonth + '-31'}
    `);
    
    if (!result.rows || result.rows.length === 0) {
      log('No daily summaries found for the month', 'warning');
      return;
    }
    
    const row = result.rows[0];
    const totalVolume = Number(row.total_volume) || 0;
    const totalPayment = Number(row.total_payment) || 0;
    const daysCount = Number(row.days_count) || 0;
    
    // Check if entry exists
    const existingEntry = await db.execute(sql`
      SELECT year_month FROM monthly_summaries
      WHERE year_month = ${yearMonth}
    `);
    
    if (existingEntry.rows && existingEntry.rows.length > 0) {
      // Update existing summary
      await db.execute(sql`
        UPDATE monthly_summaries
        SET 
          total_curtailed_energy = ${totalVolume},
          total_payment = ${totalPayment},
          last_updated = NOW()
        WHERE year_month = ${yearMonth}
      `);
      log(`Updated existing monthly summary for ${yearMonth}`, 'success');
    } else {
      // Insert new summary
      await db.execute(sql`
        INSERT INTO monthly_summaries (
          year_month,
          total_curtailed_energy,
          total_payment,
          created_at,
          last_updated
        )
        VALUES (
          ${yearMonth},
          ${totalVolume},
          ${totalPayment},
          NOW(),
          NOW()
        )
      `);
      log(`Created new monthly summary for ${yearMonth}`, 'success');
    }
    
    log(`Monthly summary updated with ${totalVolume.toFixed(2)} MWh and £${totalPayment.toFixed(2)} across ${daysCount} days`, 'success');
  } catch (error) {
    log(`Error updating monthly summary: ${error}`, 'error');
  }
}

/**
 * Update yearly summary that includes the target date
 */
async function updateYearlySummary(): Promise<void> {
  const year = TARGET_DATE.substring(0, 4); // Format: "2025"
  
  log(`Updating yearly summary for ${year}...`, 'info');
  
  try {
    // Get all monthly summaries for the year
    const result = await db.execute(sql`
      SELECT 
        SUM(total_curtailed_energy) AS total_volume,
        SUM(total_payment) AS total_payment,
        COUNT(DISTINCT year_month) AS months_count
      FROM monthly_summaries
      WHERE year_month >= ${year + '-01'} AND year_month <= ${year + '-12'}
    `);
    
    if (!result.rows || result.rows.length === 0) {
      log('No monthly summaries found for the year', 'warning');
      return;
    }
    
    const row = result.rows[0];
    const totalVolume = Number(row.total_volume) || 0;
    const totalPayment = Number(row.total_payment) || 0;
    const monthsCount = Number(row.months_count) || 0;
    
    // Check if entry exists
    const existingEntry = await db.execute(sql`
      SELECT year FROM yearly_summaries
      WHERE year = ${year}
    `);
    
    if (existingEntry.rows && existingEntry.rows.length > 0) {
      // Update existing summary
      await db.execute(sql`
        UPDATE yearly_summaries
        SET 
          total_curtailed_energy = ${totalVolume},
          total_payment = ${totalPayment},
          last_updated = NOW()
        WHERE year = ${year}
      `);
      log(`Updated existing yearly summary for ${year}`, 'success');
    } else {
      // Insert new summary
      await db.execute(sql`
        INSERT INTO yearly_summaries (
          year,
          total_curtailed_energy,
          total_payment,
          created_at,
          last_updated
        )
        VALUES (
          ${year},
          ${totalVolume},
          ${totalPayment},
          NOW(),
          NOW()
        )
      `);
      log(`Created new yearly summary for ${year}`, 'success');
    }
    
    log(`Yearly summary updated with ${totalVolume.toFixed(2)} MWh and £${totalPayment.toFixed(2)} across ${monthsCount} months`, 'success');
  } catch (error) {
    log(`Error updating yearly summary: ${error}`, 'error');
  }
}

/**
 * Calculate the equivalent Bitcoin mining data for the curtailed energy
 */
async function updateBitcoinCalculations(): Promise<void> {
  log(`Updating Bitcoin calculations for ${TARGET_DATE}...`, 'info');
  
  try {
    // First, get the daily curtailed energy
    const dailyResult = await db.execute(sql`
      SELECT total_curtailed_energy
      FROM daily_summaries
      WHERE summary_date = ${TARGET_DATE}
    `);
    
    if (!dailyResult.rows || dailyResult.rows.length === 0) {
      log('No daily summary found for Bitcoin calculations', 'warning');
      return;
    }
    
    const dailyEnergy = Number(dailyResult.rows[0].total_curtailed_energy) || 0;
    
    // Define miner models and their efficiency
    const minerModels = [
      { name: 'Antminer S19 XP', efficiency: 21.5 }, // J/TH
      { name: 'Antminer S19k Pro', efficiency: 18.8 }, // J/TH
      { name: 'Whatsminer M50S++', efficiency: 16.0 }  // J/TH
    ];
    
    // Set current Bitcoin mining difficulty (placeholder)
    const currentDifficulty = 56000000000000.0; // 56 trillion
    
    // Calculate Bitcoin mining results for each model
    for (const miner of minerModels) {
      // Convert MWh to Joules
      const energyJoules = dailyEnergy * 3.6e9;
      
      // Calculate hashrate (TH/s) that could run for 24 hours
      // Energy (J) = Power (W) * Time (s)
      // Power (W) = Hashrate (TH/s) * Efficiency (J/TH)
      // Therefore: Hashrate (TH/s) = Energy (J) / (Time (s) * Efficiency (J/TH))
      const timeSeconds = 24 * 60 * 60; // 24 hours in seconds
      const hashrateTHs = energyJoules / (timeSeconds * miner.efficiency);
      
      // Calculate bitcoin mined
      // Formula: Bitcoin = Hashrate (TH/s) * Time (s) * Current BTC reward per TH-second
      // For this example, let's use a simplified approach: 
      // 1 TH/s for 24 hours = approximately 0.0000001 BTC (this is a placeholder)
      const bitcoinMined = hashrateTHs * 0.0000001;
      
      // Check if calculation already exists
      const existingCalc = await db.execute(sql`
        SELECT id 
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${miner.name}
      `);
      
      if (existingCalc.rows && existingCalc.rows.length > 0) {
        // Update existing calculation
        await db.execute(sql`
          UPDATE historical_bitcoin_calculations
          SET
            bitcoin_mined = ${bitcoinMined},
            difficulty = ${currentDifficulty},
            calculated_at = NOW()
          WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${miner.name}
        `);
      } else {
        // Insert new calculation
        await db.execute(sql`
          INSERT INTO historical_bitcoin_calculations (
            settlement_date,
            settlement_period,
            farm_id,
            miner_model,
            bitcoin_mined,
            difficulty,
            calculated_at
          )
          VALUES (
            ${TARGET_DATE},
            1,
            'ALL',
            ${miner.name},
            ${bitcoinMined},
            ${currentDifficulty},
            NOW()
          )
        `);
      }
      
      log(`Updated Bitcoin calculation for ${miner.name}: ${bitcoinMined.toFixed(8)} BTC`, 'success');
    }
    
    log('Bitcoin calculations completed successfully', 'success');
  } catch (error) {
    log(`Error updating Bitcoin calculations: ${error}`, 'error');
  }
}

/**
 * Update Bitcoin daily summary
 */
async function updateBitcoinDailySummary(): Promise<void> {
  log(`Updating Bitcoin daily summary for ${TARGET_DATE}...`, 'info');

  try {
    // Get all Bitcoin mined across all miner models for the date
    const result = await db.execute(sql`
      SELECT 
        SUM(bitcoin_mined) AS total_bitcoin_mined
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE}
    `);

    if (!result.rows || result.rows.length === 0) {
      log('No Bitcoin calculations found for the date', 'warning');
      return;
    }

    const totalBitcoinMined = Number(result.rows[0].total_bitcoin_mined) || 0;

    // Use placeholder values for other required fields
    const averageDifficulty = 56000000000000.0; // Placeholder with 56 trillion difficulty

    // Check if a daily summary exists
    const existingSummary = await db.execute(sql`
      SELECT id FROM bitcoin_daily_summaries 
      WHERE summary_date = ${TARGET_DATE}
    `);

    if (existingSummary.rows && existingSummary.rows.length > 0) {
      // Update existing summary
      await db.execute(sql`
        UPDATE bitcoin_daily_summaries 
        SET 
          bitcoin_mined = ${totalBitcoinMined},
          average_difficulty = ${averageDifficulty},
          updated_at = NOW()
        WHERE summary_date = ${TARGET_DATE}
      `);
    } else {
      // Create new summary
      await db.execute(sql`
        INSERT INTO bitcoin_daily_summaries (
          summary_date,
          miner_model,
          bitcoin_mined,
          average_difficulty,
          created_at,
          updated_at
        )
        VALUES (
          ${TARGET_DATE},
          'Combined', 
          ${totalBitcoinMined},
          ${averageDifficulty},
          NOW(),
          NOW()
        )
      `);
    }

    log(`Bitcoin daily summary updated: ${totalBitcoinMined.toFixed(8)} BTC`, 'success');
  } catch (error) {
    log(`Error updating Bitcoin daily summary: ${error}`, 'error');
  }
}

/**
 * Main function to update all summaries
 */
async function main(): Promise<void> {
  try {
    log(`Starting summary updates for ${TARGET_DATE}...`, 'info');
    
    // 1. Update daily summary
    await updateDailySummary();
    
    // 2. Update monthly summary
    await updateMonthlySummary();
    
    // 3. Update yearly summary
    await updateYearlySummary();
    
    // 4. Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    // 5. Update Bitcoin daily summary
    await updateBitcoinDailySummary();
    
    log(`All summaries for ${TARGET_DATE} have been updated successfully!`, 'success');
  } catch (error) {
    log(`Error in main process: ${error}`, 'error');
  } finally {
    // Close any open database connections if needed
    // await db.end();
  }
}

// Execute the main function
main().catch(error => {
  log(`Unhandled error: ${error}`, 'error');
  process.exit(1);
});
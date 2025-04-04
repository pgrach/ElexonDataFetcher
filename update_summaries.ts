/**
 * Update Summary Tables for March 21, 2025
 * 
 * This script calculates and updates the daily, monthly, and yearly summaries
 * based on the curtailment records for March 21, 2025.
 * Target values for reconciliation:
 * - Subsidies Paid: £1,240,439.58
 * - Energy Curtailed: 50,518.72 MWh
 */

import pg from 'pg';
import { exit } from 'process';

const { Pool } = pg;

const TARGET_DATE = '2025-03-21';

// Initialize database pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5
});

async function updateSummaries(): Promise<void> {
  console.log(`Updating summary tables for ${TARGET_DATE}...`);
  
  const client = await pool.connect();
  try {
    // 1. Update daily summary
    console.log(`Updating daily summary...`);
    await client.query(`
      INSERT INTO daily_summaries (
        summary_date,
        total_curtailed_energy, 
        total_payment,
        total_wind_generation,
        wind_onshore_generation,
        wind_offshore_generation
      )
      SELECT 
          settlement_date, 
          SUM(ABS(CAST(volume AS DECIMAL))), 
          SUM(CAST(payment AS DECIMAL)),
          0, -- total_wind_generation (will be updated by other process)
          0, -- wind_onshore_generation
          0  -- wind_offshore_generation
      FROM 
          curtailment_records
      WHERE 
          settlement_date = $1
      GROUP BY 
          settlement_date
      ON CONFLICT (summary_date) 
      DO UPDATE SET
          total_curtailed_energy = EXCLUDED.total_curtailed_energy,
          total_payment = EXCLUDED.total_payment,
          last_updated = NOW()
    `, [TARGET_DATE]);
    
    console.log('Daily summary updated');
    
    // 2. Get the year and month for monthly update
    const yearMonth = TARGET_DATE.substring(0, 7); // 'YYYY-MM' format
    
    // 3. Update monthly summary
    console.log(`Updating monthly summary for ${yearMonth}...`);
    await client.query(`
      INSERT INTO monthly_summaries (
        year_month,
        total_curtailed_energy,
        total_payment,
        total_wind_generation,
        wind_onshore_generation,
        wind_offshore_generation
      )
      SELECT 
          LEFT(summary_date::text, 7) AS year_month,
          SUM(total_curtailed_energy) AS total_curtailed_energy,
          SUM(total_payment) AS total_payment,
          SUM(total_wind_generation) AS total_wind_generation,
          SUM(wind_onshore_generation) AS wind_onshore_generation,
          SUM(wind_offshore_generation) AS wind_offshore_generation
      FROM 
          daily_summaries
      WHERE 
          LEFT(summary_date::text, 7) = $1
      GROUP BY 
          LEFT(summary_date::text, 7)
      ON CONFLICT (year_month) 
      DO UPDATE SET
          total_curtailed_energy = EXCLUDED.total_curtailed_energy,
          total_payment = EXCLUDED.total_payment,
          total_wind_generation = EXCLUDED.total_wind_generation,
          wind_onshore_generation = EXCLUDED.wind_onshore_generation,
          wind_offshore_generation = EXCLUDED.wind_offshore_generation,
          last_updated = NOW()
    `, [yearMonth]);
    
    console.log('Monthly summary updated');
    
    // 4. Get the year for yearly update
    const year = TARGET_DATE.substring(0, 4); // 'YYYY' format
    
    // 5. Update yearly summary
    console.log(`Updating yearly summary for ${year}...`);
    await client.query(`
      INSERT INTO yearly_summaries (
        year,
        total_curtailed_energy,
        total_payment,
        total_wind_generation,
        wind_onshore_generation,
        wind_offshore_generation
      )
      SELECT 
          LEFT(year_month, 4) AS year,
          SUM(total_curtailed_energy) AS total_curtailed_energy,
          SUM(total_payment) AS total_payment,
          SUM(total_wind_generation) AS total_wind_generation,
          SUM(wind_onshore_generation) AS wind_onshore_generation,
          SUM(wind_offshore_generation) AS wind_offshore_generation
      FROM 
          monthly_summaries
      WHERE 
          LEFT(year_month, 4) = $1
      GROUP BY 
          LEFT(year_month, 4)
      ON CONFLICT (year) 
      DO UPDATE SET
          total_curtailed_energy = EXCLUDED.total_curtailed_energy,
          total_payment = EXCLUDED.total_payment,
          total_wind_generation = EXCLUDED.total_wind_generation,
          wind_onshore_generation = EXCLUDED.wind_onshore_generation,
          wind_offshore_generation = EXCLUDED.wind_offshore_generation,
          last_updated = NOW()
    `, [year]);
    
    console.log('Yearly summary updated');
    
    // 6. Verify the updates
    const result = await client.query(`
      SELECT 
          'daily' AS summary_type,
          total_curtailed_energy,
          total_payment
      FROM 
          daily_summaries
      WHERE 
          summary_date = $1
      
      UNION ALL
      
      SELECT 
          'monthly' AS summary_type,
          total_curtailed_energy,
          total_payment
      FROM 
          monthly_summaries
      WHERE 
          year_month = $2
      
      UNION ALL
      
      SELECT 
          'yearly' AS summary_type,
          total_curtailed_energy,
          total_payment
      FROM 
          yearly_summaries
      WHERE 
          year = $3
    `, [TARGET_DATE, yearMonth, year]);
    
    console.log('\nVerification Results:');
    result.rows.forEach(row => {
      console.log(`- ${row.summary_type.padEnd(7)}: ${parseFloat(row.total_curtailed_energy).toFixed(2)} MWh, £${parseFloat(row.total_payment).toFixed(2)}`);
    });
    
  } catch (error) {
    console.error(`Error updating summaries: ${error}`);
    throw error;
  } finally {
    client.release();
  }
}

async function updateBitcoinCalculations(): Promise<void> {
  try {
    console.log(`\nUpdating Bitcoin calculations for ${TARGET_DATE}...`);
    
    // List of miner models to process
    const minerModels = ['S19J_PRO', 'M20S', 'S9'];
    
    // Import the Bitcoin service
    const { processSingleDay } = await import('./server/services/bitcoinService');
    
    // Process each miner model
    for (const minerModel of minerModels) {
      console.log(`Processing ${TARGET_DATE} with model ${minerModel}...`);
      await processSingleDay(TARGET_DATE, minerModel);
      console.log(`Completed Bitcoin calculations for ${minerModel}`);
    }
    
    console.log(`Bitcoin calculations updated`);
  } catch (error) {
    console.error(`Error updating Bitcoin calculations: ${error}`);
    throw error;
  }
}

async function main(): Promise<void> {
  console.log('='.repeat(50));
  console.log(`UPDATING SUMMARIES FOR ${TARGET_DATE}`);
  console.log('='.repeat(50));
  
  try {
    // Step 1: Update summary tables
    await updateSummaries();
    
    // Step 2: Update Bitcoin calculations
    await updateBitcoinCalculations();
    
    console.log('\nAll updates completed successfully!');
  } catch (error) {
    console.error(`Error in main function: ${error}`);
    exit(1);
  } finally {
    // Close the database pool
    await pool.end();
  }
}

// Run the main function
main();
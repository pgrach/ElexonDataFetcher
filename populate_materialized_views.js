/**
 * Materialized View Population Script
 * 
 * This script populates the materialized view tables with data from existing
 * historical_bitcoin_calculations and curtailment_records data.
 */

const { Pool } = require('pg');
require('dotenv').config();

// Database configuration
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Constants
const DEFAULT_DAYS_TO_POPULATE = 30;
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

/**
 * Main execution function
 */
async function main() {
  const args = process.argv.slice(2);
  const command = args[0];
  
  console.log('Starting materialized view population...');
  
  try {
    if (command === 'recent') {
      const days = parseInt(args[1]) || DEFAULT_DAYS_TO_POPULATE;
      await populateRecentMaterializedViews(days);
    } else if (command === 'range') {
      const startDate = args[1];
      const endDate = args[2];
      
      if (!startDate || !endDate) {
        console.error('Error: Both start date and end date are required for range command');
        console.log('Usage: node populate_materialized_views.js range YYYY-MM-DD YYYY-MM-DD');
        process.exit(1);
      }
      
      await populateDateRangeMaterializedViews(startDate, endDate);
    } else {
      console.log('Usage:');
      console.log('  node populate_materialized_views.js recent [days=30]');
      console.log('  node populate_materialized_views.js range YYYY-MM-DD YYYY-MM-DD');
    }
  } catch (error) {
    console.error('Error in population script:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

/**
 * Populate materialized views for recent dates
 */
async function populateRecentMaterializedViews(days = DEFAULT_DAYS_TO_POPULATE) {
  console.log(`Populating materialized views for the last ${days} days...`);
  
  const client = await pool.connect();
  try {
    // Get the date range
    const result = await client.query(`
      SELECT 
        MIN(settlement_date) AS earliest_date,
        MAX(settlement_date) AS latest_date
      FROM curtailment_records
      WHERE settlement_date >= CURRENT_DATE - INTERVAL '${days} days'
    `);
    
    const { earliest_date, latest_date } = result.rows[0];
    
    if (!earliest_date || !latest_date) {
      console.log('No data found for the specified date range');
      return;
    }
    
    console.log(`Date range: ${earliest_date} to ${latest_date}`);
    
    // Populate settlement_period_mining table
    console.log('Populating settlement_period_mining table...');
    await client.query(`
      INSERT INTO settlement_period_mining 
        (settlement_date, settlement_period, farm_id, miner_model, curtailed_energy, bitcoin_mined)
      SELECT 
        c.settlement_date,
        c.settlement_period,
        c.bmu_id AS farm_id,
        h.miner_model,
        c.volume AS curtailed_energy,
        h.bitcoin_mined
      FROM 
        curtailment_records c
      JOIN 
        historical_bitcoin_calculations h
      ON 
        c.settlement_date = h.settlement_date AND
        c.settlement_period = h.settlement_period AND
        c.bmu_id = h.farm_id
      WHERE 
        c.settlement_date BETWEEN $1 AND $2
      ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
      DO UPDATE SET
        curtailed_energy = EXCLUDED.curtailed_energy,
        bitcoin_mined = EXCLUDED.bitcoin_mined,
        updated_at = NOW()
    `, [earliest_date, latest_date]);
    
    // Populate daily_mining_potential
    console.log('Populating daily_mining_potential table...');
    for (const minerModel of MINER_MODELS) {
      await client.query(`
        INSERT INTO daily_mining_potential
          (summary_date, farm_id, miner_model, total_curtailed_energy, total_bitcoin_mined)
        SELECT 
          settlement_date AS summary_date,
          farm_id,
          miner_model,
          SUM(curtailed_energy)::TEXT AS total_curtailed_energy,
          SUM(bitcoin_mined)::TEXT AS total_bitcoin_mined
        FROM 
          settlement_period_mining
        WHERE 
          settlement_date BETWEEN $1 AND $2
          AND miner_model = $3
        GROUP BY 
          settlement_date, farm_id, miner_model
        ON CONFLICT (summary_date, farm_id, miner_model)
        DO UPDATE SET
          total_curtailed_energy = EXCLUDED.total_curtailed_energy,
          total_bitcoin_mined = EXCLUDED.total_bitcoin_mined,
          updated_at = NOW()
      `, [earliest_date, latest_date, minerModel]);
    }
    
    // Extract years from the date range
    const startYear = new Date(earliest_date).getFullYear().toString();
    const endYear = new Date(latest_date).getFullYear().toString();
    const years = [startYear];
    if (startYear !== endYear) {
      years.push(endYear);
    }
    
    // Populate yearly_mining_potential
    console.log('Populating yearly_mining_potential table...');
    for (const year of years) {
      for (const minerModel of MINER_MODELS) {
        await client.query(`
          INSERT INTO yearly_mining_potential
            (year, farm_id, miner_model, total_curtailed_energy, total_bitcoin_mined)
          SELECT 
            $1 AS year,
            farm_id,
            miner_model,
            SUM(CAST(total_curtailed_energy AS DECIMAL))::TEXT AS total_curtailed_energy,
            SUM(CAST(total_bitcoin_mined AS DECIMAL))::TEXT AS total_bitcoin_mined
          FROM 
            daily_mining_potential
          WHERE 
            EXTRACT(YEAR FROM summary_date) = $1::INTEGER
            AND miner_model = $2
          GROUP BY 
            farm_id, miner_model
          ON CONFLICT (year, farm_id, miner_model)
          DO UPDATE SET
            total_curtailed_energy = EXCLUDED.total_curtailed_energy,
            total_bitcoin_mined = EXCLUDED.total_bitcoin_mined,
            updated_at = NOW()
        `, [year, minerModel]);
      }
    }
    
    console.log('Materialized view population completed successfully!');
  } catch (error) {
    console.error('Error populating materialized views:', error);
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Populate materialized views for a specific date range
 */
async function populateDateRangeMaterializedViews(startDate, endDate) {
  console.log(`Populating materialized views for date range: ${startDate} to ${endDate}`);
  
  const client = await pool.connect();
  try {
    // Validate date range
    const validateResult = await client.query(`
      SELECT 
        COUNT(*) AS count
      FROM curtailment_records
      WHERE settlement_date BETWEEN $1 AND $2
    `, [startDate, endDate]);
    
    const { count } = validateResult.rows[0];
    
    if (count === '0') {
      console.log('No data found for the specified date range');
      return;
    }
    
    console.log(`Found ${count} curtailment records in the date range`);
    
    // Populate settlement_period_mining table
    console.log('Populating settlement_period_mining table...');
    await client.query(`
      INSERT INTO settlement_period_mining 
        (settlement_date, settlement_period, farm_id, miner_model, curtailed_energy, bitcoin_mined)
      SELECT 
        c.settlement_date,
        c.settlement_period,
        c.bmu_id AS farm_id,
        h.miner_model,
        c.volume AS curtailed_energy,
        h.bitcoin_mined
      FROM 
        curtailment_records c
      JOIN 
        historical_bitcoin_calculations h
      ON 
        c.settlement_date = h.settlement_date AND
        c.settlement_period = h.settlement_period AND
        c.bmu_id = h.farm_id
      WHERE 
        c.settlement_date BETWEEN $1 AND $2
      ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
      DO UPDATE SET
        curtailed_energy = EXCLUDED.curtailed_energy,
        bitcoin_mined = EXCLUDED.bitcoin_mined,
        updated_at = NOW()
    `, [startDate, endDate]);
    
    // Populate daily_mining_potential
    console.log('Populating daily_mining_potential table...');
    for (const minerModel of MINER_MODELS) {
      await client.query(`
        INSERT INTO daily_mining_potential
          (summary_date, farm_id, miner_model, total_curtailed_energy, total_bitcoin_mined)
        SELECT 
          settlement_date AS summary_date,
          farm_id,
          miner_model,
          SUM(curtailed_energy)::TEXT AS total_curtailed_energy,
          SUM(bitcoin_mined)::TEXT AS total_bitcoin_mined
        FROM 
          settlement_period_mining
        WHERE 
          settlement_date BETWEEN $1 AND $2
          AND miner_model = $3
        GROUP BY 
          settlement_date, farm_id, miner_model
        ON CONFLICT (summary_date, farm_id, miner_model)
        DO UPDATE SET
          total_curtailed_energy = EXCLUDED.total_curtailed_energy,
          total_bitcoin_mined = EXCLUDED.total_bitcoin_mined,
          updated_at = NOW()
      `, [startDate, endDate, minerModel]);
    }
    
    // Extract years from the date range
    const startYear = new Date(startDate).getFullYear().toString();
    const endYear = new Date(endDate).getFullYear().toString();
    const years = [startYear];
    if (startYear !== endYear) {
      years.push(endYear);
    }
    
    // Populate yearly_mining_potential
    console.log('Populating yearly_mining_potential table...');
    for (const year of years) {
      for (const minerModel of MINER_MODELS) {
        await client.query(`
          INSERT INTO yearly_mining_potential
            (year, farm_id, miner_model, total_curtailed_energy, total_bitcoin_mined)
          SELECT 
            $1 AS year,
            farm_id,
            miner_model,
            SUM(CAST(total_curtailed_energy AS DECIMAL))::TEXT AS total_curtailed_energy,
            SUM(CAST(total_bitcoin_mined AS DECIMAL))::TEXT AS total_bitcoin_mined
          FROM 
            daily_mining_potential
          WHERE 
            EXTRACT(YEAR FROM summary_date) = $1::INTEGER
            AND miner_model = $2
          GROUP BY 
            farm_id, miner_model
          ON CONFLICT (year, farm_id, miner_model)
          DO UPDATE SET
            total_curtailed_energy = EXCLUDED.total_curtailed_energy,
            total_bitcoin_mined = EXCLUDED.total_bitcoin_mined,
            updated_at = NOW()
        `, [year, minerModel]);
      }
    }
    
    console.log('Materialized view population completed successfully!');
  } catch (error) {
    console.error('Error populating materialized views:', error);
    throw error;
  } finally {
    client.release();
  }
}

// Execute the main function
main().catch(console.error);
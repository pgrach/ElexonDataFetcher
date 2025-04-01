/**
 * Direct Database Verification for Critical Dates
 * 
 * This script directly queries the database using the pg client to verify
 * the completeness and accuracy of data for March 28 and 29, 2025.
 */

import pg from 'pg';
import dotenv from 'dotenv';

dotenv.config();

// Create a PostgreSQL client
const client = new pg.Client({
  connectionString: process.env.DATABASE_URL
});

async function connectToDatabase() {
  try {
    await client.connect();
    console.log('Connected to database successfully');
  } catch (error) {
    console.error('Failed to connect to database:', error);
    process.exit(1);
  }
}

async function verifyCurtailmentRecords(date: string) {
  console.log(`\n=== Curtailment Records for ${date} ===`);
  
  try {
    // Count total records for the date
    const totalRecordsResult = await client.query(`
      SELECT COUNT(*) as count
      FROM curtailment_records
      WHERE settlement_date = $1
    `, [date]);
    
    // Count unique periods for the date
    const periodsResult = await client.query(`
      SELECT COUNT(DISTINCT settlement_period) as period_count
      FROM curtailment_records
      WHERE settlement_date = $1
    `, [date]);
    
    // Count unique farm_ids for the date
    const farmsResult = await client.query(`
      SELECT COUNT(DISTINCT farm_id) as farm_count
      FROM curtailment_records
      WHERE settlement_date = $1
    `, [date]);
    
    // Sum volumes and payments
    const volumeAndPayment = await client.query(`
      SELECT 
        SUM(volume) as total_volume,
        SUM(payment) as total_payment
      FROM curtailment_records
      WHERE settlement_date = $1
    `, [date]);
    
    // Get period coverage
    const periodsWithData = await client.query(`
      SELECT settlement_period
      FROM curtailment_records
      WHERE settlement_date = $1
      GROUP BY settlement_period
      ORDER BY settlement_period
    `, [date]);
    
    console.log(`Records: ${totalRecordsResult.rows[0].count}`);
    console.log(`Periods: ${periodsResult.rows[0].period_count} of 48`);
    console.log(`Farms: ${farmsResult.rows[0].farm_count}`);
    console.log(`Total Volume: ${parseFloat(volumeAndPayment.rows[0].total_volume).toFixed(2)} MWh`);
    console.log(`Total Payment: £${parseFloat(volumeAndPayment.rows[0].total_payment).toFixed(2)}`);
    
    // Generate a string of present periods to allow for visual inspection
    const presentPeriods = periodsWithData.rows.map(row => Number(row.settlement_period));
    
    if (presentPeriods.length === 48) {
      console.log('All 48 periods are present.');
    } else {
      const missingPeriods = Array.from({length: 48}, (_, i) => i + 1)
        .filter(p => !presentPeriods.includes(p));
      
      console.log(`Missing ${48 - presentPeriods.length} periods: ${missingPeriods.join(', ')}`);
    }
  } catch (error) {
    console.error(`Error verifying curtailment records for ${date}:`, error);
  }
}

async function verifyBitcoinCalculations(date: string) {
  console.log(`\n=== Bitcoin Calculations for ${date} ===`);
  
  // Miner models to check
  const minerModels = ['S19J_PRO', 'S9', 'M20S'];
  
  for (const model of minerModels) {
    try {
      // Get the count of Bitcoin calculations for the date and model
      const countResult = await client.query(`
        SELECT COUNT(*) as count
        FROM historical_bitcoin_calculations
        WHERE settlement_date = $1 AND miner_model = $2
      `, [date, model]);
      
      // Count unique periods for the date and model
      const periodsResult = await client.query(`
        SELECT COUNT(DISTINCT settlement_period) as period_count
        FROM historical_bitcoin_calculations
        WHERE settlement_date = $1 AND miner_model = $2
      `, [date, model]);
      
      // Sum up the mining potential
      const potentialResult = await client.query(`
        SELECT 
          SUM(bitcoin_mined) as total_btc
        FROM historical_bitcoin_calculations
        WHERE settlement_date = $1 AND miner_model = $2
      `, [date, model]);
      
      // Check the records per farm count
      const farmsResult = await client.query(`
        SELECT COUNT(DISTINCT farm_id) as farm_count
        FROM historical_bitcoin_calculations
        WHERE settlement_date = $1 AND miner_model = $2
      `, [date, model]);
      
      console.log(`\n${model}`);
      console.log(`Calculations: ${countResult.rows[0].count}`);
      console.log(`Periods covered: ${periodsResult.rows[0].period_count} of 48`);
      console.log(`Farms covered: ${farmsResult.rows[0].farm_count}`);
      console.log(`Total Bitcoin potential: ${parseFloat(potentialResult.rows[0].total_btc || '0').toFixed(8)} BTC`);
      
      // Calculate expected number of records (one per farm per period)
      const expectedUniqueCount = await client.query(`
        SELECT COUNT(DISTINCT (farm_id, settlement_period)) as count
        FROM curtailment_records
        WHERE settlement_date = $1
      `, [date]);
      
      console.log(`Expected unique calculations (farm/period): ${expectedUniqueCount.rows[0].count}`);
      console.log(`Total curtailment records: ${(await client.query(`
        SELECT COUNT(*) as count
        FROM curtailment_records
        WHERE settlement_date = $1
      `, [date])).rows[0].count}`);
      
      // Determine if the reconciliation is complete
      const expectedRecords = parseInt(expectedUniqueCount.rows[0].count);
      const actualRecords = parseInt(countResult.rows[0].count);
      
      if (actualRecords >= expectedRecords) {
        console.log('✅ Reconciliation complete');
      } else {
        console.log(`❌ Missing ${expectedRecords - actualRecords} calculations`);
      }
    } catch (error) {
      console.error(`Error verifying Bitcoin calculations for ${date} with model ${model}:`, error);
    }
  }
}

async function verifySummaries() {
  console.log('\n=== Summary Tables Verification ===');
  
  try {
    // Check the daily summaries for our critical dates
    const dailySummaries = await client.query(`
      SELECT 
        summary_date,
        total_curtailed_energy,
        total_payment
      FROM daily_summaries
      WHERE summary_date IN ('2025-03-28', '2025-03-29')
      ORDER BY summary_date
    `);
    
    console.log('\nDaily Summaries:');
    if (dailySummaries.rows.length > 0) {
      for (const summary of dailySummaries.rows) {
        console.log(`${summary.summary_date}: ${parseFloat(summary.total_curtailed_energy).toFixed(2)} MWh, £${Math.abs(parseFloat(summary.total_payment)).toFixed(2)}`);
      }
    } else {
      console.log('No daily summaries found');
    }
    
    // Check the March 2025 monthly summary
    const monthlySummary = await client.query(`
      SELECT 
        year_month,
        total_curtailed_energy,
        total_payment
      FROM monthly_summaries
      WHERE year_month = '2025-03'
    `);
    
    console.log('\nMonthly Summary:');
    if (monthlySummary.rows.length > 0) {
      console.log(`${monthlySummary.rows[0].year_month}: ${parseFloat(monthlySummary.rows[0].total_curtailed_energy).toFixed(2)} MWh, £${Math.abs(parseFloat(monthlySummary.rows[0].total_payment)).toFixed(2)}`);
    } else {
      console.log('No monthly summary found for 2025-03');
    }
    
    // Check the 2025 yearly summary
    const yearlySummary = await client.query(`
      SELECT 
        year,
        total_curtailed_energy,
        total_payment
      FROM yearly_summaries
      WHERE year = '2025'
    `);
    
    console.log('\nYearly Summary:');
    if (yearlySummary.rows.length > 0) {
      console.log(`${yearlySummary.rows[0].year}: ${parseFloat(yearlySummary.rows[0].total_curtailed_energy).toFixed(2)} MWh, £${Math.abs(parseFloat(yearlySummary.rows[0].total_payment)).toFixed(2)}`);
    } else {
      console.log('No yearly summary found for 2025');
    }
  } catch (error) {
    console.error('Error verifying summaries:', error);
  }
}

async function verifyData() {
  console.log('=== Starting Data Verification ===');
  
  await connectToDatabase();
  
  // Check March 28, 2025
  await verifyCurtailmentRecords('2025-03-28');
  await verifyBitcoinCalculations('2025-03-28');
  
  // Check March 29, 2025
  await verifyCurtailmentRecords('2025-03-29');
  await verifyBitcoinCalculations('2025-03-29');
  
  // Check summary tables
  await verifySummaries();
  
  // Close the database connection
  await client.end();
  
  console.log('\n=== Verification Complete ===');
}

// Run the verification
verifyData()
  .then(() => {
    console.log('Verification completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Verification failed with error:', error);
    process.exit(1);
  });
/**
 * Verification script for 2025-03-27 data
 * Generates a comprehensive summary of all 48 periods
 */

const { Pool } = require('pg');

// Database connection
const dbPool = new Pool({
  connectionString: process.env.DATABASE_URL
});

/**
 * Generate a detailed summary of records for 2025-03-27
 */
async function verifyCurtailmentRecords() {
  const SETTLEMENT_DATE = '2025-03-27';
  let client;
  
  try {
    console.log(`\n=== Verifying curtailment data for ${SETTLEMENT_DATE} ===`);
    
    // Get a client from the pool
    client = await dbPool.connect();
    
    // Get overall statistics
    const overallResult = await client.query(`
      SELECT 
        COUNT(*) as total_count,
        COUNT(DISTINCT settlement_period) as distinct_periods,
        COUNT(DISTINCT farm_id) as distinct_farms,
        SUM(ABS(volume::numeric)) as total_volume,
        SUM(payment::numeric) as total_payment
      FROM curtailment_records
      WHERE settlement_date = $1
    `, [SETTLEMENT_DATE]);
    
    const row = overallResult.rows[0];
    
    console.log(`===== Overall Statistics =====`);
    console.log(`Total records: ${row.total_count}`);
    console.log(`Distinct periods: ${row.distinct_periods} of 48`);
    console.log(`Distinct wind farms: ${row.distinct_farms}`);
    console.log(`Total volume: ${parseFloat(row.total_volume).toLocaleString()} MWh`);
    console.log(`Total payment: £${parseFloat(row.total_payment).toLocaleString()}`);
    
    // Check for any missing periods
    const periodResult = await client.query(`
      SELECT 
        settlement_period,
        COUNT(*) as record_count,
        SUM(ABS(volume::numeric)) as period_volume,
        SUM(payment::numeric) as period_payment
      FROM curtailment_records
      WHERE settlement_date = $1
      GROUP BY settlement_period
      ORDER BY settlement_period
    `, [SETTLEMENT_DATE]);
    
    console.log(`\n===== Period Coverage =====`);
    
    // Check for missing periods
    const existingPeriods = periodResult.rows.map(row => parseInt(row.settlement_period));
    const allPeriods = Array.from({ length: 48 }, (_, i) => i + 1);
    const missingPeriods = allPeriods.filter(p => !existingPeriods.includes(p));
    
    if (missingPeriods.length > 0) {
      console.log(`Missing periods: ${missingPeriods.join(', ')}`);
    } else {
      console.log(`All 48 periods are present ✅`);
    }
    
    // Display period details in a table format
    console.log('\n===== Period Details =====');
    console.log('Period | Records | Volume (MWh) | Payment (£)');
    console.log('-------|---------|--------------|------------');
    
    for (const row of periodResult.rows) {
      const period = row.settlement_period.toString().padStart(2, ' ');
      const count = row.record_count.toString().padStart(7, ' ');
      const volume = parseFloat(row.period_volume).toFixed(2).padStart(12, ' ');
      const payment = parseFloat(row.period_payment).toFixed(2).padStart(12, ' ');
      
      console.log(`${period}   | ${count} | ${volume} | ${payment}`);
    }
    
    // List the top farms by volume
    const farmResult = await client.query(`
      SELECT 
        farm_id,
        lead_party_name,
        COUNT(*) as period_count,
        SUM(ABS(volume::numeric)) as farm_volume,
        SUM(payment::numeric) as farm_payment
      FROM curtailment_records
      WHERE settlement_date = $1
      GROUP BY farm_id, lead_party_name
      ORDER BY farm_volume DESC
      LIMIT 10
    `, [SETTLEMENT_DATE]);
    
    console.log('\n===== Top 10 Farms by Volume =====');
    console.log('Farm ID     | Lead Party               | Periods | Volume (MWh) | Payment (£)');
    console.log('------------|--------------------------|---------|--------------|------------');
    
    for (const row of farmResult.rows) {
      const farmId = row.farm_id.padEnd(11, ' ');
      const party = (row.lead_party_name || 'Unknown').padEnd(26, ' ');
      const periods = row.period_count.toString().padStart(7, ' ');
      const volume = parseFloat(row.farm_volume).toFixed(2).padStart(12, ' ');
      const payment = parseFloat(row.farm_payment).toFixed(2).padStart(12, ' ');
      
      console.log(`${farmId}| ${party}| ${periods} | ${volume} | ${payment}`);
    }
    
    console.log(`\n===== Verification complete! =====`);
    
    if (parseInt(row.distinct_periods) === 48) {
      console.log(`✅ All 48 periods successfully populated for ${SETTLEMENT_DATE}`);
      
      // Extract specific information about the newly added periods 35-48
      const newlyAddedResult = await client.query(`
        SELECT 
          COUNT(*) as record_count,
          SUM(ABS(volume::numeric)) as total_volume,
          SUM(payment::numeric) as total_payment
        FROM curtailment_records
        WHERE settlement_date = $1 AND settlement_period BETWEEN 35 AND 48
      `, [SETTLEMENT_DATE]);
      
      const newlyAddedRow = newlyAddedResult.rows[0];
      
      console.log(`\n===== Summary of Newly Added Periods (35-48) =====`);
      console.log(`Records added: ${newlyAddedRow.record_count}`);
      console.log(`Total volume: ${parseFloat(newlyAddedRow.total_volume).toLocaleString()} MWh`);
      console.log(`Total payment: £${parseFloat(newlyAddedRow.total_payment).toLocaleString()}`);
      console.log(`Average records per period: ${(parseInt(newlyAddedRow.record_count) / 14).toFixed(1)}`);
    } else {
      console.log(`❌ Data incomplete: Only ${row.distinct_periods} of 48 periods populated`);
    }
    
  } catch (error) {
    console.error('Error verifying data:', error);
  } finally {
    if (client) {
      client.release();
    }
    
    // Close the pool to exit the script
    await dbPool.end();
  }
}

// Run the verification
verifyCurtailmentRecords();
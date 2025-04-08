import { db } from './db';
import { sql } from 'drizzle-orm';

async function queryMarchBitcoin() {
  try {
    // First, let's check the exact SQL query with logging
    const sqlQuery = `
      SELECT SUM(bitcoin_mined::NUMERIC) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date >= '2025-03-01'
        AND settlement_date <= '2025-03-31'
        AND miner_model = 'S19J_PRO'
    `;
    
    console.log('Executing SQL query:');
    console.log(sqlQuery);
    
    // Execute the query using raw SQL to debug
    const result = await db.execute(sql.raw(sqlQuery));
    
    console.log('Query result:', result);
    
    if (result.length > 0) {
      console.log('Total Bitcoin:', result[0].total_bitcoin);
    } else {
      console.log('No results returned');
    }
    
    // Let's also check how many records exist in the date range
    const countQuery = `
      SELECT COUNT(*) as record_count
      FROM historical_bitcoin_calculations
      WHERE settlement_date >= '2025-03-01'
        AND settlement_date <= '2025-03-31'
        AND miner_model = 'S19J_PRO'
    `;
    
    console.log('\nChecking record count:');
    console.log(countQuery);
    
    const countResult = await db.execute(sql.raw(countQuery));
    console.log('Record count result:', countResult);
    
    if (countResult.length > 0) {
      console.log('Total records:', countResult[0].record_count);
    } else {
      console.log('No count results returned');
    }
    
    // Also check records for a specific day
    const sampleDayQuery = `
      SELECT settlement_date, settlement_period, bitcoin_mined, difficulty
      FROM historical_bitcoin_calculations
      WHERE settlement_date = '2025-03-31'
        AND miner_model = 'S19J_PRO'
      LIMIT 5
    `;
    
    console.log('\nChecking sample records for 2025-03-31:');
    
    const sampleResult = await db.execute(sql.raw(sampleDayQuery));
    console.log('Sample records:', sampleResult);
    
  } catch (error) {
    console.error('Error querying March 2025 Bitcoin data:', error);
  }
}

queryMarchBitcoin();
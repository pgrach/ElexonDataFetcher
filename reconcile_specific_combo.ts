/**
 * Reconcile a Specific Farm/Model Combination
 * 
 * This focused script processes a single farm and miner model combination
 * to provide detailed debugging and ensure careful insertion.
 */

import pg from 'pg';
import { db } from './db';
import { eq, and } from 'drizzle-orm';
import { curtailmentRecords, historicalBitcoinCalculations } from './db/schema';
import { getDifficultyData } from './server/services/dynamodbService';

// Configuration
const TARGET_DATE = process.argv[2] || '2023-12-21';
const TARGET_PERIOD = parseInt(process.argv[3] || '7');
const TARGET_FARM = process.argv[4] || 'E_BOWBE-1';
const TARGET_MODEL = process.argv[5] || 'S19J_PRO';

// Database connection
const dbUrl = process.env.DATABASE_URL;
if (!dbUrl) {
  console.error('DATABASE_URL environment variable is not set');
  process.exit(1);
}

const pool = new pg.Pool({
  connectionString: dbUrl,
  max: 3,
});

async function reconcileSpecificCombo() {
  const client = await pool.connect();
  
  try {
    console.log(`\n=== Processing ${TARGET_DATE} Period ${TARGET_PERIOD} Farm ${TARGET_FARM} Model ${TARGET_MODEL} ===\n`);
    
    // Check if the combination already exists
    const existingQuery = `
      SELECT id
      FROM historical_bitcoin_calculations
      WHERE settlement_date = $1
        AND settlement_period = $2
        AND farm_id = $3
        AND miner_model = $4
    `;
    
    const existingResult = await client.query(existingQuery, [
      TARGET_DATE, 
      TARGET_PERIOD, 
      TARGET_FARM,
      TARGET_MODEL
    ]);
    
    if (existingResult.rows.length > 0) {
      console.log('Record already exists');
      return;
    }
    
    // Check if the curtailment record exists
    const curtailmentQuery = `
      SELECT volume, payment, lead_party_name
      FROM curtailment_records
      WHERE settlement_date = $1
        AND settlement_period = $2
        AND farm_id = $3
      LIMIT 1
    `;
    
    const curtailmentResult = await client.query(curtailmentQuery, [
      TARGET_DATE, 
      TARGET_PERIOD, 
      TARGET_FARM
    ]);
    
    if (curtailmentResult.rows.length === 0) {
      console.log('No curtailment record found');
      return;
    }
    
    const { volume, payment, lead_party_name } = curtailmentResult.rows[0];
    console.log('Curtailment record found:');
    console.log({ volume, payment, lead_party_name });
    
    const volumeMWh = Math.abs(Number(volume));
    if (volumeMWh < 0.01) {
      console.log('Zero volume, skipping...');
      return;
    }
    
    // Get difficulty
    const difficulty = await getDifficultyData(TARGET_DATE);
    console.log(`Using difficulty: ${difficulty}`);
    
    // Insert the record - note: actual schema doesn't have curtailed_energy or lead_party_name
    const insertQuery = `
      INSERT INTO historical_bitcoin_calculations 
        (settlement_date, settlement_period, farm_id, miner_model, 
         difficulty, bitcoin_mined, calculated_at)
      VALUES 
        ($1, $2, $3, $4, $5, $6, $7)
      RETURNING id;
    `;
    
    const insertResult = await client.query(insertQuery, [
      TARGET_DATE, 
      TARGET_PERIOD, 
      TARGET_FARM, 
      TARGET_MODEL, 
      difficulty.toString(), 
      "0", // Placeholder, will be updated in batch
      new Date()
    ]);
    
    if (insertResult && insertResult.rowCount && insertResult.rowCount > 0) {
      console.log(`Successfully inserted record with ID: ${insertResult.rows[0].id}`);
    } else {
      console.log('Failed to insert record');
    }
    
    // Verify the insertion
    const verifyQuery = `
      SELECT id, settlement_date, settlement_period, farm_id, miner_model, difficulty, bitcoin_mined
      FROM historical_bitcoin_calculations
      WHERE settlement_date = $1
        AND settlement_period = $2
        AND farm_id = $3
        AND miner_model = $4
    `;
    
    const verifyResult = await client.query(verifyQuery, [
      TARGET_DATE, 
      TARGET_PERIOD, 
      TARGET_FARM,
      TARGET_MODEL
    ]);
    
    if (verifyResult.rows.length > 0) {
      console.log('\nVerification successful:');
      console.log(verifyResult.rows[0]);
    } else {
      console.log('\nVerification failed: Record not found after insertion');
    }
    
  } catch (error) {
    console.error('Error:', error);
  } finally {
    client.release();
    await pool.end();
  }
}

// Run the script
reconcileSpecificCombo()
  .then(() => {
    console.log('\nDone');
    process.exit(0);
  })
  .catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
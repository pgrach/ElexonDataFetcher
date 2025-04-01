/**
 * Complete Missing Bitcoin Calculations
 * 
 * This script ensures that all curtailment records have corresponding Bitcoin calculations
 * for critical dates (March 28-29, 2025) by identifying missing records and adding them.
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

/**
 * Gets the current Bitcoin mining difficulty
 */
async function getBitcoinDifficulty(): Promise<number> {
  try {
    const result = await client.query(`
      SELECT difficulty
      FROM bitcoin_difficulty
      ORDER BY created_at DESC
      LIMIT 1
    `);
    
    if (result.rows.length > 0) {
      return parseFloat(result.rows[0].difficulty);
    }
    
    // Default value if no records found
    return 113757508810853;
  } catch (error) {
    console.error('Error fetching Bitcoin difficulty:', error);
    return 113757508810853;
  }
}

/**
 * Calculate Bitcoin mined based on MWh and current difficulty
 */
function calculateBitcoinMined(volume: number, difficulty: number, minerModel: string): number {
  // 2022 efficiency factors (updated for different miner models)
  const efficiencyFactor = minerModel === 'S19J_PRO' 
    ? 0.000000000755 
    : minerModel === 'S9'
      ? 0.000000000235
      : 0.000000000466; // M20S
  
  return Math.abs(volume) * efficiencyFactor * (56642000000000 / difficulty);
}

/**
 * Find missing calculations for a given date and miner model
 */
async function findMissingCalculations(date: string, minerModel: string): Promise<Array<{
  settlement_date: string;
  settlement_period: number;
  farm_id: string;
  volume: number;
}>> {
  try {
    const result = await client.query(`
      SELECT c.settlement_date, c.settlement_period, c.farm_id, c.volume
      FROM curtailment_records c
      LEFT JOIN historical_bitcoin_calculations b 
        ON c.settlement_date = b.settlement_date 
        AND c.settlement_period = b.settlement_period 
        AND c.farm_id = b.farm_id
        AND b.miner_model = $1
      WHERE c.settlement_date = $2
        AND b.id IS NULL
    `, [minerModel, date]);
    
    return result.rows;
  } catch (error) {
    console.error(`Error finding missing calculations for ${date} with model ${minerModel}:`, error);
    return [];
  }
}

/**
 * Insert missing calculations
 */
async function insertMissingCalculations(
  missingRecords: Array<{
    settlement_date: string;
    settlement_period: number;
    farm_id: string;
    volume: number;
  }>, 
  minerModel: string,
  difficulty: number
): Promise<number> {
  if (missingRecords.length === 0) {
    console.log(`No missing calculations to insert for ${minerModel}`);
    return 0;
  }
  
  let insertedCount = 0;
  
  try {
    // Use a transaction for bulk inserts
    await client.query('BEGIN');
    
    for (const record of missingRecords) {
      const bitcoinMined = calculateBitcoinMined(
        Math.abs(record.volume), 
        difficulty, 
        minerModel
      );
      
      await client.query(`
        INSERT INTO historical_bitcoin_calculations (
          settlement_date, 
          settlement_period, 
          farm_id, 
          miner_model, 
          bitcoin_mined, 
          difficulty,
          calculated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
      `, [
        record.settlement_date,
        record.settlement_period,
        record.farm_id,
        minerModel,
        bitcoinMined,
        difficulty
      ]);
      
      insertedCount++;
    }
    
    await client.query('COMMIT');
    
    console.log(`Successfully inserted ${insertedCount} Bitcoin calculations for ${minerModel}`);
    return insertedCount;
  } catch (error) {
    await client.query('ROLLBACK');
    console.error(`Error inserting calculations for ${minerModel}:`, error);
    return 0;
  }
}

/**
 * Update Bitcoin summaries after adding calculations
 */
async function updateBitcoinSummaries(date: string) {
  const yearMonth = date.substring(0, 7); // YYYY-MM
  const year = date.substring(0, 4);      // YYYY
  
  try {
    // Update Monthly Bitcoin Summary
    console.log(`Updating monthly Bitcoin summary for ${yearMonth}...`);
    
    for (const minerModel of ['S19J_PRO', 'S9', 'M20S']) {
      console.log(`Calculating monthly Bitcoin summary for ${yearMonth} with ${minerModel}`);
      
      const monthlyResult = await client.query(`
        SELECT SUM(bitcoin_mined) as total_btc
        FROM historical_bitcoin_calculations
        WHERE settlement_date LIKE $1 || '%'
          AND miner_model = $2
      `, [yearMonth, minerModel]);
      
      const monthlyBitcoinTotal = parseFloat(monthlyResult.rows[0].total_btc || '0');
      
      // Upsert the monthly summary
      await client.query(`
        INSERT INTO monthly_bitcoin_summaries (
          year_month, 
          miner_model, 
          total_btc_mined, 
          created_at, 
          updated_at
        )
        VALUES ($1, $2, $3, NOW(), NOW())
        ON CONFLICT (year_month, miner_model) 
        DO UPDATE SET 
          total_btc_mined = $3,
          updated_at = NOW()
      `, [yearMonth, minerModel, monthlyBitcoinTotal]);
      
      console.log(`Updated monthly summary for ${yearMonth}: ${monthlyBitcoinTotal.toFixed(8)} BTC`);
    }
    
    // Update Yearly Bitcoin Summary
    console.log(`Updating yearly Bitcoin summary for ${year}...`);
    
    console.log('=== Manual Yearly Bitcoin Summary Update ===');
    console.log(`Updating summaries for ${year}`);
    
    for (const minerModel of ['S19J_PRO', 'S9', 'M20S']) {
      console.log(`- Processing ${minerModel}`);
      console.log(`Calculating yearly Bitcoin summary for ${year} with ${minerModel}`);
      
      // Find all monthly summaries for this year and model
      const monthlySummaries = await client.query(`
        SELECT year_month, total_btc_mined 
        FROM monthly_bitcoin_summaries
        WHERE year_month LIKE $1 || '%'
          AND miner_model = $2
      `, [year, minerModel]);
      
      console.log(`Found ${monthlySummaries.rows.length} monthly summaries for ${year}`);
      
      // Sum up the monthly totals
      let yearlyTotal = 0;
      for (const monthly of monthlySummaries.rows) {
        yearlyTotal += parseFloat(monthly.total_btc_mined);
      }
      
      // Upsert the yearly summary
      await client.query(`
        INSERT INTO yearly_bitcoin_summaries (
          year, 
          miner_model, 
          total_btc_mined, 
          created_at, 
          updated_at
        )
        VALUES ($1, $2, $3, NOW(), NOW())
        ON CONFLICT (year, miner_model) 
        DO UPDATE SET 
          total_btc_mined = $3,
          updated_at = NOW()
      `, [year, minerModel, yearlyTotal]);
      
      console.log(`Updated yearly summary for ${year}: ${yearlyTotal.toFixed(8)} BTC with ${minerModel}`);
    }
    
    // Print verification data
    const verificationResults = await client.query(`
      SELECT miner_model, total_btc_mined
      FROM yearly_bitcoin_summaries
      WHERE year = $1
      ORDER BY miner_model
    `, [year]);
    
    console.log(`Verification Results for ${year}:`);
    for (const result of verificationResults.rows) {
      console.log(`- ${result.miner_model}: ${parseFloat(result.total_btc_mined).toFixed(8)} BTC`);
    }
    
    console.log('=== Yearly Summary Update Complete ===');
  } catch (error) {
    console.error('Error updating Bitcoin summaries:', error);
  }
}

/**
 * Process missing calculations for a specific date
 */
async function processMissingCalculations(date: string): Promise<void> {
  console.log(`\n=== Processing missing calculations for ${date} ===`);
  
  // Get current Bitcoin difficulty
  const difficulty = await getBitcoinDifficulty();
  console.log(`Using Bitcoin difficulty: ${difficulty}`);
  
  const minerModels = ['S19J_PRO', 'S9', 'M20S'];
  let totalInserted = 0;
  
  for (const model of minerModels) {
    console.log(`\nChecking for missing ${model} calculations...`);
    
    // Find missing calculations
    const missingCalculations = await findMissingCalculations(date, model);
    console.log(`Found ${missingCalculations.length} missing calculations for ${model}`);
    
    if (missingCalculations.length > 0) {
      // Insert missing calculations
      const inserted = await insertMissingCalculations(missingCalculations, model, difficulty);
      totalInserted += inserted;
    }
  }
  
  if (totalInserted > 0) {
    console.log(`\nTotal of ${totalInserted} calculations inserted for ${date}`);
    
    // Update Bitcoin summaries
    await updateBitcoinSummaries(date);
  } else {
    console.log(`\nNo missing calculations found for ${date}`);
  }
}

async function main() {
  console.log('=== Starting Missing Calculations Completion ===');
  
  await connectToDatabase();
  
  // Process March 28, 2025
  await processMissingCalculations('2025-03-28');
  
  // Process March 29, 2025
  await processMissingCalculations('2025-03-29');
  
  // Close the database connection
  await client.end();
  
  console.log('\n=== Missing Calculations Process Complete ===');
}

// Run the main function
main()
  .then(() => {
    console.log('Process completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Process failed with error:', error);
    process.exit(1);
  });
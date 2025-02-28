/**
 * Batch-Limited Reconciliation Tool
 * 
 * This script processes a limited number of missing bitcoin calculations per run,
 * making it suitable for environments with execution time limits.
 */

import pg from 'pg';
import { DynamoDBClient, DescribeTableCommand, ScanCommand } from '@aws-sdk/client-dynamodb';
import { DEFAULT_DIFFICULTY } from './server/types/bitcoin';

const { Pool } = pg;
type PoolClient = pg.PoolClient;

// Connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

// DynamoDB client
const dynamoClient = new DynamoDBClient({
  region: 'eu-north-1' // This should match your DynamoDB region
});

// Table name for difficulty data
const DIFFICULTY_TABLE = 'asics-dynamodb-DifficultyTable-DQ308ID3POT6';

// Sleep utility
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Define a combination type
interface Combination {
  date: string;
  period: number;
  farmId: string;
  minerModel: string;
}

/**
 * Get difficulty data for a specific date from DynamoDB
 */
async function getDifficultyData(date: string): Promise<number> {
  try {
    console.log(`[DynamoDB] Fetching difficulty for date: ${date}`);
    
    // First check if the table exists and is active
    const describeCommand = new DescribeTableCommand({
      TableName: DIFFICULTY_TABLE
    });
    
    try {
      const tableInfo = await dynamoClient.send(describeCommand);
      console.log(`[DynamoDB] Table ${DIFFICULTY_TABLE} status:`, {
        status: tableInfo.Table?.TableStatus,
        itemCount: tableInfo.Table?.ItemCount,
        keySchema: tableInfo.Table?.KeySchema
      });
    } catch (e) {
      console.warn(`[DynamoDB] Table description failed:`, e);
      console.warn(`[DynamoDB] Using default difficulty: ${DEFAULT_DIFFICULTY}`);
      return DEFAULT_DIFFICULTY;
    }
    
    // Scan for the specific date
    console.log(`[DynamoDB] Executing difficulty scan:`, {
      table: DIFFICULTY_TABLE,
      date: date,
      command: 'ScanCommand'
    });
    
    const scanCommand = new ScanCommand({
      TableName: DIFFICULTY_TABLE,
      FilterExpression: '#date = :date',
      ExpressionAttributeNames: {
        '#date': 'Date'
      },
      ExpressionAttributeValues: {
        ':date': { S: date }
      }
    });
    
    const response = await dynamoClient.send(scanCommand);
    
    if (response.Items && response.Items.length > 0) {
      const difficultyRecord = response.Items[0];
      const difficultyValue = difficultyRecord.Difficulty?.N || DEFAULT_DIFFICULTY.toString();
      
      console.log(`[DynamoDB] Found historical difficulty for ${date}:`, {
        difficulty: difficultyValue,
        id: difficultyRecord.ID?.S,
        totalRecords: response.Items.length
      });
      
      return parseInt(difficultyValue.replace(/,/g, ''));
    } else {
      console.warn(`[DynamoDB] No difficulty found for date ${date}, using default: ${DEFAULT_DIFFICULTY}`);
      return DEFAULT_DIFFICULTY;
    }
  } catch (error) {
    console.error(`[DynamoDB] Error fetching difficulty for ${date}:`, error);
    console.warn(`[DynamoDB] Using default difficulty: ${DEFAULT_DIFFICULTY}`);
    return DEFAULT_DIFFICULTY;
  }
}

/**
 * Get missing combinations for a specific date
 */
async function getMissingCombinations(date: string, limit: number = 10): Promise<Combination[]> {
  const client = await pool.connect();
  try {
    // This query finds specific date/period/farm/model combinations that are missing
    const query = `
      WITH curtailment_by_period AS (
        SELECT 
          cr.settlement_date,
          cr.settlement_period,
          cr.farm_id
        FROM 
          curtailment_records cr
        WHERE 
          cr.settlement_date = $1
      )
      
      SELECT 
        cp.settlement_date as date,
        cp.settlement_period as period,
        cp.farm_id as farm_id,
        'M20S' as miner_model
      FROM 
        curtailment_by_period cp
      WHERE 
        NOT EXISTS (
          SELECT 1 
          FROM historical_bitcoin_calculations hbc
          WHERE 
            hbc.settlement_date = cp.settlement_date
            AND hbc.settlement_period = cp.settlement_period
            AND hbc.farm_id = cp.farm_id
            AND hbc.miner_model = 'M20S'
        )
      ORDER BY cp.settlement_period, cp.farm_id
      LIMIT $2;
    `;
    
    const result = await client.query(query, [date, limit]);
    
    return result.rows.map(row => ({
      date: row.date as string,
      period: row.period as number,
      farmId: row.farm_id as string,
      minerModel: row.miner_model as string
    }));
    
  } catch (error) {
    console.error('Error getting missing combinations:', error);
    return [];
  } finally {
    client.release();
  }
}

/**
 * Get curtailment record for a specific combination
 */
async function getCurtailmentRecord(client: PoolClient, date: string, period: number, farmId: string) {
  const query = `
    SELECT volume, payment, lead_party_name
    FROM curtailment_records
    WHERE settlement_date = $1
      AND settlement_period = $2
      AND farm_id = $3
  `;
  
  const result = await client.query(query, [date, period, farmId]);
  return result.rows.length > 0 ? result.rows[0] : null;
}

/**
 * Process a specific combination
 */
async function processCombination(combo: Combination): Promise<boolean> {
  const client = await pool.connect();
  try {
    console.log(`\n=== Processing ${combo.date} Period ${combo.period} Farm ${combo.farmId} Model ${combo.minerModel} ===\n`);
    
    // Get curtailment record
    const curtailmentRecord = await getCurtailmentRecord(client, combo.date, combo.period, combo.farmId);
    
    if (!curtailmentRecord) {
      console.log(`No curtailment record found for ${combo.date} Period ${combo.period} Farm ${combo.farmId}`);
      return false;
    }
    
    console.log('Curtailment record found:');
    console.log(curtailmentRecord);
    
    // Check if record already exists to avoid duplicates
    const existsQuery = `
      SELECT id FROM historical_bitcoin_calculations
      WHERE settlement_date = $1
        AND settlement_period = $2
        AND farm_id = $3
        AND miner_model = $4
    `;
    
    const existsResult = await client.query(existsQuery, [
      combo.date, 
      combo.period, 
      combo.farmId, 
      combo.minerModel
    ]);
    
    if (existsResult.rows.length > 0) {
      console.log(`Record already exists for this combination with ID: ${existsResult.rows[0].id}`);
      return true;
    }
    
    // Get difficulty
    const difficulty = await getDifficultyData(combo.date);
    console.log(`Using difficulty: ${difficulty}`);
    
    // Insert the record
    const insertQuery = `
      INSERT INTO historical_bitcoin_calculations 
        (settlement_date, settlement_period, farm_id, miner_model, 
         difficulty, bitcoin_mined, calculated_at)
      VALUES 
        ($1, $2, $3, $4, $5, $6, $7)
      RETURNING id;
    `;
    
    const insertResult = await client.query(insertQuery, [
      combo.date, 
      combo.period, 
      combo.farmId, 
      combo.minerModel, 
      difficulty.toString(), 
      "0", // Bitcoin mined placeholder - will be updated later
      new Date()
    ]);
    
    if (insertResult && insertResult.rowCount && insertResult.rowCount > 0) {
      console.log(`Successfully inserted record with ID: ${insertResult.rows[0].id}`);
      return true;
    } else {
      console.log('Failed to insert record');
      return false;
    }
  } catch (error) {
    console.error('Error processing combination:', error);
    return false;
  } finally {
    client.release();
  }
}

/**
 * Process a batch of combinations
 */
async function processBatch(combinations: Combination[]): Promise<{ success: number, failure: number }> {
  let successCount = 0;
  let failureCount = 0;
  
  for (const combo of combinations) {
    // Add slight delay to avoid overwhelming the database
    await sleep(100);
    
    const success = await processCombination(combo);
    if (success) {
      successCount++;
    } else {
      failureCount++;
    }
  }
  
  return { success: successCount, failure: failureCount };
}

/**
 * Main reconciliation function
 */
async function reconcileBatchForDate(date: string, batchSize: number = 10): Promise<void> {
  console.log(`\n========== Reconciling batch for ${date} (limit: ${batchSize}) ==========\n`);
  
  // Get missing combinations
  const missingCombinations = await getMissingCombinations(date, batchSize);
  
  if (missingCombinations.length === 0) {
    console.log(`No missing combinations found for ${date}`);
    return;
  }
  
  console.log(`Found ${missingCombinations.length} missing combinations for ${date}:`);
  missingCombinations.forEach((combo, index) => {
    console.log(`${index + 1}. Date: ${combo.date}, Period: ${combo.period}, Farm: ${combo.farmId}, Model: ${combo.minerModel}`);
  });
  
  // Process the batch
  const result = await processBatch(missingCombinations);
  
  // Print summary
  console.log(`\n===== Reconciliation Summary for ${date} =====`);
  console.log(`Total combinations: ${missingCombinations.length}`);
  console.log(`Successfully processed: ${result.success}`);
  console.log(`Failed: ${result.failure}`);
}

/**
 * Main function
 */
async function main(): Promise<void> {
  try {
    // Get command line arguments
    const args = process.argv.slice(2);
    
    if (args.length < 1) {
      console.error('Usage: npx tsx reconcile_batch_limit.ts <date> [batch_size]');
      console.error('Example: npx tsx reconcile_batch_limit.ts 2023-12-21 5');
      process.exit(1);
    }
    
    const date = args[0];
    const batchSize = args.length > 1 ? parseInt(args[1]) : 10;
    
    await reconcileBatchForDate(date, batchSize);
    
  } catch (error) {
    console.error('Fatal error:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

// Run the script
main()
  .then(() => {
    console.log('\nBatch reconciliation completed successfully');
    process.exit(0);
  })
  .catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
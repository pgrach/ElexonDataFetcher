/**
 * Date-Period Combination Reconciliation Tool
 * 
 * This script efficiently reconciles specific combinations of dates and periods
 * that are missing calculations, allowing for targeted fixes of problematic data.
 */

import { Pool, PoolClient } from 'pg';
import { DynamoDBClient, DescribeTableCommand, ScanCommand } from '@aws-sdk/client-dynamodb';
import { DEFAULT_DIFFICULTY } from './server/types/bitcoin';

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
 * Get missing combinations for a date and period
 */
async function getMissingCombinations(date: string, period: number): Promise<Array<{farmId: string, minerModel: string}>> {
  const client = await pool.connect();
  try {
    // This query finds farms that have curtailment records but no bitcoin calculations
    // for the specified miner model, date, and period
    const query = `
      WITH farm_curtailment AS (
        SELECT 
          cr.farm_id,
          cr.settlement_date,
          cr.settlement_period
        FROM 
          curtailment_records cr
        WHERE 
          cr.settlement_date = $1
          AND cr.settlement_period = $2
      )
      
      SELECT 
        fc.farm_id,
        'S19J_PRO' as miner_model
      FROM 
        farm_curtailment fc
      WHERE 
        NOT EXISTS (
          SELECT 1 
          FROM historical_bitcoin_calculations hbc
          WHERE 
            hbc.settlement_date = fc.settlement_date
            AND hbc.settlement_period = fc.settlement_period
            AND hbc.farm_id = fc.farm_id
            AND hbc.miner_model = 'S19J_PRO'
        )
      
      UNION ALL
      
      SELECT 
        fc.farm_id,
        'S9' as miner_model
      FROM 
        farm_curtailment fc
      WHERE 
        NOT EXISTS (
          SELECT 1 
          FROM historical_bitcoin_calculations hbc
          WHERE 
            hbc.settlement_date = fc.settlement_date
            AND hbc.settlement_period = fc.settlement_period
            AND hbc.farm_id = fc.farm_id
            AND hbc.miner_model = 'S9'
        )
      
      UNION ALL
      
      SELECT 
        fc.farm_id,
        'M20S' as miner_model
      FROM 
        farm_curtailment fc
      WHERE 
        NOT EXISTS (
          SELECT 1 
          FROM historical_bitcoin_calculations hbc
          WHERE 
            hbc.settlement_date = fc.settlement_date
            AND hbc.settlement_period = fc.settlement_period
            AND hbc.farm_id = fc.farm_id
            AND hbc.miner_model = 'M20S'
        )
      ORDER BY farm_id, miner_model
    `;
    
    const result = await client.query(query, [date, period]);
    return result.rows.map(row => ({
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
async function processCombination(
  date: string, 
  period: number, 
  farmId: string, 
  minerModel: string
): Promise<boolean> {
  const client = await pool.connect();
  try {
    console.log(`\n=== Processing ${date} Period ${period} Farm ${farmId} Model ${minerModel} ===\n`);
    
    // Get curtailment record
    const curtailmentRecord = await getCurtailmentRecord(client, date, period, farmId);
    
    if (!curtailmentRecord) {
      console.log(`No curtailment record found for ${date} Period ${period} Farm ${farmId}`);
      return false;
    }
    
    console.log('Curtailment record found:');
    console.log(curtailmentRecord);
    
    // Get difficulty
    const difficulty = await getDifficultyData(date);
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
      date, 
      period, 
      farmId, 
      minerModel, 
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
 * Main reconciliation function for a date and period
 */
async function reconcileDatePeriod(date: string, period: number): Promise<void> {
  console.log(`\n========== Reconciling ${date} Period ${period} ==========\n`);
  
  // Get missing combinations
  const missingCombinations = await getMissingCombinations(date, period);
  
  if (missingCombinations.length === 0) {
    console.log(`No missing combinations found for ${date} Period ${period}`);
    return;
  }
  
  console.log(`Found ${missingCombinations.length} missing combinations for ${date} Period ${period}:`);
  console.table(missingCombinations);
  
  // Process each combination
  let successCount = 0;
  let failureCount = 0;
  
  for (const combo of missingCombinations) {
    // Add slight delay to avoid overwhelming the database
    await sleep(100);
    
    const success = await processCombination(date, period, combo.farmId, combo.minerModel);
    if (success) {
      successCount++;
    } else {
      failureCount++;
    }
  }
  
  console.log(`\n===== Reconciliation Summary for ${date} Period ${period} =====`);
  console.log(`Total combinations: ${missingCombinations.length}`);
  console.log(`Successfully processed: ${successCount}`);
  console.log(`Failed: ${failureCount}`);
}

/**
 * Process specific date with multiple periods
 */
async function reconcileDate(date: string, periods: number[]): Promise<void> {
  console.log(`\n=========== Reconciling ${date} for Periods ${periods.join(', ')} ===========\n`);
  
  for (const period of periods) {
    await reconcileDatePeriod(date, period);
  }
  
  console.log(`\nCompleted reconciliation for ${date}`);
}

/**
 * Main function
 */
async function main(): Promise<void> {
  try {
    // Get command line arguments
    const args = process.argv.slice(2);
    
    if (args.length < 1) {
      console.error('Usage: npx tsx reconcile_date_period_combination.ts <date> [period1,period2,...]');
      console.error('Example: npx tsx reconcile_date_period_combination.ts 2023-12-21 7,8,9');
      process.exit(1);
    }
    
    const date = args[0];
    const periods = args.length > 1 ? 
      args[1].split(',').map(p => parseInt(p.trim())) : 
      Array.from({ length: 48 }, (_, i) => i + 1); // All 48 periods if not specified
    
    await reconcileDate(date, periods);
    
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
    console.log('\nReconciliation completed successfully');
    process.exit(0);
  })
  .catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
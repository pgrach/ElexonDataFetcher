/**
 * Consolidated Reconciliation System
 * 
 * This file contains all the necessary functions for reconciling curtailment records
 * with Bitcoin calculations, replacing multiple separate scripts. This is the primary
 * entry point for all reconciliation operations.
 * 
 * Usage examples:
 * 
 * 1. Check status: npx tsx reconciliation.ts status
 * 2. Fix all missing: npx tsx reconciliation.ts reconcile
 * 3. Check specific date: npx tsx reconciliation.ts date 2023-12-25
 * 4. Fix specific period: npx tsx reconciliation.ts period 2023-12-21 7
 * 5. Fix specific combination: npx tsx reconciliation.ts combo 2023-12-21 7 E_BABAW-1 M20S
 * 6. Process batch with limit: npx tsx reconciliation.ts batch 2023-12-21 10
 * 7. Fix December 2023: npx tsx reconciliation.ts december
 * 8. Fix date range: npx tsx reconciliation.ts range 2023-12-01 2023-12-31
 */

import { db } from "./db";
import { sql } from "drizzle-orm";
import { auditAndFixBitcoinCalculations } from "./server/services/historicalReconciliation";
import pg from 'pg';
import { DynamoDBClient, DescribeTableCommand, ScanCommand } from '@aws-sdk/client-dynamodb';
import { DEFAULT_DIFFICULTY } from './server/types/bitcoin';

// Pool for direct database access when needed
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL
});

// DynamoDB client
const dynamoClient = new DynamoDBClient({
  region: 'eu-north-1'
});

// Table name for difficulty data
const DIFFICULTY_TABLE = 'asics-dynamodb-DifficultyTable-DQ308ID3POT6';

// Sleep utility
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Constants
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

/**
 * Get summary statistics about reconciliation status
 */
async function getReconciliationStatus() {
  console.log("=== Bitcoin Calculations Reconciliation Status ===\n");
  console.log("Checking current reconciliation status...\n");

  // Get total curtailment records and unique date-period-farm combinations
  const curtailmentResult = await db.execute(sql`
    SELECT 
      COUNT(*) as total_records,
      COUNT(DISTINCT (settlement_date || '-' || settlement_period || '-' || farm_id)) as unique_combinations
    FROM curtailment_records
  `);
  
  const totalCurtailmentRecords = Number(curtailmentResult.rows[0].total_records);
  const uniqueCombinations = Number(curtailmentResult.rows[0].unique_combinations);
  
  // Get Bitcoin calculation counts by miner model
  const bitcoinCounts: Record<string, number> = {};
  
  for (const model of MINER_MODELS) {
    const result = await db.execute(sql`
      SELECT COUNT(*) as count
      FROM historical_bitcoin_calculations
      WHERE miner_model = ${model}
    `);
    
    bitcoinCounts[model] = Number(result[0]?.count) || 0;
  }
  
  // Expected Bitcoin calculation count for 100% reconciliation
  // For each unique date-period-farm combination, we should have one calculation per miner model
  const expectedTotal = uniqueCombinations * MINER_MODELS.length;
  const actualTotal = Object.values(bitcoinCounts).reduce((sum, count) => sum + Number(count), 0);
  
  // Calculate reconciliation percentage with safety checks
  let reconciliationPercentage = 100;
  if (expectedTotal > 0) {
    reconciliationPercentage = Math.min((actualTotal / expectedTotal) * 100, 100);
  }
  
  const status = {
    totalCurtailmentRecords,
    uniqueDatePeriodFarmCombinations: uniqueCombinations,
    bitcoinCalculationsByModel: bitcoinCounts,
    totalBitcoinCalculations: actualTotal,
    expectedBitcoinCalculations: expectedTotal,
    missingCalculations: expectedTotal - actualTotal,
    reconciliationPercentage: Math.round(reconciliationPercentage * 100) / 100
  };
  
  // Print status
  console.log("=== Overall Status ===");
  console.log(`Curtailment Records: ${status.totalCurtailmentRecords}`);
  console.log(`Unique Period-Farm Combinations: ${status.uniqueDatePeriodFarmCombinations}`);
  console.log(`Bitcoin Calculations: ${status.totalBitcoinCalculations}`);
  console.log(`Expected Calculations: ${status.expectedBitcoinCalculations}`);
  console.log(`Missing Calculations: ${status.missingCalculations}`);
  console.log(`Reconciliation: ${status.reconciliationPercentage}%\n`);
  
  console.log("Bitcoin Calculations by Model:");
  for (const [model, count] of Object.entries(status.bitcoinCalculationsByModel)) {
    console.log(`- ${model}: ${count}`);
  }
  
  return status;
}

/**
 * Find dates with missing Bitcoin calculations
 */
async function findDatesWithMissingCalculations() {
  const result = await db.execute(sql`
    WITH dates_with_curtailment AS (
      SELECT DISTINCT settlement_date
      FROM curtailment_records
      ORDER BY settlement_date DESC
    ),
    unique_date_combos AS (
      SELECT 
        settlement_date,
        COUNT(DISTINCT (settlement_period || '-' || farm_id)) as unique_combinations
      FROM curtailment_records
      GROUP BY settlement_date
    ),
    date_calculations AS (
      SELECT 
        c.settlement_date,
        COUNT(DISTINCT b.id) as calculation_count,
        u.unique_combinations * ${MINER_MODELS.length} as expected_count
      FROM dates_with_curtailment c
      JOIN unique_date_combos u ON c.settlement_date = u.settlement_date
      LEFT JOIN historical_bitcoin_calculations b 
        ON c.settlement_date = b.settlement_date
      GROUP BY c.settlement_date, u.unique_combinations
    )
    SELECT 
      settlement_date::text as date,
      calculation_count,
      expected_count,
      ROUND((calculation_count * 100.0) / expected_count, 2) as completion_percentage
    FROM date_calculations
    WHERE calculation_count < expected_count
    ORDER BY completion_percentage ASC, settlement_date DESC
    LIMIT 30
  `);
  
  const missingDates = result.rows.map(row => ({
    date: String(row.date),
    actual: Number(row.calculation_count),
    expected: Number(row.expected_count),
    completionPercentage: Number(row.completion_percentage)
  }));
  
  console.log("Finding dates with missing calculations...\n");
  
  if (missingDates.length === 0) {
    console.log("No dates with missing calculations found!");
    return [];
  }
  
  console.log(`Found ${missingDates.length} dates with missing calculations:`);
  missingDates.forEach(d => {
    console.log(`- ${d.date}: ${d.actual}/${d.expected} (${d.completionPercentage}%)`);
  });
  
  return missingDates;
}

/**
 * Main reconciliation function
 */
async function reconcileBitcoinCalculations() {
  try {
    console.log("=== Starting Bitcoin Calculation Reconciliation ===\n");
    
    // Get initial reconciliation status
    const initialStatus = await getReconciliationStatus();
    
    // If we're already at 100%, we're done
    if (initialStatus.reconciliationPercentage === 100) {
      console.log("\n✅ Already at 100% reconciliation! No action needed.");
      return;
    }
    
    // Find dates with missing calculations
    const missingDates = await findDatesWithMissingCalculations();
    
    if (missingDates.length === 0) return;
    
    console.log("\nTo fix missing calculations, run:\nnpx tsx reconciliation.ts reconcile");
    
  } catch (error) {
    console.error("Error during reconciliation process:", error);
    throw error;
  }
}

/**
 * Fix a specific date
 */
async function fixDate(date: string) {
  console.log(`\n=== Fixing Bitcoin Calculations for ${date} ===\n`);
  
  try {
    const result = await auditAndFixBitcoinCalculations(date);
    
    if (result.success) {
      console.log(`✅ ${date}: Fixed - ${result.message}`);
    } else {
      console.log(`❌ ${date}: Failed - ${result.message}`);
    }
    
    return result;
  } catch (error) {
    console.error(`Error fixing ${date}:`, error);
    throw error;
  }
}

/**
 * Fix all missing dates
 */
async function fixAllMissingDates() {
  console.log("=== Fixing All Missing Calculations ===\n");
  
  // Get initial reconciliation status
  const initialStatus = await getReconciliationStatus();
  
  // Find dates with missing calculations
  const missingDates = await findDatesWithMissingCalculations();
  
  if (missingDates.length === 0) return;
  
  // Process the dates
  console.log(`\nProcessing ${missingDates.length} dates...\n`);
  
  let successful = 0;
  let failed = 0;
  
  for (const item of missingDates) {
    try {
      console.log(`Processing ${item.date}...`);
      const result = await auditAndFixBitcoinCalculations(item.date);
      
      if (result.success) {
        console.log(`✅ ${item.date}: Fixed - ${result.message}\n`);
        successful++;
      } else {
        console.log(`❌ ${item.date}: Failed - ${result.message}\n`);
        failed++;
      }
    } catch (error) {
      console.error(`Error processing ${item.date}:`, error);
      failed++;
    }
  }
  
  // Get final reconciliation status
  console.log("\nChecking final reconciliation status...");
  const finalStatus = await getReconciliationStatus();
  
  console.log("\n=== Reconciliation Summary ===");
  console.log(`Dates Processed: ${missingDates.length}`);
  console.log(`Successful: ${successful}`);
  console.log(`Failed: ${failed}`);
  console.log(`Initial Reconciliation: ${initialStatus.reconciliationPercentage}%`);
  console.log(`Final Reconciliation: ${finalStatus.reconciliationPercentage}%`);
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
 * Get missing combinations for a specific date and period
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
async function getCurtailmentRecord(client: pg.PoolClient, date: string, period: number, farmId: string) {
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
async function processSpecificCombination(
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
    
    // Check if record already exists to avoid duplicates
    const existsQuery = `
      SELECT id FROM historical_bitcoin_calculations
      WHERE settlement_date = $1
        AND settlement_period = $2
        AND farm_id = $3
        AND miner_model = $4
    `;
    
    const existsResult = await client.query(existsQuery, [
      date, 
      period, 
      farmId, 
      minerModel
    ]);
    
    if (existsResult.rows.length > 0) {
      console.log(`Record already exists for this combination with ID: ${existsResult.rows[0].id}`);
      return true;
    }
    
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
 * Fix a specific period on a date
 */
async function fixPeriod(date: string, period: number): Promise<{success: number, failure: number}> {
  console.log(`\n========== Reconciling ${date} Period ${period} ==========\n`);
  
  // Get missing combinations
  const missingCombinations = await getMissingCombinations(date, period);
  
  if (missingCombinations.length === 0) {
    console.log(`No missing combinations found for ${date} Period ${period}`);
    return { success: 0, failure: 0 };
  }
  
  console.log(`Found ${missingCombinations.length} missing combinations for ${date} Period ${period}:`);
  console.table(missingCombinations);
  
  // Process each combination
  let successCount = 0;
  let failureCount = 0;
  
  for (const combo of missingCombinations) {
    // Add slight delay to avoid overwhelming the database
    await sleep(100);
    
    const success = await processSpecificCombination(date, period, combo.farmId, combo.minerModel);
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
  
  return { success: successCount, failure: failureCount };
}

/**
 * Get missing combinations for a date (limited batch)
 */
async function getMissingCombinationsForDate(date: string, limit: number = 10): Promise<Array<{date: string, period: number, farmId: string, minerModel: string}>> {
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
 * Process a batch of missing combinations
 */
async function processBatch(date: string, batchSize: number = 10): Promise<void> {
  console.log(`\n========== Reconciling batch for ${date} (limit: ${batchSize}) ==========\n`);
  
  // Get missing combinations
  const missingCombinations = await getMissingCombinationsForDate(date, batchSize);
  
  if (missingCombinations.length === 0) {
    console.log(`No missing combinations found for ${date}`);
    return;
  }
  
  console.log(`Found ${missingCombinations.length} missing combinations for ${date}:`);
  missingCombinations.forEach((combo, index) => {
    console.log(`${index + 1}. Date: ${combo.date}, Period: ${combo.period}, Farm: ${combo.farmId}, Model: ${combo.minerModel}`);
  });
  
  // Process each combination
  let successCount = 0;
  let failureCount = 0;
  
  for (const combo of missingCombinations) {
    // Add slight delay to avoid overwhelming the database
    await sleep(100);
    
    const success = await processSpecificCombination(combo.date, combo.period, combo.farmId, combo.minerModel);
    if (success) {
      successCount++;
    } else {
      failureCount++;
    }
  }
  
  // Print summary
  console.log(`\n===== Reconciliation Batch Summary for ${date} =====`);
  console.log(`Total combinations: ${missingCombinations.length}`);
  console.log(`Successfully processed: ${successCount}`);
  console.log(`Failed: ${failureCount}`);
}

/**
 * Fix December 2023 data
 */
async function fixDecember2023(): Promise<void> {
  console.log("\n=== Fixing December 2023 Data ===\n");
  
  const startDate = '2023-12-01';
  const endDate = '2023-12-31';
  
  // Get status of December dates
  const query = `
    WITH december_stats AS (
      SELECT 
        cr.settlement_date,
        COUNT(*) * 3 AS expected_count,
        (
          SELECT COUNT(*) 
          FROM historical_bitcoin_calculations hbc
          WHERE hbc.settlement_date = cr.settlement_date
        ) AS actual_count
      FROM 
        curtailment_records cr
      WHERE 
        cr.settlement_date >= $1 AND cr.settlement_date <= $2
      GROUP BY 
        cr.settlement_date
    )
    SELECT 
      settlement_date::text as date,
      expected_count,
      actual_count,
      CASE 
        WHEN expected_count > 0 THEN 
          ROUND((actual_count::numeric / expected_count::numeric), 4)
        ELSE 0 
      END AS completion_percentage,
      (expected_count - actual_count) AS missing_count
    FROM 
      december_stats
    WHERE 
      expected_count > actual_count
    ORDER BY 
      missing_count DESC;
  `;
  
  const client = await pool.connect();
  try {
    const result = await client.query(query, [startDate, endDate]);
    
    if (result.rows.length === 0) {
      console.log("All December 2023 data is already reconciled!");
      return;
    }
    
    console.log(`Found ${result.rows.length} dates in December 2023 with missing calculations:`);
    
    // Sort by most missing
    const dates = result.rows.map(row => ({
      date: row.date,
      missing: parseInt(row.missing_count, 10),
      completion: parseFloat(row.completion_percentage)
    }));
    
    console.table(dates);
    
    // Process each date
    let processedDates = 0;
    for (const dateInfo of dates) {
      if (processedDates >= 5) {
        console.log("Processed maximum number of dates (5) in this run. Run again to process more.");
        break;
      }
      
      console.log(`\nProcessing ${dateInfo.date} (${dateInfo.missing} missing, ${dateInfo.completion * 100}% complete)...`);
      await fixDate(dateInfo.date);
      processedDates++;
    }
    
  } catch (error) {
    console.error("Error fixing December 2023:", error);
  } finally {
    client.release();
  }
}

/**
 * Process a date range
 */
async function fixDateRange(startDate: string, endDate: string): Promise<void> {
  console.log(`\n=== Processing Date Range: ${startDate} to ${endDate} ===\n`);
  
  // Get dates in the range with missing calculations
  const query = `
    WITH date_stats AS (
      SELECT 
        cr.settlement_date,
        COUNT(*) * 3 AS expected_count,
        (
          SELECT COUNT(*) 
          FROM historical_bitcoin_calculations hbc
          WHERE hbc.settlement_date = cr.settlement_date
        ) AS actual_count
      FROM 
        curtailment_records cr
      WHERE 
        cr.settlement_date >= $1 AND cr.settlement_date <= $2
      GROUP BY 
        cr.settlement_date
    )
    SELECT 
      settlement_date::text as date,
      expected_count,
      actual_count,
      CASE 
        WHEN expected_count > 0 THEN 
          ROUND((actual_count::numeric / expected_count::numeric), 4)
        ELSE 0 
      END AS completion_percentage,
      (expected_count - actual_count) AS missing_count
    FROM 
      date_stats
    WHERE 
      expected_count > actual_count
    ORDER BY 
      missing_count DESC;
  `;
  
  const client = await pool.connect();
  try {
    const result = await client.query(query, [startDate, endDate]);
    
    if (result.rows.length === 0) {
      console.log(`All dates from ${startDate} to ${endDate} are already reconciled!`);
      return;
    }
    
    console.log(`Found ${result.rows.length} dates in range with missing calculations:`);
    
    // Sort by most missing
    const dates = result.rows.map(row => ({
      date: row.date,
      missing: parseInt(row.missing_count, 10),
      completion: parseFloat(row.completion_percentage)
    }));
    
    console.table(dates);
    
    // Process each date
    let processedDates = 0;
    for (const dateInfo of dates) {
      if (processedDates >= 5) {
        console.log("Processed maximum number of dates (5) in this run. Run again to process more.");
        break;
      }
      
      console.log(`\nProcessing ${dateInfo.date} (${dateInfo.missing} missing, ${dateInfo.completion * 100}% complete)...`);
      await fixDate(dateInfo.date);
      processedDates++;
    }
    
  } catch (error) {
    console.error(`Error fixing date range ${startDate} to ${endDate}:`, error);
  } finally {
    client.release();
  }
}

/**
 * Main function
 */
async function main() {
  const command = process.argv[2]?.toLowerCase();
  const param1 = process.argv[3];
  const param2 = process.argv[4];
  const param3 = process.argv[5];
  const param4 = process.argv[6];
  
  switch (command) {
    case "status":
      await getReconciliationStatus();
      console.log("\nTo find missing calculations, run:\nnpx tsx reconciliation.ts find");
      break;
      
    case "find":
      await getReconciliationStatus();
      await findDatesWithMissingCalculations();
      break;
      
    case "reconcile":
      await fixAllMissingDates();
      break;
      
    case "date":
      if (!param1) {
        console.error("Error: Date parameter required");
        console.log("Usage: npx tsx reconciliation.ts date YYYY-MM-DD");
        process.exit(1);
      }
      await fixDate(param1);
      break;
      
    case "period":
      if (!param1 || !param2) {
        console.error("Error: Date and period parameters required");
        console.log("Usage: npx tsx reconciliation.ts period YYYY-MM-DD PERIOD_NUMBER");
        process.exit(1);
      }
      await fixPeriod(param1, parseInt(param2, 10));
      break;
      
    case "combo":
      if (!param1 || !param2 || !param3 || !param4) {
        console.error("Error: Date, period, farm_id, and miner_model parameters required");
        console.log("Usage: npx tsx reconciliation.ts combo YYYY-MM-DD PERIOD_NUMBER FARM_ID MINER_MODEL");
        process.exit(1);
      }
      await processSpecificCombination(param1, parseInt(param2, 10), param3, param4);
      break;
      
    case "batch":
      if (!param1) {
        console.error("Error: Date parameter required");
        console.log("Usage: npx tsx reconciliation.ts batch YYYY-MM-DD [BATCH_SIZE]");
        process.exit(1);
      }
      const batchSize = param2 ? parseInt(param2, 10) : 10;
      await processBatch(param1, batchSize);
      break;
      
    case "december":
      await fixDecember2023();
      break;
      
    case "range":
      if (!param1 || !param2) {
        console.error("Error: Start date and end date parameters required");
        console.log("Usage: npx tsx reconciliation.ts range START_DATE END_DATE");
        process.exit(1);
      }
      await fixDateRange(param1, param2);
      break;
      
    default:
      // Default behavior - just show status
      console.log("Bitcoin Reconciliation Tool\n");
      console.log("Commands:");
      console.log("  status     - Show reconciliation status");
      console.log("  find       - Find dates with missing calculations");
      console.log("  reconcile  - Fix all missing calculations");
      console.log("  date DATE  - Fix a specific date");
      console.log("  period DATE PERIOD - Fix a specific period on a date");
      console.log("  combo DATE PERIOD FARM_ID MODEL - Fix a specific combination");
      console.log("  batch DATE [SIZE] - Process a batch of combinations for a date");
      console.log("  december   - Fix December 2023 data");
      console.log("  range START END - Fix a date range");
      console.log("\nExample: npx tsx reconciliation.ts status");
      
      await getReconciliationStatus();
      await findDatesWithMissingCalculations();
  }
}

// Run the main function if script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .then(() => {
      console.log("\n=== Reconciliation Complete ===");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error:", error);
      process.exit(1);
    });
}

export { 
  getReconciliationStatus, 
  findDatesWithMissingCalculations, 
  reconcileBitcoinCalculations, 
  fixDate,
  fixPeriod,
  fixAllMissingDates,
  processBatch,
  processSpecificCombination,
  fixDecember2023,
  fixDateRange
};
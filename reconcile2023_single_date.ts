/**
 * Reconcile 2023 - Single Date Processor
 * 
 * This script fixes missing Bitcoin calculations for a specific date in 2023.
 * It's optimized for efficiency and provides detailed logging of the process.
 * 
 * Usage:
 *   npx tsx reconcile2023_single_date.ts YYYY-MM-DD
 *   
 * Example:
 *   npx tsx reconcile2023_single_date.ts 2023-01-04
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { count, and, eq, sql } from "drizzle-orm";
import { processDailyCurtailment } from "./server/services/curtailment";
import { minerModels } from "./server/types/bitcoin";
import { DynamoDBClient, PutItemCommand, QueryCommand } from "@aws-sdk/client-dynamodb";
import * as fs from "fs";
import path from "path";
import { fileURLToPath } from 'url';

// Get __dirname equivalent in ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Constants
const DIFFICULTY_TABLE = 'asics-dynamodb-DifficultyTable-DQ308ID3POT6';
const DIFFICULTY_DATA_FILE = path.join(__dirname, 'data', '2023_difficulty_data.json');
const FALLBACK_DIFFICULTY = 30802868313908; // Average difficulty for 2023

// Initialize DynamoDB client
const dynamoDb = new DynamoDBClient({
  region: process.env.AWS_REGION || 'eu-north-1',
});

/**
 * Format date for DynamoDB
 */
function formatDateForDynamoDB(dateStr: string): string {
  return dateStr.replace(/-/g, '');
}

/**
 * Generate a random UUID
 */
function generateUUID(): string {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    const r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

/**
 * Load 2023 difficulty data from file
 */
function load2023DifficultyData(): Record<string, number> {
  try {
    if (fs.existsSync(DIFFICULTY_DATA_FILE)) {
      const data = fs.readFileSync(DIFFICULTY_DATA_FILE, 'utf8');
      return JSON.parse(data);
    }
  } catch (error) {
    console.error(`Error loading difficulty data from file:`, error);
  }
  
  // Return empty object if file doesn't exist or can't be read
  return {};
}

/**
 * Check if difficulty data exists for a date in DynamoDB
 */
async function checkDifficultyExists(date: string): Promise<boolean> {
  try {
    const formattedDate = formatDateForDynamoDB(date);
    
    const command = new QueryCommand({
      TableName: DIFFICULTY_TABLE,
      KeyConditionExpression: 'ID = :hashKey AND #date = :rangeKey',
      ExpressionAttributeNames: {
        '#date': 'Date', // 'Date' is a reserved word in DynamoDB expressions
      },
      ExpressionAttributeValues: {
        ':hashKey': { S: 'difficulty' },
        ':rangeKey': { S: formattedDate }
      }
    });
    
    const response = await dynamoDb.send(command);
    return response.Items !== undefined && response.Items.length > 0;
  } catch (error) {
    console.error(`Error checking if difficulty data exists for ${date}:`, error);
    return false;
  }
}

/**
 * Store difficulty data for a date in DynamoDB
 */
async function storeDifficultyData(date: string, difficulty: number): Promise<boolean> {
  try {
    console.log(`Storing difficulty ${difficulty.toLocaleString()} for ${date}...`);
    
    const formattedDate = formatDateForDynamoDB(date);
    const id = generateUUID();
    
    const command = new PutItemCommand({
      TableName: DIFFICULTY_TABLE,
      Item: {
        'ID': { S: 'difficulty' },
        'Date': { S: formattedDate },
        'uuid': { S: id },
        'difficulty': { N: difficulty.toString() },
      },
    });
    
    await dynamoDb.send(command);
    console.log(`Successfully stored difficulty data for ${date}`);
    return true;
  } catch (error) {
    console.error(`Error storing difficulty data for ${date}:`, error);
    return false;
  }
}

/**
 * Ensure difficulty data exists for a date before processing
 */
async function ensureDifficultyData(date: string): Promise<boolean> {
  console.log(`Checking difficulty data for ${date}...`);
  
  // First check if it already exists in DynamoDB
  const exists = await checkDifficultyExists(date);
  if (exists) {
    console.log(`Difficulty data already exists for ${date}`);
    return true;
  }
  
  // If not, get it from the local file
  console.log(`Difficulty data not found in DynamoDB, checking local file...`);
  const difficultyData = load2023DifficultyData();
  
  if (difficultyData[date]) {
    // Store it in DynamoDB for future use
    const difficulty = difficultyData[date];
    console.log(`Found difficulty in local file for ${date}: ${difficulty.toLocaleString()}`);
    
    return await storeDifficultyData(date, difficulty);
  }
  
  // If all else fails, use fallback difficulty
  console.log(`No difficulty data found for ${date}, using fallback value: ${FALLBACK_DIFFICULTY.toLocaleString()}`);
  return await storeDifficultyData(date, FALLBACK_DIFFICULTY);
}

interface MissingCalculation {
  period: number;
  farmId: string;
}

/**
 * Sleep for specified milliseconds
 */
async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Get missing calculations for a specific date and miner model
 */
async function getMissingCalculations(date: string, minerModel: string): Promise<MissingCalculation[]> {
  // This query finds all curtailment records that don't have a corresponding Bitcoin calculation
  // for this specific date and miner model
  const result = await db.execute(sql`
    SELECT cr.settlement_period, cr.farm_id
    FROM curtailment_records cr
    LEFT JOIN historical_bitcoin_calculations hbc ON 
      cr.settlement_date = hbc.settlement_date AND
      cr.settlement_period = hbc.settlement_period AND
      cr.farm_id = hbc.farm_id AND
      hbc.miner_model = ${minerModel}
    WHERE 
      cr.settlement_date = ${date} AND
      hbc.settlement_date IS NULL
    ORDER BY cr.settlement_period, cr.farm_id
  `);
  
  return result.rows.map((row: any) => ({
    period: parseInt(row.settlement_period),
    farmId: row.farm_id,
  }));
}

/**
 * Get summary of missing calculations for a date
 */
async function getDateSummary(date: string): Promise<{
  totalCurtailmentRecords: number;
  missingByModel: Record<string, number>;
  totalMissing: number;
}> {
  // Get total number of curtailment records
  const curtailmentResult = await db.select({
    count: count(),
  }).from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  const totalCurtailmentRecords = curtailmentResult[0].count;
  
  // Check each miner model
  const missingByModel: Record<string, number> = {};
  let totalMissing = 0;
  
  for (const minerModel of Object.keys(minerModels)) {
    // Get missing calculations for this model
    const missing = await getMissingCalculations(date, minerModel);
    missingByModel[minerModel] = missing.length;
    totalMissing += missing.length;
  }
  
  return {
    totalCurtailmentRecords,
    missingByModel,
    totalMissing
  };
}

/**
 * Fix missing calculations for a specific date
 */
async function fixDate(date: string): Promise<boolean> {
  console.log(`\n===== PROCESSING DATE: ${date} =====`);
  
  // Step 1: Get summary of missing calculations before processing
  console.log("Step 1: Checking for missing calculations...");
  const beforeSummary = await getDateSummary(date);
  
  if (beforeSummary.totalMissing === 0) {
    console.log(`No missing calculations found for ${date}. Already complete!`);
    return true;
  }
  
  console.log(`Found ${beforeSummary.totalMissing} missing calculations for ${date}`);
  console.log("Missing by model:");
  for (const [model, count] of Object.entries(beforeSummary.missingByModel)) {
    console.log(`  ${model}: ${count}`);
  }
  
  // Step 2: Ensure Bitcoin difficulty data exists
  console.log("\nStep 2: Verifying Bitcoin difficulty data...");
  const difficultyExists = await ensureDifficultyData(date);
  if (!difficultyExists) {
    console.error(`Failed to ensure difficulty data for ${date}. This may cause calculation errors.`);
    // Continue anyway, using the fallback value
  }
  
  // Step 3: Process all calculations for the date
  console.log("\nStep 3: Processing Bitcoin calculations...");
  try {
    await processDailyCurtailment(date);
    console.log(`Successfully processed calculations for ${date}`);
  } catch (error) {
    console.error(`Error processing calculations for ${date}:`, error);
    return false;
  }
  
  // Allow time for database to process
  await sleep(1000);
  
  // Step 4: Verify results
  console.log("\nStep 4: Verifying results...");
  const afterSummary = await getDateSummary(date);
  
  const totalFixed = beforeSummary.totalMissing - afterSummary.totalMissing;
  const percentFixed = (totalFixed / beforeSummary.totalMissing * 100).toFixed(2);
  
  console.log(`\n===== RESULTS FOR ${date} =====`);
  console.log(`Before: ${beforeSummary.totalMissing} missing calculations`);
  console.log(`After: ${afterSummary.totalMissing} missing calculations`);
  console.log(`Fixed: ${totalFixed} calculations (${percentFixed}%)`);
  
  if (afterSummary.totalMissing === 0) {
    console.log(`\n✅ SUCCESS: All missing calculations for ${date} have been fixed!`);
    return true;
  } else if (afterSummary.totalMissing < beforeSummary.totalMissing) {
    console.log(`\n⚠️ PARTIAL SUCCESS: Fixed ${totalFixed} of ${beforeSummary.totalMissing} calculations (${percentFixed}%)`);
    
    // If we fixed more than 80%, we consider it a success
    if (totalFixed > beforeSummary.totalMissing * 0.8) {
      return true;
    }
  } else {
    console.log(`\n❌ FAILED: Could not fix calculations for ${date}`);
  }
  
  return false;
}

/**
 * Main function to process a date
 */
async function main() {
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    console.error("Please provide a date in format YYYY-MM-DD");
    process.exit(1);
  }
  
  const date = args[0];
  
  // Validate date format
  if (!date.match(/^\d{4}-\d{2}-\d{2}$/)) {
    console.error("Invalid date format. Use YYYY-MM-DD");
    process.exit(1);
  }
  
  try {
    const success = await fixDate(date);
    process.exit(success ? 0 : 1);
  } catch (error) {
    console.error("Unhandled error:", error);
    process.exit(1);
  }
}

// Run the script
main();
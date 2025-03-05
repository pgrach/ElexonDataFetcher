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
import { and, count, eq, sql } from "drizzle-orm";
import { minerModels } from "./server/types/bitcoin";
import { processDailyCurtailment } from "./server/services/curtailment";
import { DynamoDBClient, ScanCommand, PutItemCommand } from "@aws-sdk/client-dynamodb";
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

// Utility functions
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
  console.log(`Finding missing calculations for ${date}, model ${minerModel}...`);
  
  const result = await db.execute(sql`
    SELECT c.settlement_period, c.farm_id
    FROM curtailment_records c
    LEFT JOIN historical_bitcoin_calculations h
    ON c.settlement_date = h.settlement_date
    AND c.settlement_period = h.settlement_period
    AND c.farm_id = h.farm_id
    AND h.miner_model = ${minerModel}
    WHERE c.settlement_date = ${date}
    AND h.id IS NULL
    ORDER BY c.settlement_period, c.farm_id
  `);
  
  const missingCalculations = result.rows.map((row: any) => ({
    period: parseInt(row.settlement_period, 10),
    farmId: row.farm_id
  }));
  
  console.log(`Found ${missingCalculations.length} missing calculations for ${minerModel}`);
  return missingCalculations;
}

/**
 * Get summary of missing calculations for a date
 */
async function getDateSummary(date: string): Promise<{
  totalCurtailmentRecords: number;
  missingByModel: Record<string, number>;
  totalMissing: number;
}> {
  // Get total curtailment records
  const curtailmentResult = await db.select({
    count: count(),
  }).from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  const totalCurtailmentRecords = curtailmentResult[0].count;
  
  // Check calculations for each miner model
  const missingByModel: Record<string, number> = {};
  let totalMissing = 0;
  
  for (const minerModel of Object.keys(minerModels)) {
    // Count bitcoin calculations for this date and model
    const calculationsResult = await db.select({
      count: count(),
    }).from(historicalBitcoinCalculations)
      .where(and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      ));
    
    const calculationsCount = calculationsResult[0].count;
    const missingCount = totalCurtailmentRecords - calculationsCount;
    
    missingByModel[minerModel] = missingCount;
    totalMissing += missingCount;
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
  
  // Check initial status
  const beforeSummary = await getDateSummary(date);
  
  console.log(`Current status for ${date}:`);
  console.log(`Total curtailment records: ${beforeSummary.totalCurtailmentRecords}`);
  console.log(`Total missing calculations: ${beforeSummary.totalMissing}`);
  console.log("Missing by model:");
  
  for (const [model, count] of Object.entries(beforeSummary.missingByModel)) {
    console.log(`  ${model}: ${count}`);
    
    if (count > 0) {
      // Get specific missing calculations for detailed logging
      const missingDetails = await getMissingCalculations(date, model);
      const uniquePeriods = [...new Set(missingDetails.map(item => item.period))].sort((a, b) => a - b);
      
      // Log unique periods with missing data
      console.log(`  Missing periods: ${uniquePeriods.join(', ')}`);
    }
  }
  
  // If no missing calculations, return early
  if (beforeSummary.totalMissing === 0) {
    console.log(`\nNo missing calculations for ${date}. Skipping.`);
    return true;
  }
  
  console.log(`\nFixing missing calculations for ${date}...`);
  
  try {
    // Reprocess the entire day's calculations through the curtailment service
    console.log("Calling processDailyCurtailment...");
    await processDailyCurtailment(date);
    console.log("processDailyCurtailment completed successfully");
    
    // Allow time for database to process
    console.log("Waiting for database processing...");
    await sleep(1000);
    
    // Verify results
    const afterSummary = await getDateSummary(date);
    
    console.log(`\nVerification results for ${date}:`);
    console.log(`Total curtailment records: ${afterSummary.totalCurtailmentRecords}`);
    console.log(`Total missing calculations: ${afterSummary.totalMissing}`);
    console.log("Missing by model:");
    
    for (const [model, count] of Object.entries(afterSummary.missingByModel)) {
      console.log(`  ${model}: ${count} (was: ${beforeSummary.missingByModel[model]})`);
      
      if (count > 0) {
        // Get specific missing calculations for detailed logging
        const missingDetails = await getMissingCalculations(date, model);
        const uniquePeriods = [...new Set(missingDetails.map(item => item.period))].sort((a, b) => a - b);
        
        // Log any remaining missing periods
        console.log(`  Still missing periods: ${uniquePeriods.join(', ')}`);
      }
    }
    
    // Check if all missing calculations were fixed
    if (afterSummary.totalMissing === 0) {
      console.log(`\n✅ SUCCESS: All missing calculations for ${date} were fixed!`);
      
      // Calculate improvement
      const fixedCount = beforeSummary.totalMissing - afterSummary.totalMissing;
      console.log(`Fixed ${fixedCount} missing calculations`);
      
      return true;
    } else if (afterSummary.totalMissing < beforeSummary.totalMissing) {
      // Some improvement but not complete
      const fixedCount = beforeSummary.totalMissing - afterSummary.totalMissing;
      const fixedPercent = (fixedCount / beforeSummary.totalMissing * 100).toFixed(2);
      
      console.log(`\n⚠️ PARTIAL SUCCESS: Fixed ${fixedCount} of ${beforeSummary.totalMissing} missing calculations (${fixedPercent}%)`);
      console.log(`${afterSummary.totalMissing} calculations still missing`);
      
      return false;
    } else {
      console.log(`\n❌ FAILED: No calculations were fixed for ${date}`);
      return false;
    }
  } catch (error) {
    console.error(`\n❌ ERROR: Failed to fix calculations for ${date}:`, error);
    return false;
  }
}

/**
 * Main function to process a date
 */
async function main() {
  // Parse command line arguments
  const date = process.argv[2];
  
  if (!date || !date.match(/^\d{4}-\d{2}-\d{2}$/)) {
    console.error("Please provide a valid date in YYYY-MM-DD format");
    console.log("Usage: npx tsx reconcile2023_single_date.ts YYYY-MM-DD");
    process.exit(1);
  }
  
  // Validate the date is in 2023
  if (!date.startsWith('2023-')) {
    console.error("This script is specifically for 2023 dates. Please provide a date in 2023.");
    process.exit(1);
  }
  
  // Process the date
  const startTime = Date.now();
  const success = await fixDate(date);
  const duration = (Date.now() - startTime) / 1000; // seconds
  
  // Log execution summary
  console.log(`\n===== EXECUTION SUMMARY =====`);
  console.log(`Date: ${date}`);
  console.log(`Status: ${success ? 'Success' : 'Incomplete'}`);
  console.log(`Duration: ${duration.toFixed(2)} seconds`);
  console.log(`Completed at: ${new Date().toISOString()}`);
  
  // Append to log file for tracking progress
  const logEntry = `${date},${success ? 'Success' : 'Incomplete'},${duration.toFixed(2)},${new Date().toISOString()}\n`;
  fs.appendFileSync('reconcile2023_progress.csv', logEntry);
  
  process.exit(success ? 0 : 1);
}

// Run the main function
main().catch(error => {
  console.error("Unhandled error:", error);
  process.exit(1);
});
/**
 * Direct Bitcoin Calculator for 2025-03-04
 * 
 * This script calculates Bitcoin mining potential for all periods without relying
 * on the DynamoDB service for difficulty data. Instead, it uses a fixed difficulty
 * value extracted from our existing calculations.
 */

import { db } from "./db";
import { sql, and, eq } from "drizzle-orm";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { minerModels } from "./server/types/bitcoin";

// Configuration
const DATE = "2025-03-04";
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const BATCH_SIZE = 100;
const DEFAULT_DIFFICULTY = 108105433845147; // Current network difficulty as fallback

// ANSI color codes for console output
const colors = {
  reset: "\x1b[0m",
  bright: "\x1b[1m",
  dim: "\x1b[2m",
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  blue: "\x1b[36m",
  magenta: "\x1b[35m"
};

function log(message: string, type: "info" | "success" | "warning" | "error" | "title" = "info"): void {
  const timestamp = new Date().toISOString().split('T')[1].replace('Z', '');
  
  switch (type) {
    case "title":
      console.log(`${colors.bright}${colors.magenta}${message}${colors.reset}`);
      break;
    case "info":
      console.log(`[${timestamp}] ${colors.blue}${message}${colors.reset}`);
      break;
    case "success":
      console.log(`[${timestamp}] ${colors.green}${message}${colors.reset}`);
      break;
    case "warning":
      console.log(`[${timestamp}] ${colors.yellow}${message}${colors.reset}`);
      break;
    case "error":
      console.log(`[${timestamp}] ${colors.red}${message}${colors.reset}`);
      break;
  }
}

async function clearExistingBitcoinCalculations(): Promise<void> {
  // First get the count of records we'll delete
  const countResult = await db
    .select({ count: sql<number>`COUNT(*)` })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, DATE));
    
  const count = countResult[0]?.count || 0;
  
  // Then delete them
  await db.delete(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, DATE));
    
  log(`Cleared ${count} existing Bitcoin calculation records`, "info");
}

async function processPeriodRange(startPeriod: number, endPeriod: number, minerModel: string, difficulty: number): Promise<number> {
  // Get all curtailment records for the period range
  const result = await db.execute(sql`
    SELECT 
      cr.id, 
      cr.settlement_date,
      cr.settlement_period,
      cr.farm_id,
      cr.volume
    FROM 
      curtailment_records cr
    WHERE 
      cr.settlement_date = ${DATE}
      AND cr.settlement_period BETWEEN ${startPeriod} AND ${endPeriod}
  `);
  
  // Convert the query result to a proper array
  const records = Array.isArray(result) ? result : [];
  
  if (records.length === 0) {
    log(`No curtailment records found for periods ${startPeriod}-${endPeriod}`, "info");
    return 0;
  }
  
  log(`Processing ${records.length} records for periods ${startPeriod}-${endPeriod} with model ${minerModel}`, "info");
  
  // Process in batches
  const totalRecords = records.length;
  let processedCount = 0;
  
  // Process records in batches
  for (let i = 0; i < totalRecords; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    
    // Create the calculation records
    const calculationRecords = batch.map(record => {
      // Calculate Bitcoin using the utility function
      const bitcoinMined = calculateBitcoin(
        Math.abs(parseFloat(record.volume.toString())),
        minerModel,
        difficulty
      );
      
      // Create a calculation record
      return {
        settlementDate: record.settlement_date,
        settlementPeriod: record.settlement_period,
        farmId: record.farm_id,
        minerModel: minerModel,
        difficulty: difficulty.toString(),
        bitcoinMined: bitcoinMined.toString(),
        calculatedAt: new Date()
      };
    });
    
    if (calculationRecords.length > 0) {
      // Insert the batch
      await db.insert(historicalBitcoinCalculations).values(calculationRecords);
      
      // Update progress
      processedCount += batch.length;
      log(`Inserted batch ${i/BATCH_SIZE + 1}: ${batch.length} records (${processedCount}/${totalRecords})`, "info");
    }
  }
  
  return processedCount;
}

async function processAllPeriods(minerModel: string, difficulty: number): Promise<number> {
  let totalProcessed = 0;
  
  // Process in batches of 8 periods each (6 batches total)
  for (let periodStart = 1; periodStart <= 48; periodStart += 8) {
    const periodEnd = Math.min(periodStart + 7, 48);
    const processed = await processPeriodRange(periodStart, periodEnd, minerModel, difficulty);
    totalProcessed += processed;
    
    // Small pause between batches to avoid overloading the database
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  
  return totalProcessed;
}

async function retrieveDifficultyFromExistingData(): Promise<number> {
  // Try to get the difficulty from existing calculations
  const existingCalc = await db
    .select({ difficulty: historicalBitcoinCalculations.difficulty })
    .from(historicalBitcoinCalculations)
    .where(
      and(
        eq(historicalBitcoinCalculations.settlementDate, DATE),
        sql`difficulty IS NOT NULL`
      )
    )
    .limit(1);
  
  if (existingCalc.length > 0 && existingCalc[0].difficulty) {
    return Number(existingCalc[0].difficulty);
  }
  
  // Fallback to default
  return DEFAULT_DIFFICULTY;
}

function calculateBitcoin(
  curtailedMwh: number,
  minerModel: string, 
  difficulty: number
): number {
  // Bitcoin network constants
  const BLOCK_REWARD = 3.125;
  const SETTLEMENT_PERIOD_MINUTES = 30;
  const BLOCKS_PER_SETTLEMENT_PERIOD = 3;

  const miner = minerModels[minerModel];
  if (!miner) throw new Error(`Invalid miner model: ${minerModel}`);

  const curtailedKwh = curtailedMwh * 1000;
  const minerConsumptionKwh = (miner.power / 1000) * (SETTLEMENT_PERIOD_MINUTES / 60);
  const potentialMiners = Math.floor(curtailedKwh / minerConsumptionKwh);
  
  const difficultyNum = typeof difficulty === 'string' ? parseFloat(difficulty) : difficulty;
  const hashesPerBlock = difficultyNum * Math.pow(2, 32);
  const networkHashRate = hashesPerBlock / 600;
  const networkHashRateTH = networkHashRate / 1e12;
  
  const totalHashPower = potentialMiners * miner.hashrate;
  const ourNetworkShare = totalHashPower / networkHashRateTH;
  
  return Number((ourNetworkShare * BLOCK_REWARD * BLOCKS_PER_SETTLEMENT_PERIOD).toFixed(8));
}

async function main() {
  try {
    log(`Processing direct Bitcoin calculations for ${DATE}`, "title");
    
    // Step 1: Get the difficulty value
    const difficulty = await retrieveDifficultyFromExistingData();
    log(`Using difficulty value: ${difficulty.toLocaleString()}`, "info");
    
    // Step 2: Clear existing calculations
    await clearExistingBitcoinCalculations();
    
    // Step 3: Process calculations for each miner model
    for (const minerModel of MINER_MODELS) {
      log(`Processing calculations for ${minerModel}...`, "info");
      const processed = await processAllPeriods(minerModel, difficulty);
      log(`Completed ${minerModel} calculations (${processed} records)`, "success");
    }
    
    // Step 4: Verify results
    const verificationResults = await Promise.all(
      MINER_MODELS.map(async (model) => {
        const stats = await db
          .select({
            recordCount: sql<number>`COUNT(*)`,
            periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
            totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`
          })
          .from(historicalBitcoinCalculations)
          .where(
            and(
              eq(historicalBitcoinCalculations.settlementDate, DATE),
              eq(historicalBitcoinCalculations.minerModel, model)
            )
          );
        
        return {
          model,
          records: stats[0].recordCount || 0,
          periods: stats[0].periodCount || 0,
          bitcoin: Number(stats[0].totalBitcoin || 0).toFixed(8)
        };
      })
    );
    
    log("Bitcoin calculation summary:", "title");
    verificationResults.forEach(result => {
      log(`${result.model}: ${result.records} records across ${result.periods} periods, total: ${result.bitcoin} BTC`, 
        result.periods === 48 ? "success" : "warning");
    });
    
    log("Processing complete!", "success");
  } catch (error) {
    log(`Error: ${error}`, "error");
    process.exit(1);
  }
}

main();
/**
 * Simple Bitcoin Calculations for 2025-03-29
 * 
 * This script processes Bitcoin calculations for March 29, 2025 data
 * using a fixed difficulty value (bypassing DynamoDB).
 * 
 * Usage:
 *   npx tsx process_bitcoin_calculations_simple_2025-03-29.ts
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { eq, and, inArray, sql } from "drizzle-orm";
import fs from "fs";
import path from "path";

// Constants
const TARGET_DATE = "2025-03-29";
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];
const DIFFICULTY = "55633605879865"; // Use a fixed difficulty value based on March 28
const LOG_FILE = `process_bitcoin_calculations_simple_${TARGET_DATE}.log`;

// Miner models data
const minerModels = {
  S19J_PRO: { hashrate: 100, power: 3050 },  // 100 TH/s @ 3050W
  S9: { hashrate: 14, power: 1350 },          // 14 TH/s @ 1350W
  M20S: { hashrate: 68, power: 3360 }         // 68 TH/s @ 3360W
};

// Bitcoin calculation constants
const BLOCK_REWARD = 3.125;
const SETTLEMENT_PERIOD_MINUTES = 30;
const BLOCKS_PER_SETTLEMENT_PERIOD = 3;

// Create log directory if it doesn't exist
const logDir = path.join(process.cwd(), "logs");
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir, { recursive: true });
}

// Logging utility
async function log(message: string, level: "info" | "error" | "warning" | "success" = "info"): Promise<void> {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] [${level.toUpperCase()}] ${message}`;
  
  // Log to console with color
  const colors = {
    info: "\x1b[36m", // Cyan
    error: "\x1b[31m", // Red
    warning: "\x1b[33m", // Yellow
    success: "\x1b[32m" // Green
  };
  console.log(`${colors[level]}${formattedMessage}\x1b[0m`);
  
  // Log to file
  const logPath = path.join(logDir, LOG_FILE);
  fs.appendFileSync(logPath, formattedMessage + "\n");
}

// Delay utility
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Calculate Bitcoin for BMU
function calculateBitcoinForBMU(
  curtailedMwh: number,
  minerModel: string,
  difficulty: number | string
): number {
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

// Process the Bitcoin calculations for a single day and miner model
async function processSingleDay(date: string, minerModel: string): Promise<void> {
  try {
    await log(`Processing ${date} with ${minerModel} using fixed difficulty ${DIFFICULTY}`, "info");

    return await db.transaction(async (tx) => {
      const curtailmentData = await tx
        .select({
          periods: sql<number[]>`array_agg(DISTINCT settlement_period)`,
          farmIds: sql<string[]>`array_agg(DISTINCT farm_id)`
        })
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, date),
            sql`ABS(volume::numeric) > 0`
          )
        );

      if (!curtailmentData[0] || !curtailmentData[0].periods || curtailmentData[0].periods.length === 0) {
        await log(`No curtailment records with volume for ${date}`, "error");
        return;
      }

      const periods = curtailmentData[0].periods;
      const farmIds = curtailmentData[0].farmIds;

      await tx.delete(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, date),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );

      const records = await tx
        .select()
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, date),
            inArray(curtailmentRecords.settlementPeriod, periods),
            sql`ABS(volume::numeric) > 0`
          )
        );

      await log(`Found ${records.length} curtailment records across ${periods.length} periods and ${farmIds.length} farms`, "info");

      const periodGroups = new Map<number, { totalVolume: number; farms: Map<string, number> }>();

      for (const record of records) {
        if (!periodGroups.has(record.settlementPeriod)) {
          periodGroups.set(record.settlementPeriod, {
            totalVolume: 0,
            farms: new Map<string, number>()
          });
        }

        const group = periodGroups.get(record.settlementPeriod)!;
        const absVolume = Math.abs(Number(record.volume));
        group.totalVolume += absVolume;
        group.farms.set(
          record.farmId,
          (group.farms.get(record.farmId) || 0) + absVolume
        );
      }

      const bulkInsertData: Array<{
        settlementDate: string;
        settlementPeriod: number;
        farmId: string;
        minerModel: string;
        bitcoinMined: string;
        difficulty: string;
        calculatedAt: Date;
      }> = [];

      for (const [period, data] of periodGroups) {
        const periodBitcoin = calculateBitcoinForBMU(
          data.totalVolume,
          minerModel,
          DIFFICULTY
        );

        for (const [farmId, farmVolume] of data.farms) {
          const bitcoinShare = (periodBitcoin * farmVolume) / data.totalVolume;
          bulkInsertData.push({
            settlementDate: date,
            settlementPeriod: period,
            farmId,
            minerModel,
            bitcoinMined: bitcoinShare.toFixed(8),
            difficulty: DIFFICULTY,
            calculatedAt: new Date()
          });
        }
      }

      if (bulkInsertData.length > 0) {
        await tx.insert(historicalBitcoinCalculations)
          .values(bulkInsertData);

        await log(`Inserted ${bulkInsertData.length} Bitcoin calculation records for ${date} with model ${minerModel}`, "success");
      } else {
        await log(`No records to insert for ${date} with model ${minerModel}`, "warning");
      }
    });
  } catch (error) {
    await log(`Error processing ${date} for ${minerModel}: ${error}`, "error");
    throw error;
  }
}

// Process the Bitcoin calculations for the target date
async function processTargetDate(): Promise<void> {
  try {
    await log(`\n=== Starting Bitcoin Calculations for ${TARGET_DATE} ===\n`, "info");
    
    // First, verify we have curtailment records for this date
    const recordCount = await db
      .select({ 
        count: sql<string>`COUNT(*)` 
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const count = Number(recordCount[0]?.count || "0");
    
    if (count === 0) {
      await log(`No curtailment records found for ${TARGET_DATE}. Cannot proceed with Bitcoin calculations.`, "error");
      return;
    }
    
    await log(`Found ${count} curtailment records for ${TARGET_DATE}. Proceeding with Bitcoin calculations.`, "success");
    
    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      try {
        await processSingleDay(TARGET_DATE, minerModel);
      } catch (error) {
        await log(`Error processing ${TARGET_DATE} with ${minerModel}: ${error}`, "error");
      }
      
      // Add a small delay between processing different miner models
      await delay(2000);
    }
    
    // Verify the calculations were successful
    await log(`\nVerifying Bitcoin calculations for ${TARGET_DATE}...`, "info");
    
    // Check the calculation results
    const verificationResults = await db
      .select({
        minerModel: historicalBitcoinCalculations.minerModel,
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT ${historicalBitcoinCalculations.settlementPeriod})`,
        farmCount: sql<number>`COUNT(DISTINCT ${historicalBitcoinCalculations.farmId})`,
        totalBitcoin: sql<string>`SUM(${historicalBitcoinCalculations.bitcoinMined}::numeric)`
      })
      .from(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
      .groupBy(historicalBitcoinCalculations.minerModel);
    
    if (verificationResults.length === 0) {
      await log(`No Bitcoin calculation results found for ${TARGET_DATE} after processing.`, "error");
    } else {
      await log(`Bitcoin calculation results for ${TARGET_DATE}:`, "success");
      for (const result of verificationResults) {
        await log(`- ${result.minerModel}: ${result.recordCount} records across ${result.periodCount} periods and ${result.farmCount} farms, total: ${Number(result.totalBitcoin).toFixed(8)} BTC`, "success");
      }
    }
    
    // Run manual script to update the monthly summary
    await log(`\nUpdating monthly Bitcoin summary for 2025-03...`, "info");
    
    try {
      const { exec } = require("child_process");
      
      await new Promise<void>((resolve, reject) => {
        exec("npx tsx server/services/bitcoinService.ts --update-monthly 2025-03", (error: Error | null, stdout: string, stderr: string) => {
          if (error) {
            log(`Error updating monthly summary: ${error}`, "error");
            log(stderr, "error");
            reject(error);
            return;
          }
          
          log(stdout, "info");
          resolve();
        });
      });
      
      await log(`Monthly Bitcoin summary updated successfully`, "success");
    } catch (error) {
      await log(`Error updating monthly summary: ${error}`, "error");
    }
    
    await log(`\n=== Bitcoin Calculations Completed for ${TARGET_DATE} ===\n`, "success");
  } catch (error) {
    await log(`Unhandled error during processing: ${error}`, "error");
  }
}

// Run the script
processTargetDate()
  .then(() => {
    console.log(`\nProcess completed. See ${LOG_FILE} for details.`);
    process.exit(0);
  })
  .catch(error => {
    console.error(`\nUnhandled error: ${error}`);
    process.exit(1);
  });
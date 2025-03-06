#!/usr/bin/env tsx
/**
 * Direct Data Reingestion Tool
 * 
 * This script directly calls the necessary functions to reingest data for 2025-03-04
 * and update the Bitcoin calculations without the overhead of the full reingest-data.ts script.
 */

import { processDailyCurtailment } from "./server/services/curtailment";
import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { eq, sql, count } from "drizzle-orm";
import { performance } from "perf_hooks";

// We'll have to write our own Bitcoin processing logic since processSingleDay is not exported
import { getDifficultyData } from "./server/services/dynamodbService";
import { DEFAULT_DIFFICULTY, minerModels } from "./server/types/bitcoin";

// Default configuration
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const TARGET_DATE = '2025-03-04';
const BLOCK_REWARD = 3.125;
const SETTLEMENT_PERIOD_MINUTES = 30;
const BLOCKS_PER_SETTLEMENT_PERIOD = 3;

// Bitcoin calculation function
function calculateBitcoinForBMU(
  curtailedMwh: number,
  minerModel: string,
  difficulty: number
): number {
  // Get miner stats
  const minerStats = minerModels[minerModel];
  if (!minerStats) {
    throw new Error(`Unknown miner model: ${minerModel}`);
  }

  // Convert MWh to kWh
  const kWh = curtailedMwh * 1000;
  
  // Calculate total possible hashes with this energy
  const hashesPerJoule = minerStats.hashrate * 1e12 / (minerStats.power * 3600);
  const totalJoules = kWh * 3.6e6;
  const totalHashes = totalJoules * hashesPerJoule;
  
  // Calculate expected bitcoin
  const hashesPerBlock = difficulty * 2**32;
  const totalBlocks = totalHashes / hashesPerBlock;
  const bitcoinMined = totalBlocks * BLOCK_REWARD;
  
  return bitcoinMined;
}

// Function to process a single day's Bitcoin calculations
async function processSingleDay(
  date: string,
  minerModel: string
): Promise<void> {
  try {
    // Get difficulty for the date
    let difficulty;
    try {
      difficulty = await getDifficultyData(date);
      log(`Fetched difficulty for ${date}: ${difficulty}`, "info");
    } catch (error) {
      difficulty = DEFAULT_DIFFICULTY;
      log(`Using default difficulty for ${date}: ${difficulty}`, "warning");
    }

    return await db.transaction(async (tx) => {
      const curtailmentData = await tx
        .select({
          periods: sql<number[]>`array_agg(DISTINCT settlement_period)`,
          farmIds: sql<string[]>`array_agg(DISTINCT farm_id)`
        })
        .from(curtailmentRecords)
        .where(
          eq(curtailmentRecords.settlementDate, date)
        );

      if (!curtailmentData[0] || !curtailmentData[0].periods || curtailmentData[0].periods.length === 0) {
        log(`No curtailment records for ${date}`, "warning");
        return;
      }

      const periods = curtailmentData[0].periods;
      const farmIds = curtailmentData[0].farmIds;

      // Delete existing calculations for this date and miner model
      await tx.delete(historicalBitcoinCalculations)
        .where(
          eq(historicalBitcoinCalculations.settlementDate, date) &&
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        );

      // Get all curtailment records for this date
      const records = await tx
        .select()
        .from(curtailmentRecords)
        .where(
          eq(curtailmentRecords.settlementDate, date)
        );

      log(`Processing ${date} with difficulty ${difficulty}`, "info");
      log(`Found ${records.length} curtailment records across ${periods.length} periods and ${farmIds.length} farms`, "info");

      // Group records by period
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

      // Prepare data for insertion
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
          difficulty
        );

        for (const [farmId, farmVolume] of data.farms) {
          const bitcoinShare = (periodBitcoin * farmVolume) / data.totalVolume;
          bulkInsertData.push({
            settlementDate: date,
            settlementPeriod: period,
            farmId,
            minerModel,
            bitcoinMined: bitcoinShare.toFixed(8),
            difficulty: difficulty.toString(),
            calculatedAt: new Date()
          });
        }
      }

      if (bulkInsertData.length > 0) {
        await tx.insert(historicalBitcoinCalculations)
          .values(bulkInsertData);

        log(`Inserted ${bulkInsertData.length} records for ${date} ${minerModel}`, "success");
      } else {
        log(`No records to insert for ${date} ${minerModel}`, "warning");
      }
    });
  } catch (error) {
    log(`Error processing ${date} for ${minerModel}: ${error}`, "error");
    throw error;
  }
}

// Helper function for logging
function log(message: string, type: "info" | "success" | "warning" | "error" = "info") {
  const timestamp = new Date().toISOString();
  let prefix = "";
  
  switch (type) {
    case "success":
      prefix = "\x1b[32m✓\x1b[0m "; // Green checkmark
      break;
    case "warning":
      prefix = "\x1b[33m⚠\x1b[0m "; // Yellow warning
      break;
    case "error":
      prefix = "\x1b[31m✗\x1b[0m "; // Red X
      break;
    default:
      prefix = "\x1b[36m•\x1b[0m "; // Blue dot for info
  }
  
  console.log(`${prefix}[${timestamp.split('T')[1].split('.')[0]}] ${message}`);
}

// Main function
async function main() {
  const startTime = performance.now();
  log(`Starting direct data reingestion for ${TARGET_DATE}`, "info");
  
  try {
    // Step 1: Get initial state for comparison
    log("Checking initial state...", "info");
    const initialState = await db
      .select({
        recordCount: count(curtailmentRecords.id),
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    log(`Initial record count: ${initialState[0]?.recordCount || 0}`, "info");
    log(`Initial volume: ${Number(initialState[0]?.totalVolume || 0).toFixed(2)} MWh`, "info");
    log(`Initial payment: £${Number(initialState[0]?.totalPayment || 0).toFixed(2)}`, "info");
    
    // Step 2: Process curtailment data
    log("Reingesting curtailment data from Elexon API...", "info");
    await processDailyCurtailment(TARGET_DATE);
    log("Curtailment data reingestion completed", "success");
    
    // Step 3: Process Bitcoin calculations
    log("Updating Bitcoin calculations...", "info");
    
    for (const minerModel of MINER_MODELS) {
      log(`Processing ${minerModel}...`, "info");
      await processSingleDay(TARGET_DATE, minerModel);
    }
    
    log("Bitcoin calculations completed", "success");
    
    // Step 4: Verify results
    const stats = await db
      .select({
        recordCount: count(curtailmentRecords.id),
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        farmCount: sql<number>`COUNT(DISTINCT farm_id)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    // Get Bitcoin calculation stats
    const bitcoinStats = await Promise.all(
      MINER_MODELS.map(async (model) => {
        const result = await db
          .select({
            totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`
          })
          .from(historicalBitcoinCalculations)
          .where(
            eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE) &&
            eq(historicalBitcoinCalculations.minerModel, model)
          );
        
        return { model, bitcoin: result[0]?.totalBitcoin || "0" };
      })
    );
    
    const endTime = performance.now();
    const duration = ((endTime - startTime) / 1000).toFixed(1);
    
    log(`Reingestion completed in ${duration}s`, "success");
    
    // Print results
    console.log("\n=== Results ===");
    console.log(`Date: ${TARGET_DATE}`);
    console.log(`Records: ${stats[0].recordCount || 0}`);
    console.log(`Periods: ${stats[0].periodCount || 0}`);
    console.log(`Farms: ${stats[0].farmCount || 0}`);
    console.log(`Volume: ${Number(stats[0].totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Payment: £${Number(stats[0].totalPayment || 0).toFixed(2)}`);
    console.log(`Bitcoin mined (S19J_PRO): ${Number(bitcoinStats.find(s => s.model === 'S19J_PRO')?.bitcoin || 0).toFixed(8)}`);
    console.log(`Bitcoin mined (S9): ${Number(bitcoinStats.find(s => s.model === 'S9')?.bitcoin || 0).toFixed(8)}`);
    console.log(`Bitcoin mined (M20S): ${Number(bitcoinStats.find(s => s.model === 'M20S')?.bitcoin || 0).toFixed(8)}`);
    
    process.exit(0);
  } catch (error) {
    log(`Error during data reingestion: ${error}`, "error");
    process.exit(1);
  }
}

// Run the script
main();
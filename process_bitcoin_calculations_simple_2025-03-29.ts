/**
 * Simple Bitcoin Calculations for 2025-03-29
 * 
 * This script processes Bitcoin calculations for March 29, 2025 data
 * using a fixed difficulty value (bypassing DynamoDB).
 * 
 * Usage:
 *   npx tsx process_bitcoin_calculations_simple_2025-03-29.ts
 */

import { db } from './db';
import fs from 'fs';
import path from 'path';
import { format } from 'date-fns';
import { sql, eq, and, desc } from 'drizzle-orm';
import { curtailmentRecords, historicalBitcoinCalculations, bitcoinDailySummaries, bitcoinMonthlySummaries } from './db/schema';

async function log(message: string, level: "info" | "error" | "warning" | "success" = "info"): Promise<void> {
  const timestamp = new Date().toISOString();
  const prefix = level === "info" 
    ? "\x1b[37m[INFO]" 
    : level === "error" 
      ? "\x1b[31m[ERROR]" 
      : level === "warning" 
        ? "\x1b[33m[WARNING]" 
        : "\x1b[32m[SUCCESS]";
  
  console.log(`[${timestamp}] ${prefix} ${message}\x1b[0m`);
  
  // Also log to file
  const logDir = path.join(process.cwd(), 'logs');
  const logFile = path.join(logDir, `process_bitcoin_calculations_simple_2025-03-29.log`);
  
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }
  
  fs.appendFileSync(
    logFile, 
    `[${timestamp}] [${level.toUpperCase()}] ${message}\n`
  );
}

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function calculateBitcoinForBMU(
  volume: number, 
  difficulty: number, 
  minerModel: string
): number {
  // Constants for different miner models (hash rate in TH/s and power in W)
  const MINER_SPECS: Record<string, { hashRate: number; power: number }> = {
    S19J_PRO: { hashRate: 104, power: 3068 },
    S9: { hashRate: 13.5, power: 1323 },
    M20S: { hashRate: 68, power: 3360 }
  };
  
  if (!MINER_SPECS[minerModel]) {
    throw new Error(`Unknown miner model: ${minerModel}`);
  }
  
  const { hashRate, power } = MINER_SPECS[minerModel];
  
  // Convert volume from MWh to Wh and then calculate how many miners can run
  const volumeWh = Math.abs(volume) * 1_000_000;
  const minerCount = volumeWh / power;
  
  // Calculate how much Bitcoin can be mined
  // Formula: (hashRate * minerCount * 3600) / (difficulty * 2^32) * 6.25
  const bitcoinMined = (hashRate * minerCount * 3600) / (difficulty * Math.pow(2, 32)) * 6.25;
  
  return bitcoinMined;
}

async function processSingleDay(date: string, minerModel: string): Promise<void> {
  try {
    // Use the current network difficulty (from March 2025)
    const difficulty = 113757508810853; // Fixed as of March 2025
    
    log(`Processing ${date} with difficulty ${difficulty}`);
    
    // 1. Get all curtailment records for the target date
    const curtailmentData = await db
      .select({
        farmId: curtailmentRecords.farmId,
        settlementPeriod: curtailmentRecords.settlementPeriod,
        volume: curtailmentRecords.volume
      })
      .from(curtailmentRecords)
      .where(sql`${curtailmentRecords.settlementDate} = ${date}`);
    
    // Check if we have data to process
    if (!curtailmentData.length) {
      log(`No curtailment data found for ${date}`, "warning");
      return;
    }
    
    log(`Found ${curtailmentData.length} curtailment records across ${new Set(curtailmentData.map(r => r.settlementPeriod)).size} periods and ${new Set(curtailmentData.map(r => r.farmId)).size} farms`);
    
    // 2. Delete any existing Bitcoin calculations for this date and model
    log(`Deleting existing Bitcoin calculations for ${date} ${minerModel}...`);
    
    try {
      const result = await db.execute(
        sql`DELETE FROM historical_bitcoin_calculations 
            WHERE settlement_date = ${date} 
            AND miner_model = ${minerModel}`
      );
      log(`Deleted existing Bitcoin calculations: ${result}`);
    } catch (error) {
      log(`Error deleting existing calculations: ${error}`, "warning");
    }
    
    // 3. Process each curtailment record
    const calculationRecords = [];
    
    for (const record of curtailmentData) {
      // Skip positive volumes (not curtailed)
      if (Number(record.volume) >= 0) continue;
      
      // Calculate Bitcoin potential
      const bitcoinMined = calculateBitcoinForBMU(
        Number(record.volume), 
        difficulty,
        minerModel
      );
      
      // Add to batch
      calculationRecords.push({
        settlementDate: date,
        settlementPeriod: record.settlementPeriod,
        farmId: record.farmId,
        minerModel: minerModel,
        bitcoinMined: bitcoinMined.toString(),
        difficulty: difficulty.toString(),
        calculatedAt: new Date()
      });
    }
    
    // 4. Insert historical Bitcoin calculations in chunks
    const chunkSize = 100;
    
    if (calculationRecords.length > 0) {
      for (let i = 0; i < calculationRecords.length; i += chunkSize) {
        const chunk = calculationRecords.slice(i, i + chunkSize);
        
        // Use raw SQL insert to avoid type issues
        const values = chunk.map(record => {
          return `(
            '${record.settlementDate}', 
            ${record.settlementPeriod}, 
            '${record.farmId}', 
            '${record.minerModel}', 
            ${record.bitcoinMined}, 
            ${record.difficulty}, 
            '${new Date().toISOString()}'
          )`;
        }).join(',');
        
        await db.execute(sql`
          INSERT INTO historical_bitcoin_calculations 
          (settlement_date, settlement_period, farm_id, miner_model, bitcoin_mined, difficulty, calculated_at)
          VALUES ${sql.raw(values)}
          ON CONFLICT (settlement_date, settlement_period, farm_id, miner_model) 
          DO UPDATE SET 
            bitcoin_mined = EXCLUDED.bitcoin_mined,
            difficulty = EXCLUDED.difficulty,
            calculated_at = EXCLUDED.calculated_at
        `);
      }
    }
    
    log(`Inserted ${calculationRecords.length} records for ${date} ${minerModel}`);
    
    // Get unique periods processed
    const processedPeriods = [...new Set(calculationRecords.map(r => r.settlementPeriod))].sort((a, b) => a - b);
    log(`Processed periods: ${processedPeriods.join(', ')}`);
    
    // 5. Create/Update the daily summary
    const dailyTotal = await db
      .select({
        totalBitcoin: sql`SUM(${historicalBitcoinCalculations.bitcoinMined})`,
        avgDifficulty: sql`AVG(${historicalBitcoinCalculations.difficulty})`
      })
      .from(historicalBitcoinCalculations)
      .where(and(
        sql`${historicalBitcoinCalculations.settlementDate} = ${date}`,
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      ));
    
    const totalBitcoin = Number(dailyTotal[0]?.totalBitcoin || 0);
    const avgDifficulty = Number(dailyTotal[0]?.avgDifficulty || difficulty);
    
    // Check if a daily summary already exists
    const existingSummary = await db
      .select()
      .from(bitcoinDailySummaries)
      .where(and(
        sql`${bitcoinDailySummaries.summaryDate} = ${date}`,
        eq(bitcoinDailySummaries.minerModel, minerModel)
      ))
      .limit(1);
    
    // Update or insert daily summary using UPSERT
    const now = new Date().toISOString();
    await db.execute(sql`
      INSERT INTO bitcoin_daily_summaries 
      (summary_date, miner_model, bitcoin_mined, value_at_mining, average_difficulty, created_at, updated_at)
      VALUES (
        ${date},
        ${minerModel},
        ${totalBitcoin.toString()},
        ${(totalBitcoin * 65000).toString()},
        ${avgDifficulty.toString()},
        ${now},
        ${now}
      )
      ON CONFLICT (summary_date, miner_model) 
      DO UPDATE SET 
        bitcoin_mined = EXCLUDED.bitcoin_mined,
        value_at_mining = EXCLUDED.value_at_mining,
        average_difficulty = EXCLUDED.average_difficulty,
        updated_at = EXCLUDED.updated_at
    `);
    
    log(`Upserted daily summary for ${date} ${minerModel}: ${totalBitcoin.toFixed(8)} BTC`, "success");
    
  } catch (error) {
    log(`Error processing ${date} with ${minerModel}: ${error}`, "error");
    throw error;
  }
}

async function processTargetDate(): Promise<void> {
  try {
    log("=== Starting Bitcoin Calculations for 2025-03-29 ===");
    
    // Process for each miner model
    const date = '2025-03-29';
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const model of minerModels) {
      await processSingleDay(date, model);
      // Add a small delay to prevent database overload
      await delay(500);
    }
    
    // Update monthly summary for March 2025
    log(`Updating monthly Bitcoin summary for 2025-03...`);
    
    for (const model of minerModels) {
      log(`Calculating monthly Bitcoin summary for 2025-03 with ${model}`);
      
      const monthlyTotal = await db
        .select({
          totalBitcoin: sql`SUM(${bitcoinDailySummaries.bitcoinMined})`
        })
        .from(bitcoinDailySummaries)
        .where(and(
          sql`EXTRACT(YEAR FROM ${bitcoinDailySummaries.summaryDate}::DATE) = 2025`,
          sql`EXTRACT(MONTH FROM ${bitcoinDailySummaries.summaryDate}::DATE) = 3`,
          eq(bitcoinDailySummaries.minerModel, model)
        ));
      
      const totalMonthlyBitcoin = Number(monthlyTotal[0]?.totalBitcoin || 0);
      
      // Check if monthly summary exists
      const existingMonthlySummary = await db
        .select()
        .from(bitcoinMonthlySummaries)
        .where(and(
          eq(bitcoinMonthlySummaries.yearMonth, '2025-03'),
          eq(bitcoinMonthlySummaries.minerModel, model)
        ))
        .limit(1);
      
      // Update or insert monthly summary using UPSERT
      const now = new Date().toISOString();
      await db.execute(sql`
        INSERT INTO bitcoin_monthly_summaries 
        (year_month, miner_model, bitcoin_mined, value_at_mining, created_at, updated_at)
        VALUES (
          '2025-03',
          ${model},
          ${totalMonthlyBitcoin.toString()},
          ${(totalMonthlyBitcoin * 65000).toString()},
          ${now},
          ${now}
        )
        ON CONFLICT (year_month, miner_model) 
        DO UPDATE SET 
          bitcoin_mined = EXCLUDED.bitcoin_mined,
          value_at_mining = EXCLUDED.value_at_mining,
          updated_at = EXCLUDED.updated_at
      `);
      
      log(`Upserted monthly summary for 2025-03 with ${model}: ${totalMonthlyBitcoin.toFixed(8)} BTC`, "success");
    }
    
    log("=== Bitcoin Calculations Complete ===", "success");
  } catch (error) {
    log(`Fatal error in Bitcoin calculations: ${error}`, "error");
    throw error;
  }
}

// Run the main function
processTargetDate().catch(error => {
  console.error("Fatal error:", error);
  process.exit(1);
});
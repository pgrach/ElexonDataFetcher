/**
 * Add S19J PRO Calculations for 2025-03-29
 * 
 * This script adds the missing S19J PRO model Bitcoin calculations for March 29, 2025.
 * 
 * Usage:
 *   npx tsx add_s19j_pro_calculations_2025-03-29.ts
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
  const logFile = path.join(logDir, `add_s19j_pro_calculations_2025-03-29.log`);
  
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
  difficulty: number
): number {
  // Constants for S19J PRO
  const hashRate = 104; // TH/s
  const power = 3068; // W
  
  // Convert volume from MWh to Wh and then calculate how many miners can run
  const volumeWh = Math.abs(volume) * 1_000_000;
  const minerCount = volumeWh / power;
  
  // Calculate how much Bitcoin can be mined
  // Formula: (hashRate * minerCount * 3600) / (difficulty * 2^32) * 6.25
  const bitcoinMined = (hashRate * minerCount * 3600) / (difficulty * Math.pow(2, 32)) * 6.25;
  
  return bitcoinMined;
}

async function processS19JPRO(): Promise<void> {
  try {
    const date = '2025-03-29';
    const minerModel = 'S19J_PRO';
    const difficulty = 113757508810853; // Fixed as of March 2025
    
    log(`Processing ${date} with ${minerModel} and difficulty ${difficulty}`);
    
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
    
    // Process records in batches to insert calculations
    const batchSize = 100;
    let processedCount = 0;
    let totalBitcoin = 0;
    
    // Group curtailment records by settlement period and farm
    const recordsByPeriodAndFarm = curtailmentData.reduce((acc, record) => {
      const key = `${record.settlementPeriod}_${record.farmId}`;
      if (!acc[key]) {
        acc[key] = [];
      }
      acc[key].push(record);
      return acc;
    }, {} as Record<string, typeof curtailmentData>);
    
    // Process each unique period-farm combination
    for (const [key, records] of Object.entries(recordsByPeriodAndFarm)) {
      if (records.length === 0) continue;
      
      // Extract settlement period and farm ID
      const [periodStr, farmId] = key.split('_');
      const settlementPeriod = parseInt(periodStr, 10);
      
      // Sum up the volume for this period and farm
      const totalVolume = records.reduce((sum, r) => sum + Number(r.volume), 0);
      
      // Skip positive volumes (not curtailed)
      if (totalVolume >= 0) continue;
      
      // Calculate Bitcoin for this combined record
      const bitcoinMined = calculateBitcoinForBMU(totalVolume, difficulty);
      totalBitcoin += bitcoinMined;
      
      // Insert the calculation record
      try {
        await db.execute(sql`
          INSERT INTO historical_bitcoin_calculations 
          (settlement_date, settlement_period, farm_id, miner_model, bitcoin_mined, difficulty, calculated_at)
          VALUES (
            ${date},
            ${settlementPeriod},
            ${farmId},
            ${minerModel},
            ${bitcoinMined.toString()},
            ${difficulty.toString()},
            ${new Date().toISOString()}
          )
        `);
        
        processedCount++;
        
        if (processedCount % 100 === 0) {
          log(`Processed ${processedCount} records so far...`);
        }
      } catch (error) {
        log(`Error inserting calculation for ${farmId} period ${settlementPeriod}: ${error}`, "warning");
      }
      
      // Add small delay to prevent overloading the database
      if (processedCount % 200 === 0) {
        await delay(100);
      }
    }
    
    log(`Processed ${processedCount} unique farm-period combinations for ${date} ${minerModel}`);
    log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)} BTC`);
    
    // Create/update daily summary
    await db.execute(sql`
      INSERT INTO bitcoin_daily_summaries 
      (summary_date, miner_model, bitcoin_mined, value_at_mining, average_difficulty, created_at, updated_at)
      VALUES (
        ${date},
        ${minerModel},
        ${totalBitcoin.toString()},
        ${(totalBitcoin * 65000).toString()},
        ${difficulty.toString()},
        ${new Date().toISOString()},
        ${new Date().toISOString()}
      )
      ON CONFLICT (summary_date, miner_model) 
      DO UPDATE SET 
        bitcoin_mined = EXCLUDED.bitcoin_mined,
        value_at_mining = EXCLUDED.value_at_mining,
        average_difficulty = EXCLUDED.average_difficulty,
        updated_at = EXCLUDED.updated_at
    `);
    
    log(`Updated daily summary for ${date} ${minerModel}: ${totalBitcoin.toFixed(8)} BTC`, "success");
    
    // Update monthly summary
    // First get all the daily summaries for this month and model
    const monthlySummary = await db
      .select({
        totalBitcoin: sql`SUM(${bitcoinDailySummaries.bitcoinMined})`
      })
      .from(bitcoinDailySummaries)
      .where(and(
        sql`EXTRACT(YEAR FROM ${bitcoinDailySummaries.summaryDate}::DATE) = 2025`,
        sql`EXTRACT(MONTH FROM ${bitcoinDailySummaries.summaryDate}::DATE) = 3`,
        eq(bitcoinDailySummaries.minerModel, minerModel)
      ));
    
    const totalMonthlyBitcoin = Number(monthlySummary[0]?.totalBitcoin || 0);
    
    await db.execute(sql`
      INSERT INTO bitcoin_monthly_summaries 
      (year_month, miner_model, bitcoin_mined, value_at_mining, created_at, updated_at)
      VALUES (
        '2025-03',
        ${minerModel},
        ${totalMonthlyBitcoin.toString()},
        ${(totalMonthlyBitcoin * 65000).toString()},
        ${new Date().toISOString()},
        ${new Date().toISOString()}
      )
      ON CONFLICT (year_month, miner_model) 
      DO UPDATE SET 
        bitcoin_mined = EXCLUDED.bitcoin_mined,
        value_at_mining = EXCLUDED.value_at_mining,
        updated_at = EXCLUDED.updated_at
    `);
    
    log(`Updated monthly summary for 2025-03 ${minerModel}: ${totalMonthlyBitcoin.toFixed(8)} BTC`, "success");
    
  } catch (error) {
    log(`Error processing S19J PRO: ${error}`, "error");
    throw error;
  }
}

// Run the main function
processS19JPRO().then(() => {
  log("=== S19J PRO Calculation Complete ===", "success");
}).catch(error => {
  console.error("Fatal error:", error);
  process.exit(1);
});
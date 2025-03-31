/**
 * Update Bitcoin Summaries
 * 
 * This script updates the daily, monthly, and yearly Bitcoin summaries
 * based on the calculations in historical_bitcoin_calculations.
 * 
 * Usage:
 *   npx tsx update_bitcoin_summaries.ts
 */

import { db } from './db';
import fs from 'fs';
import path from 'path';
import { format } from 'date-fns';
import { sql, eq, and } from 'drizzle-orm';
import { 
  historicalBitcoinCalculations,
  bitcoinMonthlySummaries,
  bitcoinYearlySummaries,
  bitcoinDailySummaries
} from './db/schema';

// Ensure daily bitcoin summaries reference is correct
// The name in the schema might be different from the table name
const tableName = 'bitcoin_daily_summaries';

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
  const logFile = path.join(logDir, `bitcoin_summaries_${format(new Date(), 'yyyy-MM-dd')}.log`);
  
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }
  
  fs.appendFileSync(
    logFile, 
    `[${timestamp}] [${level.toUpperCase()}] ${message}\n`
  );
}

async function updateDailySummary(date: string): Promise<void> {
  try {
    log(`Updating daily Bitcoin summary for ${date}...`);

    // Delete existing summaries for this date to avoid duplicates
    await db.execute(sql`DELETE FROM ${sql.identifier(tableName)} WHERE summary_date = ${date}`);

    // Generate daily summaries for each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      // Get the total Bitcoin mined for this date and miner model
      const result = await db
        .select({
          totalBitcoin: sql`SUM(${historicalBitcoinCalculations.bitcoinMined})`.as('total_bitcoin'),
          avgDifficulty: sql`AVG(${historicalBitcoinCalculations.difficulty})`.as('avg_difficulty')
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            sql`${historicalBitcoinCalculations.settlementDate}::text = ${date}`,
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );

      if (result.length > 0 && result[0].totalBitcoin) {
        // Insert the summary
        await db.execute(sql`
          INSERT INTO ${sql.identifier(tableName)} (
            summary_date, 
            miner_model, 
            bitcoin_mined, 
            value_at_mining,
            average_difficulty,
            created_at,
            updated_at
          )
          VALUES (
            ${date},
            ${minerModel},
            ${result[0].totalBitcoin},
            0,
            ${result[0].avgDifficulty || 0},
            NOW(),
            NOW()
          )
        `);
        
        log(`Created daily summary for ${date} with miner model ${minerModel}: ${result[0].totalBitcoin} BTC`, "success");
      } else {
        log(`No data found for ${date} with miner model ${minerModel}`, "warning");
      }
    }
  } catch (error) {
    log(`Error updating daily summary: ${error}`, "error");
    throw error;
  }
}

async function updateMonthlySummary(yearMonth: string): Promise<void> {
  try {
    const [year, month] = yearMonth.split('-');
    log(`Updating monthly Bitcoin summary for ${yearMonth}...`);

    // Delete existing summaries for this month to avoid duplicates
    await db.delete(bitcoinMonthlySummaries)
      .where(eq(bitcoinMonthlySummaries.yearMonth, yearMonth));

    // Generate monthly summaries for each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      // Get the total Bitcoin mined for this month and miner model
      const result = await db
        .select({
          totalBitcoin: sql`SUM(${historicalBitcoinCalculations.bitcoinMined})`.as('total_bitcoin'),
          avgDifficulty: sql`AVG(${historicalBitcoinCalculations.difficulty})`.as('avg_difficulty')
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            sql`EXTRACT(YEAR FROM ${historicalBitcoinCalculations.settlementDate}) = ${parseInt(year)}`,
            sql`EXTRACT(MONTH FROM ${historicalBitcoinCalculations.settlementDate}) = ${parseInt(month)}`,
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );

      if (result.length > 0 && result[0].totalBitcoin) {
        // Insert the summary
        await db.execute(sql`
          INSERT INTO bitcoin_monthly_summaries (
            year_month, 
            miner_model, 
            bitcoin_mined, 
            value_at_mining,
            created_at,
            updated_at
          )
          VALUES (
            ${yearMonth},
            ${minerModel},
            ${result[0].totalBitcoin},
            0,
            NOW(),
            NOW()
          )
        `);
        
        log(`Created monthly summary for ${yearMonth} with miner model ${minerModel}: ${result[0].totalBitcoin} BTC`, "success");
      } else {
        log(`No data found for ${yearMonth} with miner model ${minerModel}`, "warning");
      }
    }
  } catch (error) {
    log(`Error updating monthly summary: ${error}`, "error");
    throw error;
  }
}

async function updateYearlySummary(year: string): Promise<void> {
  try {
    log(`Updating yearly Bitcoin summary for ${year}...`);

    // Delete existing summaries for this year to avoid duplicates
    await db.delete(bitcoinYearlySummaries)
      .where(eq(bitcoinYearlySummaries.year, year));

    // Generate yearly summaries for each miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      // Get the total Bitcoin mined for this year and miner model
      const result = await db
        .select({
          totalBitcoin: sql`SUM(${historicalBitcoinCalculations.bitcoinMined})`.as('total_bitcoin'),
          avgDifficulty: sql`AVG(${historicalBitcoinCalculations.difficulty})`.as('avg_difficulty')
        })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            sql`EXTRACT(YEAR FROM ${historicalBitcoinCalculations.settlementDate}) = ${parseInt(year)}`,
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );

      if (result.length > 0 && result[0].totalBitcoin) {
        // Insert the summary
        await db.execute(sql`
          INSERT INTO bitcoin_yearly_summaries (
            year, 
            miner_model, 
            bitcoin_mined, 
            value_at_mining,
            created_at,
            updated_at
          )
          VALUES (
            ${year},
            ${minerModel},
            ${result[0].totalBitcoin},
            0,
            NOW(),
            NOW()
          )
        `);
        
        log(`Created yearly summary for ${year} with miner model ${minerModel}: ${result[0].totalBitcoin} BTC`, "success");
      } else {
        log(`No data found for ${year} with miner model ${minerModel}`, "warning");
      }
    }
  } catch (error) {
    log(`Error updating yearly summary: ${error}`, "error");
    throw error;
  }
}

async function main(): Promise<void> {
  try {
    log("=== Starting Bitcoin Summary Updates ===");
    
    // Update daily summary for March 29, 2025
    await updateDailySummary('2025-03-29');
    
    // Update monthly summary for March 2025
    await updateMonthlySummary('2025-03');
    
    // Update yearly summary for 2025
    await updateYearlySummary('2025');
    
    log("=== Bitcoin Summary Updates Completed ===", "success");
  } catch (error) {
    log(`Error in main process: ${error}`, "error");
  }
}

// Run the main function
main().catch(error => {
  console.error("Fatal error:", error);
  process.exit(1);
});
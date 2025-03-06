/**
 * Bitcoin Calculation Processor for 2025-03-04
 * 
 * This script processes Bitcoin calculations for all settlement periods on 2025-03-04,
 * ensuring calculations exist for all three miner models (S19J_PRO, S9, M20S).
 * 
 * Usage:
 *   npx tsx process_bitcoin_calculations.ts
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { processSingleDay } from "./server/services/bitcoinService";
import { eq, count, sql } from "drizzle-orm";

// Configuration
const DATE = "2025-03-04";
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

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
  // Delete all Bitcoin calculations for the date
  const deletedCalculations = await db
    .delete(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, DATE))
    .returning();
  
  log(`Cleared ${deletedCalculations.length} existing Bitcoin calculation records`, "info");
}

async function getAvailablePeriods(): Promise<number[]> {
  // Get all settlement periods that have curtailment data
  const result = await db
    .select({ period: curtailmentRecords.settlementPeriod })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
  
  return result.map(r => r.period);
}

async function processAllBitcoinCalculations(): Promise<void> {
  log(`Processing Bitcoin calculations for ${DATE}`, "title");
  
  try {
    // Step 1: Check if we have all curtailment data
    const curtailmentCount = await db
      .select({
        recordCount: count(),
        uniquePeriods: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalEnergy: sql<string>`SUM(ABS(volume::numeric))`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE));
    
    const uniquePeriods = curtailmentCount[0]?.uniquePeriods || 0;
    const totalRecords = curtailmentCount[0]?.recordCount || 0;
    const totalEnergy = Number(curtailmentCount[0]?.totalEnergy || 0).toFixed(2);
    
    log(`Found ${totalRecords} curtailment records across ${uniquePeriods} periods (Total: ${totalEnergy} MWh)`, "info");
    
    if (uniquePeriods < 48) {
      log(`Warning: Only ${uniquePeriods}/48 periods have curtailment data!`, "warning");
      const availablePeriods = await getAvailablePeriods();
      log(`Available periods: ${availablePeriods.join(', ')}`, "info");
    }
    
    // Step 2: Clear existing Bitcoin calculations to avoid duplicates
    await clearExistingBitcoinCalculations();
    
    // Step 3: Process Bitcoin calculations for each miner model
    log(`Processing Bitcoin calculations for all miner models...`, "info");
    
    for (const minerModel of MINER_MODELS) {
      log(`Processing calculations for ${minerModel}...`, "info");
      await processSingleDay(DATE, minerModel);
      log(`Completed ${minerModel} calculations`, "success");
    }
    
    // Step 4: Verify results
    const calculationResults = await Promise.all(
      MINER_MODELS.map(async (model) => {
        const result = await db
          .select({
            recordCount: count(),
            uniquePeriods: sql<number>`COUNT(DISTINCT settlement_period)`,
            totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`
          })
          .from(historicalBitcoinCalculations)
          .where(
            eq(historicalBitcoinCalculations.settlementDate, DATE),
            eq(historicalBitcoinCalculations.minerModel, model)
          );
        
        return {
          model,
          records: result[0]?.recordCount || 0,
          periods: result[0]?.uniquePeriods || 0,
          bitcoin: Number(result[0]?.totalBitcoin || 0).toFixed(8)
        };
      })
    );
    
    log(`Bitcoin calculation results:`, "title");
    calculationResults.forEach(result => {
      log(`${result.model}: ${result.records} records across ${result.periods} periods, total: ${result.bitcoin} BTC`, 
        result.periods === 48 ? "success" : "warning");
    });
    
    // Step 5: Check for any missing periods
    const periodsWithMissingCalculations = await Promise.all(
      MINER_MODELS.map(async (model) => {
        const result = await db.execute(sql`
          WITH all_periods AS (
              SELECT generate_series(1, 48) AS period
          )
          SELECT 
              a.period
          FROM 
              all_periods a
          LEFT JOIN (
              SELECT DISTINCT settlement_period 
              FROM historical_bitcoin_calculations 
              WHERE settlement_date = ${DATE} AND miner_model = ${model}
          ) c ON a.period = c.settlement_period
          WHERE 
              c.settlement_period IS NULL
          ORDER BY 
              a.period
        `);
        
        return { model, missingPeriods: result.map(r => r.period) };
      })
    );
    
    // Check for any models with missing periods
    const modelsWithMissingPeriods = periodsWithMissingCalculations.filter(m => m.missingPeriods.length > 0);
    
    if (modelsWithMissingPeriods.length > 0) {
      log(`Warning: Some miner models have missing period calculations:`, "warning");
      modelsWithMissingPeriods.forEach(m => {
        log(`- ${m.model}: Missing periods: ${m.missingPeriods.join(', ')}`, "warning");
      });
    } else {
      log(`All Bitcoin calculations complete for all 48 periods!`, "success");
    }
    
  } catch (error) {
    log(`Error processing Bitcoin calculations: ${error}`, "error");
    process.exit(1);
  }
}

// Run the process
processAllBitcoinCalculations();
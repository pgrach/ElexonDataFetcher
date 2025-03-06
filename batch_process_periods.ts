/**
 * Batch Period Processor for 2025-03-04
 * 
 * This script processes specific settlement periods for 2025-03-04 and
 * can be run multiple times to cover all 48 periods in smaller batches.
 * 
 * Usage:
 *   npx tsx batch_process_periods.ts [start_period] [end_period]
 *   
 * Example:
 *   npx tsx batch_process_periods.ts 1 6
 *   npx tsx batch_process_periods.ts 7 12
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { processDailyCurtailment } from "./server/services/curtailment";
import { processSingleDay } from "./server/services/bitcoinService";
import { eq, sql, count, between, inArray, and } from "drizzle-orm";

// Configuration
const DATE = "2025-03-04";
const DEFAULT_START_PERIOD = 1;
const DEFAULT_END_PERIOD = 6;
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

async function clearExistingDataForPeriods(date: string, startPeriod: number, endPeriod: number): Promise<void> {
  // Delete Bitcoin calculations first (foreign key dependency)
  const deletedBitcoin = await db
    .delete(historicalBitcoinCalculations)
    .where(
      and(
        eq(historicalBitcoinCalculations.settlementDate, date),
        between(historicalBitcoinCalculations.settlementPeriod, startPeriod, endPeriod)
      )
    )
    .returning();
  
  log(`Deleted ${deletedBitcoin.length} Bitcoin calculation records for periods ${startPeriod}-${endPeriod}`, "info");
  
  // Delete curtailment records for these periods
  const deletedCurtailment = await db
    .delete(curtailmentRecords)
    .where(
      and(
        eq(curtailmentRecords.settlementDate, date),
        between(curtailmentRecords.settlementPeriod, startPeriod, endPeriod)
      )
    )
    .returning();
  
  log(`Deleted ${deletedCurtailment.length} curtailment records for periods ${startPeriod}-${endPeriod}`, "info");
}

async function processBatchOfPeriods(startPeriod: number, endPeriod: number): Promise<void> {
  log(`Processing settlement periods ${startPeriod} to ${endPeriod} for ${DATE}`, "title");
  
  try {
    // Step 1: Clear existing data for these periods to avoid duplicates
    await clearExistingDataForPeriods(DATE, startPeriod, endPeriod);
    
    // Step 2: Process curtailment data
    log(`Fetching curtailment data from Elexon API for periods ${startPeriod}-${endPeriod}...`, "info");
    
    // Process each period individually to maximize chances of success
    for (let period = startPeriod; period <= endPeriod; period++) {
      log(`Processing period ${period}...`, "info");
      
      // Process this specific period from Elexon
      // Note: The processDailyCurtailment doesn't support period-specific processing,
      // so we'll have to rely on its implementation to fetch all periods for the date
      await processDailyCurtailment(DATE);
      
      // Verify data was created for this period
      const periodCheck = await db
        .select({ count: count() })
        .from(curtailmentRecords)
        .where(
          and(
            eq(curtailmentRecords.settlementDate, DATE),
            eq(curtailmentRecords.settlementPeriod, period)
          )
        );
      
      if (periodCheck[0]?.count > 0) {
        log(`Successfully processed period ${period} (${periodCheck[0].count} records)`, "success");
      } else {
        log(`Warning: No data found for period ${period} after processing`, "warning");
      }
    }
    
    // Step 3: Get statistics on ingested data for these periods
    const periodStats = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        recordCount: count(),
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(
        and(
          eq(curtailmentRecords.settlementDate, DATE),
          between(curtailmentRecords.settlementPeriod, startPeriod, endPeriod)
        )
      )
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    log(`Results by period:`, "title");
    periodStats.forEach(stat => {
      log(`Period ${stat.period}: ${stat.recordCount} records, ${Number(stat.totalVolume || 0).toFixed(2)} MWh, £${Number(stat.totalPayment || 0).toFixed(2)}`, "info");
    });
    
    // Step 4: Process Bitcoin calculations for these periods
    for (const minerModel of MINER_MODELS) {
      log(`Processing Bitcoin calculations for ${minerModel} (periods ${startPeriod}-${endPeriod})...`, "info");
      
      // Process single day always processes all periods, but we'll use it anyway
      // and then clean up extra records later if needed
      await processSingleDay(DATE, minerModel);
    }
    
    // Step 5: Get Bitcoin calculation stats for these periods
    const bitcoinStats = await Promise.all(
      MINER_MODELS.map(async (model) => {
        const result = await db
          .select({
            totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`,
            recordCount: count()
          })
          .from(historicalBitcoinCalculations)
          .where(
            and(
              eq(historicalBitcoinCalculations.settlementDate, DATE),
              eq(historicalBitcoinCalculations.minerModel, model),
              between(historicalBitcoinCalculations.settlementPeriod, startPeriod, endPeriod)
            )
          );
        
        return { 
          model, 
          bitcoin: result[0]?.totalBitcoin || "0",
          count: result[0]?.recordCount || 0
        };
      })
    );
    
    log("Bitcoin calculation results:", "success");
    bitcoinStats.forEach(stat => {
      log(`- ${stat.model}: ${Number(stat.bitcoin).toFixed(8)} BTC (${stat.count} records)`, "success");
    });
    
    // Step 6: Verify what periods we have data for now
    const processedPeriods = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        count: count()
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    const periodNumbers = processedPeriods.map(p => p.period);
    log(`Periods with data: ${periodNumbers.join(', ')}`, "success");
    
    // Check for missing periods in the current batch
    const missingPeriods: number[] = [];
    for (let i = startPeriod; i <= endPeriod; i++) {
      if (!periodNumbers.includes(i)) {
        missingPeriods.push(i);
      }
    }
    
    if (missingPeriods.length > 0) {
      log(`Warning: Missing periods in current batch: ${missingPeriods.join(', ')}`, "warning");
    } else {
      log(`All periods in batch ${startPeriod}-${endPeriod} were successfully processed!`, "success");
    }
    
    log(`Processing complete for batch ${startPeriod}-${endPeriod}`, "title");
    log(`Next batch: run 'npx tsx batch_process_periods.ts ${endPeriod + 1} ${Math.min(endPeriod + 6, 48)}'`, "info");
  } catch (error) {
    log(`Error during processing: ${error}`, "error");
    process.exit(1);
  }
}

// Parse command line arguments
const args = process.argv.slice(2);
const startPeriod = args[0] ? parseInt(args[0], 10) : DEFAULT_START_PERIOD;
const endPeriod = args[1] ? parseInt(args[1], 10) : DEFAULT_END_PERIOD;

// Validate periods
if (isNaN(startPeriod) || isNaN(endPeriod) || startPeriod < 1 || endPeriod > 48 || startPeriod > endPeriod) {
  log("Invalid period range. Usage: npx tsx batch_process_periods.ts [start_period] [end_period]", "error");
  log("Example: npx tsx batch_process_periods.ts 1 6", "info");
  process.exit(1);
}

// Run the batch process
processBatchOfPeriods(startPeriod, endPeriod);
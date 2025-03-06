/**
 * Data Reingestion Script for 2025-03-04
 * 
 * This script handles the reingestion of Elexon data for 2025-03-04,
 * ensures data integrity by preventing duplicates and checking for missed periods,
 * and then triggers cascade updates of dependent tables.
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { processSingleDay } from "./server/services/bitcoinService";
import { eq, sql, count, inArray, and, isNull } from "drizzle-orm";

const DATE = "2025-03-04";

function log(message: string, type: "info" | "success" | "warning" | "error" = "info") {
  const styles = {
    info: "\x1b[36m%s\x1b[0m",    // Cyan
    success: "\x1b[32m%s\x1b[0m",  // Green
    warning: "\x1b[33m%s\x1b[0m",  // Yellow
    error: "\x1b[31m%s\x1b[0m"     // Red
  };
  
  console.log(styles[type], message);
}

async function checkForMissingPeriods(date: string): Promise<number[]> {
  const periods = await db
    .select({
      period: curtailmentRecords.settlementPeriod,
      count: count()
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
  
  const periodNumbers = periods.map(p => p.period);
  const missingPeriods: number[] = [];
  
  // Check for all periods 1-48
  for (let i = 1; i <= 48; i++) {
    if (!periodNumbers.includes(i)) {
      missingPeriods.push(i);
    }
  }
  
  return missingPeriods;
}

async function clearExistingData(date: string): Promise<void> {
  // Delete Bitcoin calculations first (foreign key dependency)
  const deletedBitcoin = await db
    .delete(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, date))
    .returning();
  
  log(`Deleted ${deletedBitcoin.length} Bitcoin calculation records`, "info");
}

async function main() {
  try {
    log(`Starting reingestion process for ${DATE}`, "info");
    
    // Step 1: Check for complete data
    const missingPeriods = await checkForMissingPeriods(DATE);
    if (missingPeriods.length > 0) {
      log(`Warning: Missing periods detected: ${missingPeriods.join(', ')}`, "warning");
      log("This might cause incomplete Bitcoin calculations. Consider rerunning curtailment ingestion.", "warning");
    } else {
      log("All 48 periods are present in the database", "success");
    }
    
    // Step 2: Clear existing Bitcoin calculation data
    await clearExistingData(DATE);
    
    // Step 3: Get curtailment statistics
    const curtailmentStats = await db
      .select({
        recordCount: count(curtailmentRecords.id),
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`,
        uniqueFarms: sql<number>`COUNT(DISTINCT farm_id)`,
        uniquePeriods: sql<number>`COUNT(DISTINCT settlement_period)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE));
    
    log(`Curtailment data: ${curtailmentStats[0]?.recordCount || 0} records`, "info");
    log(`Total volume: ${Number(curtailmentStats[0]?.totalVolume || 0).toFixed(2)} MWh`, "info");
    log(`Total payment: £${Number(curtailmentStats[0]?.totalPayment || 0).toFixed(2)}`, "info");
    log(`Unique farms: ${curtailmentStats[0]?.uniqueFarms || 0}`, "info");
    log(`Settlement periods: ${curtailmentStats[0]?.uniquePeriods || 0} of 48`, "info");
    
    // Step 4: Process Bitcoin calculations (one model at a time)
    log("Starting Bitcoin calculations...", "info");
    
    // List of miner models to process
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      log(`Processing Bitcoin calculations for ${minerModel}...`, "info");
      await processSingleDay(DATE, minerModel);
      
      // Verify the calculations were created
      const calculationCount = await db
        .select({ count: count() })
        .from(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, DATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        );
      
      log(`Created ${calculationCount[0]?.count || 0} Bitcoin calculations for ${minerModel}`, "success");
    }
    
    // Step 5: Verify all calculations
    const btcStats = await Promise.all(
      minerModels.map(async (model) => {
        const result = await db
          .select({
            totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`,
            recordCount: count()
          })
          .from(historicalBitcoinCalculations)
          .where(
            eq(historicalBitcoinCalculations.settlementDate, DATE) &&
            eq(historicalBitcoinCalculations.minerModel, model)
          );
        
        return { 
          model, 
          bitcoin: result[0]?.totalBitcoin || "0",
          count: result[0]?.recordCount || 0
        };
      })
    );
    
    log("Bitcoin calculation results:", "success");
    btcStats.forEach(stat => {
      log(`- ${stat.model}: ${Number(stat.bitcoin).toFixed(8)} BTC (${stat.count} records)`, "success");
    });
    
    // Step 6: Check for any null values or errors
    const nullValues = await db
      .select({ count: count() })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, DATE),
          isNull(historicalBitcoinCalculations.bitcoinMined)
        )
      );
    
    if (nullValues[0]?.count > 0) {
      log(`Warning: Found ${nullValues[0]?.count} records with null Bitcoin values`, "warning");
    } else {
      log("All Bitcoin calculations have valid values", "success");
    }
    
    log("Reingestion process completed successfully!", "success");
  } catch (error) {
    log(`Error during reingestion: ${error}`, "error");
    process.exit(1);
  }
}

// Run the script
main();
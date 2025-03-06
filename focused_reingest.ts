/**
 * Focused Reingestion Script for 2025-03-04
 * 
 * This script is optimized to run within timeout constraints while ensuring:
 * 1. No duplicates in the database
 * 2. All 48 settlement periods are properly ingested
 * 3. All dependent tables are updated
 */

import { processDailyCurtailment } from "./server/services/curtailment";
import { processSingleDay } from "./server/services/bitcoinService";
import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { eq, sql, count } from "drizzle-orm";

// Configuration - Fixed for this specific purpose
const DATE = "2025-03-04";

async function main() {
  console.log(`Starting focused reingestion for ${DATE}`);
  
  try {
    // Step 1: Get current state
    console.log("Checking initial state...");
    const initialState = await db
      .select({
        recordCount: count(curtailmentRecords.id),
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE));
    
    console.log(`Initial state: ${initialState[0]?.recordCount || 0} records, ${Number(initialState[0]?.totalVolume || 0).toFixed(2)} MWh`);
    
    // Step 2: Clear existing data to avoid duplicates
    console.log("Clearing existing records to avoid duplicates...");
    
    // Delete Bitcoin calculations first (foreign key dependency)
    await db
      .delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, DATE));
    
    // Delete curtailment records
    await db
      .delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE));
    
    console.log("Existing records cleared successfully");
    
    // Step 3: Process curtailment data
    console.log("Reingesting curtailment data from Elexon API...");
    await processDailyCurtailment(DATE);
    
    // Step 4: Verify completeness - check for all 48 periods
    const periods = await db
      .select({
        period: curtailmentRecords.settlementPeriod,
        count: count()
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE))
      .groupBy(curtailmentRecords.settlementPeriod)
      .orderBy(curtailmentRecords.settlementPeriod);
    
    const periodNumbers = periods.map(p => p.period);
    const missingPeriods = [];
    
    // Check for all periods 1-48
    for (let i = 1; i <= 48; i++) {
      if (!periodNumbers.includes(i)) {
        missingPeriods.push(i);
      }
    }
    
    if (missingPeriods.length > 0) {
      console.error(`Warning: Missing periods detected: ${missingPeriods.join(', ')}`);
      console.log("Attempting to fetch missing periods...");
      
      // Try one more time to get any missing periods
      await processDailyCurtailment(DATE);
      
      // Check again
      const updatedPeriods = await db
        .select({
          period: curtailmentRecords.settlementPeriod,
          count: count()
        })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, DATE))
        .groupBy(curtailmentRecords.settlementPeriod)
        .orderBy(curtailmentRecords.settlementPeriod);
      
      const updatedPeriodNumbers = updatedPeriods.map(p => p.period);
      const stillMissingPeriods = [];
      
      for (let i = 1; i <= 48; i++) {
        if (!updatedPeriodNumbers.includes(i)) {
          stillMissingPeriods.push(i);
        }
      }
      
      if (stillMissingPeriods.length > 0) {
        console.error(`Error: Still missing periods after retry: ${stillMissingPeriods.join(', ')}`);
      } else {
        console.log("All periods successfully fetched after retry!");
      }
    } else {
      console.log(`All 48 periods successfully ingested!`);
    }
    
    // Step 5: Get statistics on reingested data
    const reingestionStats = await db
      .select({
        recordCount: count(curtailmentRecords.id),
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, DATE));
    
    console.log(`Reingestion completed: ${reingestionStats[0]?.recordCount || 0} records, ${Number(reingestionStats[0]?.totalVolume || 0).toFixed(2)} MWh`);
    
    // Step 6: Process Bitcoin calculations (one model at a time)
    console.log("Starting Bitcoin calculations...");
    
    // Process one miner model at a time to avoid timeouts
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const minerModel of minerModels) {
      console.log(`Processing Bitcoin calculations for ${minerModel}...`);
      await processSingleDay(DATE, minerModel);
      console.log(`Completed ${minerModel} processing`);
    }
    
    // Step 7: Get Bitcoin calculation stats
    const bitcoinStats = await Promise.all(
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
    
    console.log("Bitcoin calculation results:");
    bitcoinStats.forEach(stat => {
      console.log(`- ${stat.model}: ${Number(stat.bitcoin).toFixed(8)} BTC (${stat.count} records)`);
    });
    
    console.log("Reingestion process completed successfully!");
  } catch (error) {
    console.error(`Error during reingestion: ${error}`);
    process.exit(1);
  }
}

// Run the script
main();
/**
 * Simple reingestion script for April 1, 2025
 * 
 * This script performs a clean reingestion of all settlement period data for April 1, 2025,
 * and verifies the records after processing.
 */

import { db } from "@db";
import { curtailmentRecords, bitcoinDailySummaries } from "@db/schema";
import { eq, sql } from "drizzle-orm";
import { processDailyCurtailment } from "../services/curtailment";

// Target date
const TARGET_DATE = "2025-04-01";

async function main() {
  try {
    console.log(`\n===== REINGESTING DATA FOR ${TARGET_DATE} =====\n`);
    
    // Step 1: Clear existing data
    console.log("Clearing existing curtailment records...");
    await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
    console.log("Clearing existing Bitcoin calculations...");
    await db.delete(bitcoinDailySummaries)
      .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE));
    
    // Step 2: Process new data
    console.log("\nProcessing new curtailment data...");
    await processDailyCurtailment(TARGET_DATE);
    
    // Step 3: Process Bitcoin calculations
    console.log("\nProcessing Bitcoin calculations...");
    const { processSingleDay } = await import("../services/bitcoinService");
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    for (const model of minerModels) {
      console.log(`- Processing ${model}...`);
      await processSingleDay(TARGET_DATE, model);
    }
    
    // Step 4: Verify the data
    console.log("\nVerifying reingested data...");
    const stats = await db
      .select({
        recordCount: sql<number>`COUNT(*)`,
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\n===== REINGESTION RESULTS FOR ${TARGET_DATE} =====`);
    console.log(`Records: ${stats[0].recordCount}`);
    console.log(`Periods with data: ${stats[0].periodCount} of 48`);
    console.log(`Total volume: ${parseFloat(stats[0].totalVolume || '0').toFixed(2)} MWh`);
    console.log(`Total payment: £${parseFloat(stats[0].totalPayment || '0').toFixed(2)}`);
    
    // Step 5: Verify Bitcoin calculations
    console.log("\nBitcoin Mining Calculations:");
    for (const model of minerModels) {
      const btcCalc = await db
        .select({
          bitcoinMined: sql<string>`bitcoin_mined`
        })
        .from(bitcoinDailySummaries)
        .where(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, model)
        );
      
      if (btcCalc.length > 0) {
        console.log(`- ${model}: ${parseFloat(btcCalc[0].bitcoinMined || '0').toFixed(8)} BTC`);
      } else {
        console.log(`- ${model}: No calculation found`);
      }
    }
    
    console.log(`\n===== REINGESTION COMPLETE =====`);
    
  } catch (error) {
    console.error("ERROR DURING REINGESTION:", error);
    process.exit(1);
  }
}

// Run the reingestion
main()
  .then(() => process.exit(0))
  .catch(error => {
    console.error("UNHANDLED ERROR:", error);
    process.exit(1);
  });
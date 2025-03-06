/**
 * Simple script to process a specific date
 * This script uses the direct functions from the services rather than the CLI tool
 */

import { processDailyCurtailment } from "../server/services/curtailment";
import { processSingleDay } from "../server/services/bitcoinService";
import { db } from "../db";
import { curtailmentRecords, historicalBitcoinCalculations } from "../db/schema";
import { eq, sql, count } from "drizzle-orm";
import path from "path";

const date = "2025-03-04";
const minerModels = ["S19J_PRO", "S9", "M20S"];

async function process() {
  console.log(`Starting to process ${date}...`);
  
  try {
    // Step 1: Get initial state for comparison
    console.log("Checking initial state...");
    const initialState = await db
      .select({
        recordCount: count(curtailmentRecords.id),
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    console.log(`Initial record count: ${initialState[0]?.recordCount || 0}`);
    console.log(`Initial volume: ${Number(initialState[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Initial payment: £${Number(initialState[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Step 2: Process curtailment data
    console.log("Processing curtailment data from Elexon API...");
    await processDailyCurtailment(date);
    console.log("Curtailment data processing completed");
    
    // Check and log the current state after curtailment processing
    const midState = await db
      .select({
        recordCount: count(curtailmentRecords.id),
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    console.log(`Current record count: ${midState[0]?.recordCount || 0}`);
    console.log(`Current volume: ${Number(midState[0]?.totalVolume || 0).toFixed(2)} MWh`);
    console.log(`Current payment: £${Number(midState[0]?.totalPayment || 0).toFixed(2)}`);
    
    // Step 3: Process Bitcoin calculations
    console.log("Updating Bitcoin calculations...");
    
    for (const minerModel of minerModels) {
      console.log(`Processing ${minerModel}...`);
      await processSingleDay(date, minerModel);
      console.log(`Completed ${minerModel} processing`);
    }
    
    console.log("Bitcoin calculations completed");
    
    console.log("Process completed successfully!");
    
    // Exit with success
    process.exit(0);
  } catch (error) {
    console.error(`Error during processing: ${error}`);
    process.exit(1);
  }
}

// Run the script
process();
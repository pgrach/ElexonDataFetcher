/**
 * Quick verification script for 2023 data
 * This script checks whether the data in the database matches what our reconciliation script reports
 */

import { db } from "./db";
import { curtailmentRecords, historicalBitcoinCalculations } from "./db/schema";
import { eq, and, sql } from "drizzle-orm";

const DATES_TO_CHECK = [
  '2023-01-03',
  '2023-02-01', 
  '2023-03-01',
  '2023-04-03',
  '2023-05-04',
  '2023-08-01',
  '2023-11-02'
];

const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function checkDate(date: string) {
  console.log(`\n=== Checking ${date} ===`);
  
  // Get curtailment data
  const curtailmentData = await db
    .select({
      count: sql<number>`COUNT(*)`,
      periods: sql<number>`COUNT(DISTINCT settlement_period)`,
      farms: sql<number>`COUNT(DISTINCT farm_id)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  console.log(`Curtailment records: ${curtailmentData[0]?.count || 0}`);
  console.log(`Unique periods: ${curtailmentData[0]?.periods || 0}`);
  console.log(`Unique farms: ${curtailmentData[0]?.farms || 0}`);
  
  // Get bitcoin calculation data for each miner model
  for (const model of MINER_MODELS) {
    const calculationData = await db
      .select({
        count: sql<number>`COUNT(*)`,
        periods: sql<number>`COUNT(DISTINCT settlement_period)`,
        farms: sql<number>`COUNT(DISTINCT farm_id)`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          eq(historicalBitcoinCalculations.settlementDate, date),
          eq(historicalBitcoinCalculations.minerModel, model)
        )
      );
    
    console.log(`${model} calculations: ${calculationData[0]?.count || 0}`);
    console.log(`${model} periods: ${calculationData[0]?.periods || 0}`);
    console.log(`${model} farms: ${calculationData[0]?.farms || 0}`);
  }
}

async function main() {
  console.log("=== 2023 Data Verification ===");
  
  for (const date of DATES_TO_CHECK) {
    await checkDate(date);
  }
  
  console.log("\n=== Verification Complete ===");
  process.exit(0);
}

main().catch(err => {
  console.error("Error during verification:", err);
  process.exit(1);
});
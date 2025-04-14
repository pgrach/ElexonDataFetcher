/**
 * Quick Bitcoin Calculations for 2025-04-13
 * 
 * This script focuses specifically on processing Bitcoin calculations for April 13, 2025
 * with a more targeted and simplified approach.
 */

import { db } from "./db";
import { 
  curtailmentRecords, 
  historicalBitcoinCalculations,
  bitcoinDailySummaries
} from "./db/schema";
import { eq, and, sql, desc } from "drizzle-orm";

// Target date for reprocessing
const TARGET_DATE = "2025-04-13";
// Miner models
const MINER_MODELS = ["S19J_PRO", "M20S", "S9"];

/**
 * Calculate Bitcoin potential for a specific date and miner model
 */
async function calculateBitcoinPotential(date: string, minerModel: string): Promise<number> {
  // Get the total curtailed energy for the date
  const energyQuery = await db
    .select({ 
      totalEnergy: sql<string>`SUM(${curtailmentRecords.volume})` 
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));

  if (!energyQuery[0] || parseFloat(energyQuery[0].totalEnergy) === 0) {
    console.log(`No curtailed energy found for ${date}`);
    return 0;
  }
  
  // Total curtailed energy in MWh
  const totalEnergy = Math.abs(parseFloat(energyQuery[0].totalEnergy));
  console.log(`Total energy for ${date}: ${totalEnergy.toFixed(2)} MWh`);
  
  // Calculate Bitcoin mining potential based on miner model
  let bitcoinMined = 0;
  
  // Use fixed efficiency values for simplicity in this quick calculation
  switch (minerModel) {
    case 'S19J_PRO':
      // 29.5 J/TH efficiency
      bitcoinMined = totalEnergy * 0.001354; // Approximate conversion factor
      break;
    case 'M20S':
      // 50 J/TH efficiency 
      bitcoinMined = totalEnergy * 0.000798; // Approximate conversion factor
      break;
    case 'S9':
      // 94 J/TH efficiency
      bitcoinMined = totalEnergy * 0.000425; // Approximate conversion factor
      break;
    default:
      bitcoinMined = totalEnergy * 0.001; // Default approximation
  }
  
  return bitcoinMined;
}

/**
 * Process Bitcoin calculations for a specific date and all miner models
 */
async function processBitcoinCalculations(): Promise<void> {
  console.log(`Processing quick Bitcoin calculations for ${TARGET_DATE}...`);
  
  // Get all the curtailment records to verify
  const records = await db
    .select({
      count: sql<number>`count(*)`,
      totalEnergy: sql<string>`SUM(ABS(${curtailmentRecords.volume}))`,
      totalPayment: sql<string>`SUM(ABS(${curtailmentRecords.payment}))`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
  
  console.log(`Found ${records[0].count} curtailment records for ${TARGET_DATE}`);
  console.log(`Total energy: ${parseFloat(records[0].totalEnergy).toFixed(2)} MWh`);
  console.log(`Total payment: £${parseFloat(records[0].totalPayment).toFixed(2)}`);
  
  // Process each miner model
  for (const minerModel of MINER_MODELS) {
    console.log(`Calculating Bitcoin for ${TARGET_DATE} with ${minerModel}...`);
    
    // Calculate Bitcoin for this date and miner model
    const bitcoinMined = await calculateBitcoinPotential(TARGET_DATE, minerModel);
    
    console.log(`${minerModel}: ${bitcoinMined.toFixed(8)} BTC mined`);
    
    // Insert/update the daily summary
    const existingSummary = await db
      .select()
      .from(bitcoinDailySummaries)
      .where(
        and(
          eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
          eq(bitcoinDailySummaries.minerModel, minerModel)
        )
      );
    
    if (existingSummary.length > 0) {
      // Update existing summary
      await db
        .update(bitcoinDailySummaries)
        .set({
          bitcoinMined: bitcoinMined.toString(),
          updatedAt: new Date()
        })
        .where(
          and(
            eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
            eq(bitcoinDailySummaries.minerModel, minerModel)
          )
        );
      
      console.log(`Updated Bitcoin daily summary for ${TARGET_DATE} with ${minerModel}`);
    } else {
      // Insert new summary
      await db
        .insert(bitcoinDailySummaries)
        .values({
          summaryDate: TARGET_DATE,
          minerModel: minerModel,
          bitcoinMined: bitcoinMined.toString(),
          createdAt: new Date(),
          updatedAt: new Date()
        });
      
      console.log(`Inserted Bitcoin daily summary for ${TARGET_DATE} with ${minerModel}`);
    }
  }
  
  // Get the final results
  const summaries = await db
    .select({
      minerModel: bitcoinDailySummaries.minerModel,
      bitcoinMined: bitcoinDailySummaries.bitcoinMined
    })
    .from(bitcoinDailySummaries)
    .where(eq(bitcoinDailySummaries.summaryDate, TARGET_DATE))
    .orderBy(desc(bitcoinDailySummaries.bitcoinMined));
  
  console.log("\n==== Final Bitcoin Mining Results ====");
  let totalBitcoin = 0;
  
  for (const summary of summaries) {
    const btc = parseFloat(summary.bitcoinMined);
    totalBitcoin += btc;
    console.log(`${summary.minerModel}: ${btc.toFixed(8)} BTC`);
  }
  
  console.log(`\nTotal Bitcoin mined: ${totalBitcoin.toFixed(8)} BTC`);
  console.log("Processing completed");
}

// Run the processing
processBitcoinCalculations()
  .then(() => {
    console.log("Script execution completed successfully");
    process.exit(0);
  })
  .catch(error => {
    console.error("FATAL ERROR:", error);
    process.exit(1);
  });
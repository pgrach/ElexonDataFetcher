/**
 * Fix Bitcoin calculations for April 1, 2025
 * 
 * This script removes and recalculates Bitcoin mining calculations for 2025-04-01
 * using data from the curtailment_records table.
 */

import { db } from "./db";
import { 
  historicalBitcoinCalculations, 
  curtailmentRecords,
  bitcoinDailySummaries
} from "./db/schema";
import { eq, and, sql } from "drizzle-orm";
import { calculateBitcoin } from "./server/utils/bitcoin";
import { getDifficultyData } from "./server/services/dynamodbService";

// Target date for processing
const TARGET_DATE = "2025-04-01";

// Miner models to process
const MINER_MODELS = ["S19J_PRO", "S9", "M20S"];

async function main() {
  try {
    console.log(`\n===== FIXING BITCOIN CALCULATIONS FOR ${TARGET_DATE} =====\n`);

    // Step 1: Analyze current curtailment records
    const curtailmentCount = await db.select({
      count: sql<number>`COUNT(*)::int`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));

    console.log(`Found ${curtailmentCount[0].count} curtailment records for ${TARGET_DATE}`);

    // Step 2: Process each miner model
    for (const minerModel of MINER_MODELS) {
      console.log(`\nProcessing ${minerModel}...`);
      
      // Delete existing calculations for this date and model
      const deleteResult = await db.delete(historicalBitcoinCalculations)
        .where(
          and(
            eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE),
            eq(historicalBitcoinCalculations.minerModel, minerModel)
          )
        )
        .returning({ id: historicalBitcoinCalculations.id });
      
      console.log(`Deleted ${deleteResult.length} existing calculations for ${minerModel}`);
      
      // Get difficulty for the date
      const difficulty = await getDifficultyData(TARGET_DATE);
      console.log(`Using network difficulty: ${difficulty}`);
      
      // Get all curtailment records for this date
      const records = await db.select({
        settlementPeriod: curtailmentRecords.settlementPeriod,
        farmId: curtailmentRecords.farmId,
        volume: curtailmentRecords.volume
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
      
      // Prepare new calculations
      const newCalculations = [];
      let totalBitcoin = 0;
      
      for (const record of records) {
        const mwh = Math.abs(Number(record.volume));
        
        // Skip records with no volume
        if (mwh <= 0 || isNaN(mwh)) continue;
        
        // Calculate Bitcoin
        const bitcoinMined = calculateBitcoin(mwh, minerModel, difficulty);
        totalBitcoin += bitcoinMined;
        
        newCalculations.push({
          settlementDate: TARGET_DATE,
          settlementPeriod: record.settlementPeriod,
          minerModel: minerModel,
          farmId: record.farmId,
          bitcoinMined: bitcoinMined.toString(),
          difficulty: difficulty.toString()
        });
      }
      
      // Insert in batches of 50
      const batchSize = 50;
      for (let i = 0; i < newCalculations.length; i += batchSize) {
        const batch = newCalculations.slice(i, i + batchSize);
        await db.insert(historicalBitcoinCalculations).values(batch);
      }
      
      console.log(`Inserted ${newCalculations.length} new records for ${minerModel}`);
      console.log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)}`);
      
      // Update daily summary
      await db.delete(bitcoinDailySummaries)
        .where(
          and(
            eq(bitcoinDailySummaries.summaryDate, TARGET_DATE),
            eq(bitcoinDailySummaries.minerModel, minerModel)
          )
        );
      
      await db.insert(bitcoinDailySummaries).values({
        summaryDate: TARGET_DATE,
        minerModel: minerModel,
        bitcoinMined: totalBitcoin.toString(),
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      console.log(`Updated daily summary for ${TARGET_DATE} and ${minerModel}`);
    }
    
    console.log("\n===== BITCOIN CALCULATIONS FIX COMPLETED =====\n");
    
  } catch (error) {
    console.error("Error fixing Bitcoin calculations:", error);
  }
}

// Execute the main function
main().catch(console.error);
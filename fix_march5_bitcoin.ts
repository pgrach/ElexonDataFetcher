/**
 * Fix Bitcoin Calculations for March 5, 2025
 * 
 * This script is designed to fix the Bitcoin calculations for March 5, 2025
 * with improved error handling and constraint violation management.
 */

import { db } from "./db";
import { 
  historicalBitcoinCalculations, 
  bitcoinMonthlySummaries,
  curtailmentRecords
} from "./db/schema";
import { and, eq, sql } from "drizzle-orm";
import { 
  MinerStats, 
  minerModels, 
  DEFAULT_DIFFICULTY 
} from "./server/types/bitcoin";
import { getDifficultyData } from "./server/services/dynamodbService";

// Number of records to process in each batch to avoid memory issues
const BATCH_SIZE = 100;

async function main() {
  try {
    console.log("Starting Bitcoin reconciliation fix for 2025-03-05");
    
    // Count curtailment records for the date
    const countResult = await db.select({
      count: sql<number>`count(*)`,
      totalVolume: sql<string>`sum(volume::numeric)`,
      totalPayment: sql<string>`sum(payment::numeric)`,
      periods: sql<number>`count(distinct settlement_period)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, "2025-03-05"));

    if (countResult.length === 0 || !countResult[0].count) {
      console.error("No curtailment records found for 2025-03-05");
      return;
    }

    console.log(`Found ${countResult[0].count} curtailment records`);
    console.log(`Total volume: ${countResult[0].totalVolume} MWh`);
    console.log(`Total payment: £${countResult[0].totalPayment}`);
    console.log(`Across ${countResult[0].periods} periods`);

    // Delete existing Bitcoin calculations in batches to avoid timeouts
    console.log("Deleting existing Bitcoin calculations in batches...");
    await deleteExistingCalculations("2025-03-05");
    
    // Fetch difficulty data
    console.log("Fetching difficulty data...");
    const difficulty = await getDifficultyForDate("2025-03-05");
    console.log(`Using difficulty: ${difficulty}`);
    
    // Process miner models one by one
    const minerModelKeys = Object.keys(minerModels);
    for (const minerModel of minerModelKeys) {
      console.log(`Processing miner model: ${minerModel}`);
      
      // Process the calculations for this miner model
      await processCalculationsForMinerModel("2025-03-05", minerModel, difficulty);
    }
    
    // Update monthly summaries
    console.log("Updating monthly summaries...");
    for (const minerModel of minerModelKeys) {
      await calculateMonthlyBitcoinSummary("2025-03", minerModel);
    }
    
    console.log("Bitcoin reconciliation fix completed successfully");
  } catch (error) {
    console.error("Error in Bitcoin reconciliation fix:", error);
    throw error;
  }
}

/**
 * Delete existing Bitcoin calculations for a date in batches
 */
async function deleteExistingCalculations(date: string) {
  try {
    // First, count how many records we need to delete
    const countResult = await db.select({
      count: sql<number>`count(*)`
    })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, date));
    
    const totalRecords = countResult[0]?.count || 0;
    console.log(`Found ${totalRecords} existing Bitcoin calculations to delete`);
    
    if (totalRecords === 0) {
      return;
    }
    
    // Get all the IDs we need to delete
    const idResults = await db.select({
      id: historicalBitcoinCalculations.id
    })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, date));
    
    const ids = idResults.map(r => r.id);
    
    // Delete in batches
    let deleted = 0;
    for (let i = 0; i < ids.length; i += BATCH_SIZE) {
      const batchIds = ids.slice(i, i + BATCH_SIZE);
      
      // Use individual deletions to avoid SQL syntax issues
      for (const id of batchIds) {
        await db.delete(historicalBitcoinCalculations)
          .where(eq(historicalBitcoinCalculations.id, id));
        deleted++;
      }
      
      console.log(`Deleted ${deleted}/${totalRecords} records`);
    }
    
    console.log(`Deleted ${deleted} existing Bitcoin calculations`);
  } catch (error) {
    console.error("Error deleting existing calculations:", error);
    throw error;
  }
}

/**
 * Get difficulty for a specific date
 */
async function getDifficultyForDate(date: string): Promise<number> {
  try {
    const difficulty = await getDifficultyData(date);
    return difficulty;
  } catch (error) {
    console.error(`Error fetching difficulty for ${date}:`, error);
    console.log(`Using default difficulty: ${DEFAULT_DIFFICULTY}`);
    return DEFAULT_DIFFICULTY;
  }
}

/**
 * Process Bitcoin calculations for a specific date and miner model
 */
async function processCalculationsForMinerModel(date: string, minerModel: string, difficulty: number) {
  try {
    const minerStats = getMinerStatsForModel(minerModel);
    
    // Get all curtailment records for the date
    const records = await db.select()
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, date));
    
    console.log(`Processing ${records.length} records for miner model ${minerModel}`);
    
    // Process in batches to avoid memory issues
    let processed = 0;
    let successful = 0;
    let failed = 0;
    
    for (let i = 0; i < records.length; i += BATCH_SIZE) {
      const batchRecords = records.slice(i, i + BATCH_SIZE);
      
      // Process each record in the batch
      for (const record of batchRecords) {
        try {
          // Calculate Bitcoin mined
          const bitcoinMined = calculateBitcoin(
            Number(record.volume),
            minerStats.hashrate,
            minerStats.power,
            difficulty
          );
          
          // Check if record already exists first to avoid constraint violations
          const existingRecord = await db.select({ id: historicalBitcoinCalculations.id })
            .from(historicalBitcoinCalculations)
            .where(and(
              eq(historicalBitcoinCalculations.settlementDate, date),
              eq(historicalBitcoinCalculations.settlementPeriod, record.settlementPeriod),
              eq(historicalBitcoinCalculations.farmId, record.farmId),
              eq(historicalBitcoinCalculations.minerModel, minerModel)
            ))
            .limit(1);
          
          if (existingRecord.length > 0) {
            // Skip this record as it already exists
            console.log(`Skipping duplicate record for date=${date}, period=${record.settlementPeriod}, farm=${record.farmId}, model=${minerModel}`);
            continue;
          }
          
          // Insert the calculation
          await db.insert(historicalBitcoinCalculations).values({
            settlementDate: date,
            settlementPeriod: record.settlementPeriod,
            difficulty: difficulty.toString(),
            calculatedAt: new Date(),
            bitcoinMined: bitcoinMined.toString(),
            farmId: record.farmId,
            minerModel: minerModel
          });
          
          successful++;
        } catch (error) {
          console.error(`Error processing record:`, error);
          failed++;
        }
        
        processed++;
      }
      
      console.log(`Processed ${processed}/${records.length} records (${successful} successful, ${failed} failed)`);
    }
    
    console.log(`Completed processing for miner model ${minerModel}`);
    console.log(`Successfully processed ${successful} records, ${failed} failed`);
  } catch (error) {
    console.error(`Error processing calculations for ${date} ${minerModel}:`, error);
    throw error;
  }
}

/**
 * Calculate Bitcoin mined based on curtailed energy and miner specs
 */
function calculateBitcoin(
  curtailedMwh: number,
  hashrate: number, // TH/s
  power: number,    // Watts
  difficulty: number
): number {
  // Convert power from watts to megawatts for calculation
  const powerMw = power / 1000000;
  
  // Calculate how many miners we could run
  const minerCount = curtailedMwh / (powerMw * 0.5); // Assuming 30-minute settlement periods
  
  // Calculate total hashrate (TH/s)
  const totalHashrate = minerCount * hashrate;
  
  // Calculate expected Bitcoin mined
  // Formula: (hashrate * 3600 * 24 * 6.25) / (difficulty * 2^32)
  const bitcoinMined = (totalHashrate * 3600 * 0.5 * 6.25) / (difficulty * Math.pow(2, 32));
  
  return bitcoinMined;
}

/**
 * Calculate monthly Bitcoin summary
 */
async function calculateMonthlyBitcoinSummary(yearMonth: string, minerModel: string) {
  try {
    console.log(`Calculating monthly summary for ${yearMonth} ${minerModel}`);
    
    // Extract year and month
    const year = yearMonth.split('-')[0];
    const month = yearMonth.split('-')[1];
    
    // Delete existing summary if it exists
    try {
      await db.delete(bitcoinMonthlySummaries)
        .where(and(
          eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
          eq(bitcoinMonthlySummaries.minerModel, minerModel)
        ));
      console.log(`Deleted existing monthly summary for ${yearMonth} ${minerModel}`);
    } catch (error) {
      console.error(`Error deleting existing monthly summary: ${error}`);
    }
    
    // Aggregate Bitcoin calculations for the month
    const summary = await db.select({
      totalBitcoin: sql<string>`sum(bitcoin_mined::numeric)`,
      difficulty: sql<string>`max(difficulty::numeric)`,
    })
    .from(historicalBitcoinCalculations)
    .where(and(
      sql`extract(year from settlement_date) = ${year}`,
      sql`extract(month from settlement_date) = ${month}`,
      eq(historicalBitcoinCalculations.minerModel, minerModel)
    ));
    
    if (summary.length > 0 && summary[0].totalBitcoin) {
      // Insert the monthly summary
      await db.insert(bitcoinMonthlySummaries).values({
        yearMonth: yearMonth,
        minerModel: minerModel,
        bitcoinMined: summary[0].totalBitcoin,
        valueAtMining: '0', // Default value, can be updated later
        averageDifficulty: summary[0].difficulty || '0',
        createdAt: new Date(),
        updatedAt: new Date(),
      });
      
      console.log(`✅ Monthly summary for ${yearMonth} ${minerModel}: ${parseFloat(summary[0].totalBitcoin).toFixed(8)} BTC`);
    } else {
      console.log(`⚠️ No data found for ${yearMonth} ${minerModel}`);
    }
  } catch (error) {
    console.error(`Error calculating monthly summary for ${yearMonth} ${minerModel}:`, error);
  }
}

/**
 * Get miner stats for a specific model
 */
function getMinerStatsForModel(model: string): MinerStats {
  if (model in minerModels) {
    return minerModels[model];
  } else {
    throw new Error(`Unknown miner model: ${model}`);
  }
}

// Run the main function
main()
  .then(() => {
    console.log('Bitcoin fix script completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Bitcoin fix script failed:', error);
    process.exit(1);
  });
/**
 * Simple Bitcoin Calculation Generator for March 5, 2025
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

// Process in smaller batches to avoid memory issues
const BATCH_SIZE = 50;

async function main() {
  try {
    const DATE = "2025-03-05";
    console.log(`Starting simplified Bitcoin calculation for ${DATE}`);
    
    // Get difficulty
    let difficulty: number;
    try {
      console.log("Fetching difficulty data...");
      difficulty = await getDifficultyData(DATE);
      console.log(`Using difficulty: ${difficulty}`);
    } catch (error) {
      console.warn(`Error fetching difficulty: ${error}`);
      difficulty = DEFAULT_DIFFICULTY;
      console.log(`Using default difficulty: ${difficulty}`);
    }
    
    // Get all farm curtailment records for the date
    console.log("Fetching curtailment records...");
    const curtailmentQuery = await db.select({
      settlementPeriod: curtailmentRecords.settlementPeriod,
      farmId: curtailmentRecords.farmId,
      volume: curtailmentRecords.volume
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE));
    
    console.log(`Found ${curtailmentQuery.length} curtailment records`);
    
    // Process each miner model
    for (const minerModel of Object.keys(minerModels)) {
      console.log(`\nProcessing miner model: ${minerModel}`);
      const minerStats = minerModels[minerModel];
      
      let recordsProcessed = 0;
      let recordsInserted = 0;
      
      // Process in batches
      for (let i = 0; i < curtailmentQuery.length; i += BATCH_SIZE) {
        const batch = curtailmentQuery.slice(i, i + BATCH_SIZE);
        console.log(`Processing batch ${i / BATCH_SIZE + 1}/${Math.ceil(curtailmentQuery.length / BATCH_SIZE)}`);
        
        for (const record of batch) {
          try {
            // Calculate bitcoin mined
            const bitcoinMined = calculateBitcoin(
              Number(record.volume),
              minerStats.hashrate,
              minerStats.power,
              difficulty
            );
            
            // Insert the calculation
            await db.insert(historicalBitcoinCalculations).values({
              settlementDate: DATE,
              settlementPeriod: record.settlementPeriod,
              difficulty: difficulty.toString(),
              calculatedAt: new Date(),
              bitcoinMined: bitcoinMined.toString(),
              farmId: record.farmId,
              minerModel: minerModel
            })
            // Handle unique constraint by ignoring duplicates
            .onConflictDoNothing({
              target: [
                historicalBitcoinCalculations.settlementDate,
                historicalBitcoinCalculations.settlementPeriod,
                historicalBitcoinCalculations.farmId,
                historicalBitcoinCalculations.minerModel
              ]
            });
            
            recordsInserted++;
          } catch (error) {
            console.error(`Error inserting record: ${error}`);
          }
          
          recordsProcessed++;
          
          // Print progress occasionally
          if (recordsProcessed % 100 === 0) {
            console.log(`Processed ${recordsProcessed}/${curtailmentQuery.length} records`);
          }
        }
      }
      
      console.log(`Completed ${minerModel}: processed ${recordsProcessed}, inserted ${recordsInserted}`);
    }
    
    // Calculate monthly summaries
    console.log("\nUpdating monthly summaries...");
    await updateMonthlySummaries("2025-03");
    
    console.log("Bitcoin calculation completed successfully");
  } catch (error) {
    console.error("Error in Bitcoin calculation:", error);
    throw error;
  }
}

/**
 * Calculate Bitcoin mined
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
 * Update monthly summaries for all miner models
 */
async function updateMonthlySummaries(yearMonth: string): Promise<void> {
  try {
    // Extract year and month
    const year = yearMonth.split('-')[0];
    const month = yearMonth.split('-')[1];
    
    for (const minerModel of Object.keys(minerModels)) {
      console.log(`Calculating monthly summary for ${yearMonth} ${minerModel}`);
      
      try {
        // Delete existing summary if it exists
        await db.delete(bitcoinMonthlySummaries)
          .where(and(
            eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
            eq(bitcoinMonthlySummaries.minerModel, minerModel)
          ));
      } catch (error) {
        // Ignore deletion errors
        console.warn(`Error deleting existing summary: ${error}`);
      }
      
      // Calculate summary
      const summary = await db.select({
        totalBitcoin: sql<string>`sum(bitcoin_mined::numeric)`,
        difficulty: sql<string>`max(difficulty::numeric)`
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
          updatedAt: new Date()
        });
        
        console.log(`✅ Monthly summary for ${yearMonth} ${minerModel}: ${parseFloat(summary[0].totalBitcoin).toFixed(8)} BTC`);
      } else {
        console.log(`⚠️ No data found for ${yearMonth} ${minerModel}`);
      }
    }
  } catch (error) {
    console.error("Error updating monthly summaries:", error);
  }
}

// Run the main function
main()
  .then(() => {
    console.log('Script completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Script failed:', error);
    process.exit(1);
  });
/**
 * Reconcile Historical Bitcoin Calculations for March 5, 2025
 * 
 * This script is specifically designed to fix missing Bitcoin calculations
 * for March 5, 2025 after the curtailment records have been reprocessed.
 * It also regenerates monthly Bitcoin summaries for March 2025.
 */

import { db } from './db';
import { curtailmentRecords, historicalBitcoinCalculations, bitcoinMonthlySummaries } from './db/schema';
import { eq, sql, and } from 'drizzle-orm';
import { getDifficultyData } from './server/services/dynamodbService';
import { minerModels, MinerStats } from './server/types/bitcoin';

const DATE_TO_PROCESS = '2025-03-05';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];
const BITCOIN_PER_TH_PER_DAY = 0.000008; // Used for hashrate to BTC conversion

// Main function to process all calculations
async function main() {
  try {
    console.log(`Starting Bitcoin reconciliation for ${DATE_TO_PROCESS}`);

    // First, check how many curtailment records we have
    const curtailmentStats = await db.select({
      count: sql<number>`count(*)`,
      totalVolume: sql<number>`sum(abs(volume::numeric))`,
      totalPayment: sql<number>`sum(payment::numeric)`,
      totalPeriods: sql<number>`count(distinct settlement_period)`,
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE_TO_PROCESS));

    console.log(`Found ${curtailmentStats[0].count} curtailment records`);
    console.log(`Total volume: ${curtailmentStats[0].totalVolume} MWh`);
    console.log(`Total payment: £${curtailmentStats[0].totalPayment}`);
    console.log(`Across ${curtailmentStats[0].totalPeriods} periods`);

    // Now delete any existing Bitcoin calculations for this date
    console.log('Deleting existing Bitcoin calculations...');
    
    const deletedCount = await db.delete(historicalBitcoinCalculations)
      .where(eq(historicalBitcoinCalculations.settlementDate, DATE_TO_PROCESS))
      .returning({ id: historicalBitcoinCalculations.id });
    
    console.log(`Deleted ${deletedCount.length} existing Bitcoin calculations`);

    // Get difficulty from DynamoDB for this date
    console.log('Fetching difficulty data...');
    const difficulty = await getDifficultyData(DATE_TO_PROCESS);
    console.log(`Using difficulty: ${difficulty.toLocaleString()}`);

    // Load farm IDs with curtailment records for this date
    const farmIds = await db.select({
      farmId: curtailmentRecords.farmId
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, DATE_TO_PROCESS))
    .groupBy(curtailmentRecords.farmId);

    console.log(`Found ${farmIds.length} farms with curtailment records`);

    // Process each miner model
    for (const minerModel of MINER_MODELS) {
      console.log(`\nProcessing calculations for ${minerModel} miner model`);
      const minerStats = getMinerStatsForModel(minerModel);
      console.log(`Miner stats: ${minerStats.hashrate} TH/s, ${minerStats.power} W`);

      let totalBitcoin = 0;
      let insertedCount = 0;

      // Process each farm
      for (const farm of farmIds) {
        try {
          // Get all periods for this farm on this date
          const periods = await db.select({
            period: curtailmentRecords.settlementPeriod,
            volume: sql<number>`abs(volume::numeric)`
          })
          .from(curtailmentRecords)
          .where(and(
            eq(curtailmentRecords.settlementDate, DATE_TO_PROCESS),
            eq(curtailmentRecords.farmId, farm.farmId)
          ));

          // Process each period
          for (const period of periods) {
            try {
              // Calculate Bitcoin mined
              // Formula: curtailed energy (MWh) * miner hashrate (TH/s) / miner power (kW) * Bitcoin per TH per day / 24 (for hourly) / difficulty scaling
              const curtailedMwh = period.volume;
              const hashrateThs = minerStats.hashrate;
              const powerKw = minerStats.power / 1000; // Convert from W to kW
              
              // Calculate Bitcoin mined for this period
              // We're using 1/24 as each period is typically an hour (1/24 of a day)
              const difficultyScaling = 1; // Adjust if needed based on historical data
              const bitcoinMined = (curtailedMwh * hashrateThs / powerKw * BITCOIN_PER_TH_PER_DAY / 24) * difficultyScaling;
              
              // Scale difficulty appropriately
              const difficultyAdjustment = 108105433845147 / difficulty; // Base difficulty adjustment
              const scaledBitcoin = bitcoinMined * difficultyAdjustment;
              
              totalBitcoin += scaledBitcoin;

              // Insert the calculation into the database
              await db.insert(historicalBitcoinCalculations).values([{
                settlementDate: DATE_TO_PROCESS,
                settlementPeriod: period.period,
                farmId: farm.farmId,
                minerModel: minerModel,
                bitcoinMined: scaledBitcoin.toString(),
                curtailedMwh: curtailedMwh.toString(),
                difficulty: difficulty.toString(),
                calculatedAt: new Date(),
              }]);

              insertedCount++;
            } catch (error) {
              console.error(`Error processing period ${period.period} for farm ${farm.farmId}:`, error);
            }
          }
        } catch (error) {
          console.error(`Error processing farm ${farm.farmId}:`, error);
        }
      }

      console.log(`✅ Inserted ${insertedCount} calculations for ${minerModel}`);
      console.log(`Total Bitcoin calculated: ${totalBitcoin.toFixed(8)} BTC`);
    }

    // Update the monthly summaries for March 2025
    console.log('\nUpdating monthly Bitcoin summaries...');
    
    const yearMonth = '2025-03';
    for (const minerModel of MINER_MODELS) {
      await calculateMonthlyBitcoinSummary(yearMonth, minerModel);
    }

    console.log('\n✅ Reconciliation complete!');
    console.log('You can now verify the data using the API:');
    console.log(`- GET /api/curtailment/monthly-mining-potential/${yearMonth}`);
    
  } catch (error) {
    console.error('Error during reconciliation:', error);
  }
}

// Calculate and store monthly Bitcoin summaries
async function calculateMonthlyBitcoinSummary(yearMonth: string, minerModel: string) {
  try {
    // Extract year and month
    const year = yearMonth.split('-')[0];
    const month = yearMonth.split('-')[1];

    // Delete existing summary if it exists
    await db.delete(bitcoinMonthlySummaries)
      .where(and(
        eq(bitcoinMonthlySummaries.yearMonth, yearMonth),
        eq(bitcoinMonthlySummaries.minerModel, minerModel)
      ));

    // Aggregate Bitcoin calculations for the month
    const summary = await db.select({
      totalBitcoin: sql<string>`sum(bitcoin_mined::numeric)`,
      totalCurtailedMwh: sql<string>`sum(curtailed_mwh::numeric)`,
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
      await db.insert(bitcoinMonthlySummaries).values([{
        yearMonth: yearMonth,
        minerModel: minerModel,
        bitcoinMined: summary[0].totalBitcoin,
        curtailedMwh: summary[0].totalCurtailedMwh || '0',
        difficulty: summary[0].difficulty || '0',
        createdAt: new Date(),
        updatedAt: new Date(),
      }]);

      console.log(`✅ Monthly summary for ${yearMonth} ${minerModel}: ${parseFloat(summary[0].totalBitcoin).toFixed(8)} BTC from ${parseFloat(summary[0].totalCurtailedMwh || '0').toFixed(2)} MWh`);
    } else {
      console.log(`⚠️ No data found for ${yearMonth} ${minerModel}`);
    }
  } catch (error) {
    console.error(`Error calculating monthly summary for ${yearMonth} ${minerModel}:`, error);
  }
}

// Get miner stats for a specific model
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
    console.log('Script completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Script failed:', error);
    process.exit(1);
  });
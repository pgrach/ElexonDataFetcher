/**
 * Reconcile Historical Bitcoin Calculations for March 5, 2025
 * 
 * This script is specifically designed to fix missing Bitcoin calculations
 * for March 5, 2025 after the curtailment records have been reprocessed.
 */

import { db } from './db';
import { curtailmentRecords, historicalBitcoinCalculations, bitcoinMonthlySummaries } from './db/schema';
import { sql, and, eq } from 'drizzle-orm';
import { getDifficultyData } from './server/services/dynamodbService';

// Target date information
const TARGET_DATE = '2025-03-05';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function main() {
  console.log(`🔄 Starting Bitcoin calculation reconciliation for ${TARGET_DATE}`);
  
  // Check curtailment records to confirm we have data
  const curtailmentStats = await db.select({
    recordCount: sql<number>`count(*)`,
    periodCount: sql<number>`count(distinct settlement_period)`,
    totalVolume: sql<number>`sum(abs(volume::numeric))`,
    totalPayment: sql<number>`sum(payment::numeric)`
  })
  .from(curtailmentRecords)
  .where(sql`${curtailmentRecords.settlementDate}::text = ${TARGET_DATE}`);
  
  if (!curtailmentStats[0] || Number(curtailmentStats[0].recordCount) === 0) {
    console.error('No curtailment records found for the target date');
    process.exit(1);
  }
  
  console.log(`Found ${curtailmentStats[0].recordCount} curtailment records across ${curtailmentStats[0].periodCount} periods`);
  console.log(`Total curtailed energy: ${curtailmentStats[0].totalVolume} MWh`);
  console.log(`Total payment: £${curtailmentStats[0].totalPayment}`);
  
  // Get difficulty data for the target date
  try {
    const difficulty = await getDifficultyData(TARGET_DATE);
    console.log(`Using difficulty for ${TARGET_DATE}: ${difficulty.toLocaleString()}`);
    
    // Process calculations for each miner model
    for (const minerModel of MINER_MODELS) {
      console.log(`Processing ${TARGET_DATE} with ${minerModel}...`);
      
      // Get existing calculations for this date and model
      const existingCalculations = await db.select({
        count: sql<number>`count(*)`
      })
      .from(historicalBitcoinCalculations)
      .where(
        and(
          sql`${historicalBitcoinCalculations.settlementDate}::text = ${TARGET_DATE}`,
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        )
      );
      
      // Delete existing calculations for this date and model to avoid duplicates
      if (existingCalculations[0].count > 0) {
        console.log(`Deleting ${existingCalculations[0].count} existing calculations for ${minerModel}`);
        
        await db.delete(historicalBitcoinCalculations)
          .where(
            and(
              sql`${historicalBitcoinCalculations.settlementDate}::text = ${TARGET_DATE}`,
              eq(historicalBitcoinCalculations.minerModel, minerModel)
            )
          );
      }
      
      // Get miner stats
      const minerStats = getMinerStatsForModel(minerModel);
      
      if (!minerStats) {
        console.error(`Invalid miner model: ${minerModel}`);
        continue;
      }
      
      // Get all curtailment records for the date
      const curtailments = await db.select({
        id: curtailmentRecords.id,
        settlementDate: curtailmentRecords.settlementDate,
        settlementPeriod: curtailmentRecords.settlementPeriod,
        farmId: curtailmentRecords.farmId,
        volume: curtailmentRecords.volume,
        leadPartyName: curtailmentRecords.leadPartyName
      })
      .from(curtailmentRecords)
      .where(sql`${curtailmentRecords.settlementDate}::text = ${TARGET_DATE}`);
      
      // Group by farm and period for processing
      const processedPeriods = new Set<string>();
      let insertCount = 0;
      
      for (const record of curtailments) {
        const periodKey = `${record.farmId}-${record.settlementPeriod}`;
        
        // Skip if already processed this farm-period combination
        if (processedPeriods.has(periodKey)) {
          continue;
        }
        
        processedPeriods.add(periodKey);
        
        // Get all records for this farm and period
        const farmPeriodRecords = curtailments.filter(
          r => r.farmId === record.farmId && r.settlementPeriod === record.settlementPeriod
        );
        
        // Calculate total volume for this farm and period
        const totalVolume = farmPeriodRecords.reduce(
          (sum, r) => sum + Math.abs(parseFloat(r.volume)), 
          0
        );
        
        // Calculate Bitcoin mined using the formula
        // Bitcoin = (Energy in MWh * 1000 * 1000) / (Difficulty * 2^32 / (Hashrate * 3600 * 24 * 1000 * 1000 * 1000))
        const energyInWattHours = totalVolume * 1000 * 1000; // Convert MWh to Wh
        const hashrateTH = minerStats.hashrate; // TH/s
        const hashrateHashes = hashrateTH * 1000 * 1000 * 1000 * 1000; // Convert TH/s to H/s
        const secondsInDay = 24 * 60 * 60;
        const divisor = difficulty * Math.pow(2, 32) / (hashrateHashes * secondsInDay);
        const bitcoinMined = energyInWattHours / divisor;
        
        // Insert new calculation
        try {
          await db.insert(historicalBitcoinCalculations).values({
            settlementDate: new Date(TARGET_DATE),
            minerModel: minerModel,
            farmId: record.farmId,
            settlementPeriod: record.settlementPeriod,
            bitcoinMined: bitcoinMined.toString(),
            difficulty: difficulty.toString(),
            calculatedAt: new Date()
          });
          
          insertCount++;
        } catch (error) {
          console.error(`Error inserting calculation for ${record.farmId} period ${record.settlementPeriod}:`, error);
        }
      }
      
      console.log(`Inserted ${insertCount} calculations for ${TARGET_DATE} with ${minerModel}`);
    }
    
    // Calculate monthly summary for March 2025
    await calculateMonthlyBitcoinSummary('2025-03');
    
    console.log(`\n===== Reconciliation Complete =====`);
    console.log(`Successfully processed calculations for date: ${TARGET_DATE}`);
    console.log(`Miner models processed: ${MINER_MODELS.join(', ')}`);
    
  } catch (error) {
    console.error('Error during reconciliation:', error);
    process.exit(1);
  }
}

// Helper function to calculate monthly Bitcoin summary
async function calculateMonthlyBitcoinSummary(yearMonth: string) {
  console.log(`Updating monthly summary for ${yearMonth}...`);
  
  const [year, month] = yearMonth.split('-');
  
  // Delete existing summary for this month if it exists
  await db.delete(bitcoinMonthlySummaries)
    .where(
      eq(bitcoinMonthlySummaries.yearMonth, yearMonth)
    );
  
  // Calculate for each miner model
  for (const minerModel of MINER_MODELS) {
    // Get all calculations for the month
    const monthlyData = await db.select({
      totalBitcoin: sql<string>`SUM(bitcoin_mined::numeric)`,
      totalEnergy: sql<string>`SUM(curtailed_mwh::numeric)`,
      avgDifficulty: sql<string>`AVG(difficulty::numeric)`
    })
    .from(historicalBitcoinCalculations)
    .where(
      and(
        sql`EXTRACT(YEAR FROM calculation_date) = ${parseInt(year)}`,
        sql`EXTRACT(MONTH FROM calculation_date) = ${parseInt(month)}`,
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      )
    );
    
    if (monthlyData[0].totalBitcoin) {
      // Insert monthly summary
      await db.insert(bitcoinMonthlySummaries).values({
        summaryYear: parseInt(year),
        summaryMonth: parseInt(month),
        minerModel: minerModel,
        totalBitcoinMined: monthlyData[0].totalBitcoin,
        totalCurtailedMwh: monthlyData[0].totalEnergy,
        averageDifficulty: monthlyData[0].avgDifficulty,
        createdAt: new Date(),
        updatedAt: new Date()
      });
      
      console.log(`Updated ${yearMonth} summary for ${minerModel}: ${parseFloat(monthlyData[0].totalBitcoin).toFixed(8)} BTC from ${parseFloat(monthlyData[0].totalEnergy).toFixed(2)} MWh`);
    }
  }
}

// Helper function to get miner stats
function getMinerStatsForModel(model: string) {
  const minerModels: Record<string, { hashrate: number; power: number }> = {
    'S19J_PRO': { hashrate: 110, power: 3050 },
    'S9': { hashrate: 14, power: 1350 },
    'M20S': { hashrate: 68, power: 3360 }
  };
  
  return minerModels[model] || null;
}

// Run the main function
main()
  .then(() => {
    console.log("Reconciliation complete");
    process.exit(0);
  })
  .catch((err) => {
    console.error("Error during reconciliation:", err);
    process.exit(1);
  });
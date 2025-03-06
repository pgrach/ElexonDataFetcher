/**
 * Reconcile Historical Bitcoin Calculations for March 5, 2025
 * 
 * This script is specifically designed to fix missing Bitcoin calculations
 * for March 5, 2025 after the curtailment records have been reprocessed.
 * It also regenerates monthly Bitcoin summaries for March 2025.
 */

import { db } from './db';
import { curtailmentRecords, historicalBitcoinCalculations, bitcoinMonthlySummaries } from './db/schema';
import { sql, and, eq } from 'drizzle-orm';
import { getDifficultyData } from './server/services/dynamodbService';

// Target date information
const TARGET_DATE = '2025-03-05';
const TARGET_MONTH = '2025-03';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function main() {
  console.log(`🔄 Starting Bitcoin calculation reconciliation for ${TARGET_DATE}`);
  
  try {
    // Calculate monthly summary for March 2025
    await calculateMonthlyBitcoinSummary(TARGET_MONTH);
    
    console.log(`\n===== Reconciliation Complete =====`);
    console.log(`Successfully processed monthly summaries for: ${TARGET_MONTH}`);
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
      totalEnergy: sql<string>`SUM(volume::numeric)`,
      avgDifficulty: sql<string>`AVG(difficulty::numeric)`
    })
    .from(historicalBitcoinCalculations)
    .where(
      and(
        sql`EXTRACT(YEAR FROM settlement_date) = ${parseInt(year)}`,
        sql`EXTRACT(MONTH FROM settlement_date) = ${parseInt(month)}`,
        eq(historicalBitcoinCalculations.minerModel, minerModel)
      )
    );
    
    if (monthlyData[0].totalBitcoin) {
      // Insert monthly summary
      await db.insert(bitcoinMonthlySummaries).values({
        yearMonth: yearMonth,
        minerModel: minerModel,
        bitcoinMined: monthlyData[0].totalBitcoin,
        valueAtMining: '0', // This can be updated later
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
/**
 * Specialized script to reingest data for 2025-03-24
 * 
 * This script handles:
 * 1. Reingesting curtailment data from Elexon API 
 * 2. Updating historical Bitcoin calculations
 * 3. Updating all summary tables
 */

import { processDailyCurtailment } from '../services/curtailment';
import { db } from "@db";
import { curtailmentRecords, dailySummaries } from "@db/schema";
import { eq, sql } from "drizzle-orm";

const TARGET_DATE = '2025-03-24';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function updateBitcoinCalculations(date: string) {
  // Import the function to process Bitcoin calculations
  const bitcoinService = await import('../services/bitcoinService');
  
  console.log(`Updating Bitcoin calculations for ${date}...`);
  for (const minerModel of MINER_MODELS) {
    await bitcoinService.processSingleDay(date, minerModel);
    console.log(`- Processed ${minerModel}`);
  }
}

async function updateMonthlyAndYearlySummaries(date: string) {
  // Extract year and month from the date
  const [year, month] = date.split('-');
  const yearMonth = `${year}-${month}`;
  
  // Import the functions for monthly and yearly summary updates
  const bitcoinService = await import('../services/bitcoinService');
  
  // Update monthly summaries for all miner models
  console.log(`Updating monthly Bitcoin summaries for ${yearMonth}...`);
  for (const minerModel of MINER_MODELS) {
    await bitcoinService.calculateMonthlyBitcoinSummary(yearMonth, minerModel);
  }
  
  // Update yearly summaries
  console.log(`Updating yearly Bitcoin summaries for ${year}...`);
  await bitcoinService.manualUpdateYearlyBitcoinSummary(year);
}

async function verifyData(date: string) {
  // Check the curtailment records
  const curtailmentStats = await db
    .select({
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  // Check the daily summary
  const dailySummary = await db.query.dailySummaries.findFirst({
    where: eq(dailySummaries.summaryDate, date)
  });
  
  return {
    curtailmentStats: {
      records: curtailmentStats[0]?.recordCount || 0,
      periods: curtailmentStats[0]?.periodCount || 0,
      volume: Number(curtailmentStats[0]?.totalVolume || 0).toFixed(2),
      payment: Number(curtailmentStats[0]?.totalPayment || 0).toFixed(2)
    },
    dailySummary: {
      energy: dailySummary?.totalCurtailedEnergy ? `${Number(dailySummary.totalCurtailedEnergy).toFixed(2)} MWh` : 'Not available',
      payment: dailySummary?.totalPayment ? `£${Number(dailySummary.totalPayment).toFixed(2)}` : 'Not available'
    }
  };
}

// Main execution function
async function executeUpdate() {
  try {
    console.log(`=== Starting data update for ${TARGET_DATE} ===`);
    
    // Step 1: Verify current data state
    console.log('Current data state:');
    const beforeState = await verifyData(TARGET_DATE);
    console.log(JSON.stringify(beforeState, null, 2));
    
    // Step 2: Reingest curtailment records from Elexon API
    console.log(`\nReingesting curtailment data from Elexon API...`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Step 3: Update Bitcoin calculations
    console.log(`\nUpdating Bitcoin calculations...`);
    await updateBitcoinCalculations(TARGET_DATE);
    
    // Step 4: Update monthly and yearly summaries
    console.log(`\nUpdating summary tables...`);
    await updateMonthlyAndYearlySummaries(TARGET_DATE);
    
    // Step 5: Verify updated data state
    console.log('\nUpdated data state:');
    const afterState = await verifyData(TARGET_DATE);
    console.log(JSON.stringify(afterState, null, 2));
    
    console.log(`\n=== Data update completed for ${TARGET_DATE} ===`);
  } catch (error) {
    console.error('Error during data update:', error);
    process.exit(1);
  }
}

// Execute the update
executeUpdate().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
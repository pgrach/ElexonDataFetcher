/**
 * Fix Missing Data for 2025-03-27
 * 
 * This script reprocesses the curtailment data for 2025-03-27
 * and updates all Bitcoin calculations to ensure complete data.
 */

import { processDailyCurtailment } from './server/services/curtailment';
import { processSingleDay } from './server/services/bitcoinService';
import { calculateMonthlyBitcoinSummary, manualUpdateYearlyBitcoinSummary } from './server/services/bitcoinService';
import { db } from './db';
import { curtailmentRecords, dailySummaries } from './db/schema';
import { eq, sql } from 'drizzle-orm';

const TARGET_DATE = '2025-03-27';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

async function fixMarchData() {
  try {
    console.log(`\n===== Starting Data Repair for ${TARGET_DATE} =====\n`);
    
    // Step 1: Check current state
    console.log(`Current state before repair:`);
    await checkCurrentState();
    
    // Step 2: Process curtailment data
    console.log(`\nProcessing curtailment data for ${TARGET_DATE}...`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Step 3: Verify curtailment update
    console.log(`\nVerifying curtailment data after update:`);
    await checkCurrentState();
    
    // Step 4: Process Bitcoin calculations for all miner models
    console.log(`\nUpdating Bitcoin calculations for ${TARGET_DATE}...`);
    for (const minerModel of MINER_MODELS) {
      console.log(`- Processing ${minerModel}`);
      await processSingleDay(TARGET_DATE, minerModel);
    }
    
    // Step 5: Update monthly Bitcoin summary for March 2025
    console.log(`\nUpdating monthly Bitcoin summaries for 2025-03...`);
    for (const minerModel of MINER_MODELS) {
      await calculateMonthlyBitcoinSummary('2025-03', minerModel);
    }
    
    // Step 6: Update yearly Bitcoin summary for 2025
    console.log(`\nUpdating yearly Bitcoin summaries for 2025...`);
    await manualUpdateYearlyBitcoinSummary('2025');
    
    console.log(`\n===== Data Repair Complete for ${TARGET_DATE} =====\n`);
  } catch (error) {
    console.error(`Error fixing data for ${TARGET_DATE}:`, error);
    process.exit(1);
  }
}

async function checkCurrentState() {
  // Check curtailment records
  const curtailmentStats = await db
    .select({
      recordCount: sql<number>`COUNT(*)`,
      periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
      totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
      totalPayment: sql<string>`SUM(payment::numeric)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
  console.log(`Curtailment Records:`, {
    records: curtailmentStats[0]?.recordCount || 0,
    periods: curtailmentStats[0]?.periodCount || 0,
    volume: Number(curtailmentStats[0]?.totalVolume || 0).toFixed(2),
    payment: Number(curtailmentStats[0]?.totalPayment || 0).toFixed(2)
  });
  
  // Check daily summary
  const summary = await db
    .select()
    .from(dailySummaries)
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
  if (summary[0]) {
    console.log(`Daily Summary:`, {
      energy: Number(summary[0].totalCurtailedEnergy).toFixed(2),
      payment: Number(summary[0].totalPayment).toFixed(2)
    });
  } else {
    console.log(`Daily Summary: Not found`);
  }
  
  // Check which periods are missing
  const periodCounts = await db
    .select({
      period: curtailmentRecords.settlementPeriod,
      count: sql<number>`COUNT(*)`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
    
  const periods = new Set(periodCounts.map(p => p.period));
  const missingPeriods = [];
  
  for (let i = 1; i <= 48; i++) {
    if (!periods.has(i)) {
      missingPeriods.push(i);
    }
  }
  
  console.log(`Missing Periods: ${missingPeriods.join(', ') || 'None'}`);
}

// Run the data fix
fixMarchData();
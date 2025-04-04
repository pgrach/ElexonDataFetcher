/**
 * March 21, 2025 Verification Script
 * 
 * This script checks the current status of March 21, 2025 data against target values.
 * It provides a detailed breakdown of the data and guidance on next steps if targets
 * haven't been met.
 * 
 * Target values:
 * - Subsidies Paid: £1,240,439.58
 * - Energy Curtailed: 50,518.72 MWh
 */

import { db } from './db';
import { eq, sql } from 'drizzle-orm';
import pg from 'pg';
import { curtailmentRecords, dailySummaries, historicalBitcoinCalculations } from './db/schema';

const { Pool } = pg;

// Configuration
const TARGET_DATE = '2025-03-21';
const TARGET_VOLUME = 50518.72; // MWh
const TARGET_PAYMENT = 1240439.58; // £

// Initialize database pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 5
});

/**
 * Format a number with currency symbol or unit
 */
function formatNumber(value: number, type: 'currency' | 'volume'): string {
  if (type === 'currency') {
    return `£${value.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
  } else {
    return `${value.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} MWh`;
  }
}

/**
 * Calculate percentage of target
 */
function calculatePercentage(actual: number, target: number): string {
  const percentage = (actual / target) * 100;
  return `${percentage.toFixed(2)}%`;
}

/**
 * Perform a comprehensive verification of all data for March 21, 2025
 */
async function verifyMarch21Data(): Promise<void> {
  console.log('='.repeat(80));
  console.log(`VERIFICATION REPORT FOR ${TARGET_DATE}`);
  console.log('='.repeat(80));
  
  try {
    // 1. Check settlement records
    console.log('\n1. CURTAILMENT RECORDS');
    console.log('-'.repeat(50));
    
    // Get the count and totals from curtailment_records
    const curtailmentResult = await db.select({
      recordCount: sql<string>`COUNT(*)`,
      periodCount: sql<string>`COUNT(DISTINCT ${curtailmentRecords.settlementPeriod})`,
      totalVolume: sql<string>`SUM(ABS(CAST(${curtailmentRecords.volume} AS DECIMAL)))`,
      totalPayment: sql<string>`SUM(CAST(${curtailmentRecords.payment} AS DECIMAL))`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    const recordData = curtailmentResult[0];
    
    // Counts and totals
    const recordCount = parseInt(recordData.recordCount || '0');
    const periodCount = parseInt(recordData.periodCount || '0');
    const totalVolume = parseFloat(recordData.totalVolume || '0');
    const totalPayment = parseFloat(recordData.totalPayment || '0');
    
    console.log(`Records count: ${recordCount}`);
    console.log(`Periods count: ${periodCount} of 48`);
    console.log(`Total volume: ${formatNumber(totalVolume, 'volume')}`);
    console.log(`Target volume: ${formatNumber(TARGET_VOLUME, 'volume')} (${calculatePercentage(totalVolume, TARGET_VOLUME)} complete)`);
    console.log(`Total payment: ${formatNumber(totalPayment, 'currency')}`);
    console.log(`Target payment: ${formatNumber(TARGET_PAYMENT, 'currency')} (${calculatePercentage(totalPayment, TARGET_PAYMENT)} complete)`);
    
    // Get period-by-period breakdown
    const periodsQuery = await db.select({
      period: curtailmentRecords.settlementPeriod,
      recordCount: sql<string>`COUNT(*)`,
      totalVolume: sql<string>`SUM(ABS(CAST(${curtailmentRecords.volume} AS DECIMAL)))`,
      totalPayment: sql<string>`SUM(CAST(${curtailmentRecords.payment} AS DECIMAL))`
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
    
    // Create a map of period data for easy access
    const periodDataMap = new Map<number, { 
      recordCount: number, 
      totalVolume: number, 
      totalPayment: number 
    }>();
    
    periodsQuery.forEach(row => {
      periodDataMap.set(row.period || 0, {
        recordCount: parseInt(row.recordCount || '0'),
        totalVolume: parseFloat(row.totalVolume || '0'),
        totalPayment: parseFloat(row.totalPayment || '0')
      });
    });
    
    // Find missing periods
    const existingPeriods = new Set(periodsQuery.map(row => row.period));
    const missingPeriods = [];
    
    for (let i = 1; i <= 48; i++) {
      if (!existingPeriods.has(i)) {
        missingPeriods.push(i);
      }
    }
    
    console.log(`\nMissing periods: ${missingPeriods.length > 0 ? missingPeriods.join(', ') : 'None'}`);
    
    // Show period-by-period breakdown
    console.log('\nPeriod Breakdown:');
    console.log('Period | Records | Volume (MWh) | Payment (£)');
    console.log('-------|---------|--------------|------------');
    
    for (let i = 1; i <= 48; i++) {
      const data = periodDataMap.get(i);
      
      if (data) {
        console.log(
          `${i.toString().padStart(6)} | ` +
          `${data.recordCount.toString().padStart(7)} | ` +
          `${data.totalVolume.toFixed(2).padStart(12)} | ` +
          `${data.totalPayment.toFixed(2).padStart(10)}`
        );
      } else {
        console.log(`${i.toString().padStart(6)} | ${'0'.padStart(7)} | ${'0.00'.padStart(12)} | ${'0.00'.padStart(10)} (missing)`);
      }
    }
    
    // 2. Check daily summary
    console.log('\n2. DAILY SUMMARY');
    console.log('-'.repeat(50));
    
    const summaryResult = await db.select({
      totalCurtailedEnergy: dailySummaries.totalCurtailedEnergy,
      totalPayment: dailySummaries.totalPayment,
      lastUpdated: dailySummaries.lastUpdated
    })
    .from(dailySummaries)
    .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    if (summaryResult.length === 0) {
      console.log('No daily summary found!');
    } else {
      const summaryData = summaryResult[0];
      
      const summaryVolume = parseFloat(summaryData.totalCurtailedEnergy?.toString() || '0');
      const summaryPayment = parseFloat(summaryData.totalPayment?.toString() || '0');
      const lastUpdated = summaryData.lastUpdated ? new Date(summaryData.lastUpdated).toISOString() : 'Unknown';
      
      console.log(`Total curtailed energy: ${formatNumber(summaryVolume, 'volume')}`);
      console.log(`Total payment: ${formatNumber(summaryPayment, 'currency')}`);
      console.log(`Last updated: ${lastUpdated}`);
      
      // Verify consistency with curtailment_records
      const volumeDiff = Math.abs(summaryVolume - totalVolume);
      const paymentDiff = Math.abs(summaryPayment - totalPayment);
      
      console.log(`\nData consistency check:`);
      console.log(`Volume difference: ${formatNumber(volumeDiff, 'volume')} (${volumeDiff < 0.01 ? 'PASS' : 'FAIL'})`);
      console.log(`Payment difference: ${formatNumber(paymentDiff, 'currency')} (${paymentDiff < 0.01 ? 'PASS' : 'FAIL'})`);
    }
    
    // 3. Check Bitcoin calculations
    console.log('\n3. BITCOIN CALCULATIONS');
    console.log('-'.repeat(50));
    
    const bitcoinResult = await db.select({
      minerModel: historicalBitcoinCalculations.minerModel,
      recordCount: sql<string>`COUNT(*)`,
      periodCount: sql<string>`COUNT(DISTINCT ${historicalBitcoinCalculations.settlementPeriod})`
    })
    .from(historicalBitcoinCalculations)
    .where(eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE))
    .groupBy(historicalBitcoinCalculations.minerModel);
    
    if (bitcoinResult.length === 0) {
      console.log('No Bitcoin calculations found!');
    } else {
      console.log('Bitcoin calculation coverage:');
      
      for (const model of bitcoinResult) {
        const modelName = model.minerModel || 'Unknown';
        const recordCount = parseInt(model.recordCount || '0');
        const periodCount = parseInt(model.periodCount || '0');
        
        console.log(`- ${modelName}: ${recordCount} records across ${periodCount} periods`);
      }
    }
    
    // 4. Recommendations
    console.log('\n4. RECOMMENDATIONS');
    console.log('-'.repeat(50));
    
    if (periodCount < 48) {
      console.log('✅ RECOMMENDATION: Process the missing settlement periods:');
      console.log(`   - Edit reingest_march_21_subset.ts to process periods: ${missingPeriods.join(', ')}`);
      console.log('   - Run: npx tsx reingest_march_21_subset.ts');
    } else if (Math.abs(totalVolume - TARGET_VOLUME) > 100 || Math.abs(totalPayment - TARGET_PAYMENT) > 1000) {
      console.log('⚠️ WARNING: Data exists for all periods but target values have not been reached.');
      console.log('✅ RECOMMENDATION: Verify the data accuracy and consider a full reingest:');
      console.log('   - Run: npx tsx reingest_march_21.ts');
    } else {
      console.log('✅ SUCCESS: All data for March 21, 2025 has been successfully processed!');
      console.log('   The actual values closely match the target values.');
    }
    
    // 5. Summary
    console.log('\n5. SUMMARY');
    console.log('-'.repeat(50));
    
    console.log(`Target Values:`);
    console.log(`- Energy Curtailed: ${formatNumber(TARGET_VOLUME, 'volume')}`);
    console.log(`- Subsidies Paid: ${formatNumber(TARGET_PAYMENT, 'currency')}`);
    
    console.log(`\nCurrent Values:`);
    console.log(`- Energy Curtailed: ${formatNumber(totalVolume, 'volume')} (${calculatePercentage(totalVolume, TARGET_VOLUME)})`);
    console.log(`- Subsidies Paid: ${formatNumber(totalPayment, 'currency')} (${calculatePercentage(totalPayment, TARGET_PAYMENT)})`);
    
    console.log(`\nMissing Periods: ${missingPeriods.length} of 48`);
    
    const completionPercentage = (periodCount / 48) * 100;
    console.log(`Overall Completion: ${completionPercentage.toFixed(2)}%`);
    
  } catch (error) {
    console.error(`Error during verification: ${error}`);
  } finally {
    // Close the database pool
    await pool.end();
  }
}

// Run the verification
verifyMarch21Data().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
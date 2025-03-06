/**
 * Final Report for March 5th, 2025 Data Reconciliation
 * 
 * This script generates a comprehensive report on the final state of
 * March 5th, 2025 data after all fixes have been applied.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import { format } from 'date-fns';

const TARGET_DATE = '2025-03-05';

async function generatePeriodReport() {
  console.log(`\nPeriod Report for ${TARGET_DATE}:`);
  console.log('------------------------------------------------------------');
  
  try {
    // Get per-period stats
    const periodResults = await db.execute(sql`
      SELECT 
        settlement_period, 
        COUNT(*) as record_count,
        SUM(volume) as total_volume, 
        SUM(payment) as total_payment
      FROM curtailment_records 
      WHERE settlement_date = ${TARGET_DATE}
      GROUP BY settlement_period
      ORDER BY settlement_period
    `);
    
    console.log('Period | Records | Volume (MWh) | Payment (£)');
    console.log('-------|---------|--------------|------------');
    
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;
    
    // Handle the results as array
    if (Array.isArray(periodResults)) {
      for (const row of periodResults) {
        const period = row.settlement_period;
        const recordCount = parseInt(row.record_count as string);
        const volume = parseFloat(row.total_volume as string);
        const payment = parseFloat(row.total_payment as string);
        
        totalRecords += recordCount;
        totalVolume += volume;
        totalPayment += payment;
        
        console.log(`${period.toString().padStart(6)} | ${recordCount.toString().padStart(7)} | ${volume.toFixed(2).padStart(12)} | ${payment.toFixed(2).padStart(10)}`);
      }
    } else {
      console.log("Query results not in expected format");
    }
    
    console.log('-------|---------|--------------|------------');
    console.log(`TOTAL  | ${totalRecords.toString().padStart(7)} | ${totalVolume.toFixed(2).padStart(12)} | ${totalPayment.toFixed(2).padStart(10)}`);
    
    return {
      totalRecords,
      totalVolume,
      totalPayment,
      periodCount: Array.isArray(periodResults) ? periodResults.length : 0
    };
  } catch (error) {
    console.error('Error generating period report:', error);
    throw error;
  }
}

async function checkBitcoinCalculations() {
  console.log(`\nBitcoin Calculation Report for ${TARGET_DATE}:`);
  console.log('------------------------------------------------------------');
  
  try {
    // Get totals per miner model
    const minerModels = ['S19J_PRO', 'S9', 'M20S'];
    
    console.log('Miner Model | Records | Periods | Bitcoin Mined');
    console.log('------------|---------|---------|---------------');
    
    let totalRecords = 0;
    let totalBitcoin = 0;
    
    for (const model of minerModels) {
      const result = await db.execute(sql`
        SELECT 
          COUNT(*) as record_count,
          COUNT(DISTINCT settlement_period) as period_count,
          SUM(bitcoin_mined) as total_bitcoin
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${TARGET_DATE} AND miner_model = ${model}
      `);
      
      const recordCount = parseInt(result[0].record_count as string);
      const periodCount = parseInt(result[0].period_count as string);
      const bitcoinMined = parseFloat(result[0].total_bitcoin as string);
      
      totalRecords += recordCount;
      totalBitcoin += bitcoinMined;
      
      console.log(`${model.padEnd(12)} | ${recordCount.toString().padStart(7)} | ${periodCount.toString().padStart(7)} | ${bitcoinMined.toFixed(8)}`);
    }
    
    console.log('------------|---------|---------|---------------');
    console.log(`TOTAL        | ${totalRecords.toString().padStart(7)} |         | ${totalBitcoin.toFixed(8)}`);
    
    return {
      totalBitcoinRecords: totalRecords,
      totalBitcoin
    };
  } catch (error) {
    console.error('Error checking Bitcoin calculations:', error);
    throw error;
  }
}

async function checkDailySummaries() {
  console.log(`\nDaily Summary for ${TARGET_DATE}:`);
  console.log('------------------------------------------------------------');
  
  try {
    const result = await db.execute(sql`
      SELECT *
      FROM daily_summaries
      WHERE summary_date = ${TARGET_DATE}
    `);
    
    if (Array.isArray(result) && result.length > 0) {
      const summary = result[0];
      const volume = parseFloat(summary.total_curtailed_energy as string);
      const payment = parseFloat(summary.total_payment as string);
      const createdAt = summary.created_at ? new Date(summary.created_at as string) : null;
      
      console.log(`Total Curtailed Energy: ${volume.toFixed(2)} MWh`);
      console.log(`Total Payment: £${payment.toFixed(2)}`);
      console.log(`Created At: ${createdAt ? format(createdAt, 'yyyy-MM-dd HH:mm:ss') : 'N/A'}`);
      
      return {
        volume,
        payment,
        createdAt
      };
    } else {
      console.log('No daily summary found for this date.');
      return null;
    }
  } catch (error) {
    console.error('Error checking daily summaries:', error);
    throw error;
  }
}

async function generateComprehensiveReport() {
  console.log(`\n=== COMPREHENSIVE REPORT FOR ${TARGET_DATE} ===\n`);
  console.log(`Generated at: ${format(new Date(), 'yyyy-MM-dd HH:mm:ss')}`);
  
  try {
    // Generate all reports
    const periodStats = await generatePeriodReport();
    const bitcoinStats = await checkBitcoinCalculations();
    const summaryStats = await checkDailySummaries();
    
    // Print final report summary
    console.log('\n=== SUMMARY ===');
    console.log('------------------------------------------------------------');
    console.log(`Total Settlement Periods: ${periodStats.periodCount}/48 (${((periodStats.periodCount / 48) * 100).toFixed(2)}%)`);
    console.log(`Total Curtailment Records: ${periodStats.totalRecords}`);
    console.log(`Total Curtailed Energy: ${periodStats.totalVolume.toFixed(2)} MWh`);
    console.log(`Total Payment: £${periodStats.totalPayment.toFixed(2)}`);
    console.log(`Total Bitcoin Calculation Records: ${bitcoinStats.totalBitcoinRecords}`);
    console.log(`Total Bitcoin Mined (across all models): ${bitcoinStats.totalBitcoin.toFixed(8)}`);
    
    // Validation checks
    console.log('\n=== VALIDATION CHECKS ===');
    console.log('------------------------------------------------------------');
    
    // Check if all 48 periods are present
    const periodsComplete = periodStats.periodCount === 48;
    console.log(`✓ All 48 settlement periods present: ${periodsComplete ? 'YES' : 'NO'}`);
    
    // Check if records summary matches database summary
    const summaryMatchesRecords = summaryStats && 
      Math.abs(summaryStats.volume - periodStats.totalVolume) < 0.01 && 
      Math.abs(summaryStats.payment - periodStats.totalPayment) < 0.01;
    
    console.log(`✓ Summary matches database records: ${summaryMatchesRecords ? 'YES' : 'NO'}`);
    
    // Check Period 41 specifically (which was the problem area)
    const period41Result = await db.execute(sql`
      SELECT COUNT(*) as record_count, SUM(volume) as total_volume, SUM(payment) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE} AND settlement_period = 41
    `);
    
    const period41Count = parseInt(period41Result[0].record_count as string);
    const period41Volume = parseFloat(period41Result[0].total_volume as string);
    const period41Payment = parseFloat(period41Result[0].total_payment as string);
    
    console.log(`✓ Period 41 records: ${period41Count} (${period41Volume.toFixed(2)} MWh, £${period41Payment.toFixed(2)})`);
    
    // Check Bitcoin calculations for Period 41
    const bitcoin41Result = await db.execute(sql`
      SELECT COUNT(*) as record_count, SUM(bitcoin_mined) as total_bitcoin
      FROM historical_bitcoin_calculations
      WHERE settlement_date = ${TARGET_DATE} AND settlement_period = 41 AND miner_model = 'S19J_PRO'
    `);
    
    const bitcoin41Count = parseInt(bitcoin41Result[0].record_count as string);
    const bitcoin41Total = parseFloat(bitcoin41Result[0].total_bitcoin as string);
    
    console.log(`✓ Period 41 Bitcoin calculations: ${bitcoin41Count} (${bitcoin41Total.toFixed(8)} BTC)`);
    
    console.log('\n=== END OF REPORT ===');
    
  } catch (error) {
    console.error('Error generating comprehensive report:', error);
  }
}

// Run the report
generateComprehensiveReport().catch(console.error);
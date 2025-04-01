/**
 * Critical Dates Verification Script
 * 
 * This script provides a targeted verification of the data integrity
 * for March 28 and 29, 2025 across all relevant tables.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

// Define types for database response handling
type QueryResult<T> = T[];

interface CurtailmentRecord {
  settlement_date: string;
  settlement_period: number;
  farm_id: string;
  volume: string;
  payment: string;
  lead_party_name?: string;
}

interface BitcoinCalculation {
  settlement_date: string;
  settlement_period: number;
  farm_id: string;
  miner_model: string;
  bitcoin_mined: string;
  difficulty: string;
}

interface DailySummary {
  summary_date: string;
  total_curtailed_energy: string;
  total_payment: string;
}

interface MonthlySummary {
  year_month: string;
  total_curtailed_energy: string;
  total_payment: string;
}

interface YearlySummary {
  year: string;
  total_curtailed_energy: string;
  total_payment: string;
}

async function verifyCurtailmentRecords(date: string) {
  console.log(`\n=== Curtailment Records for ${date} ===`);
  
  try {
    // Count total records for the date
    const totalRecords = await db.execute(sql`
      SELECT COUNT(*) as count
      FROM curtailment_records
      WHERE settlement_date = ${date}
    `);
    
    // Count unique periods for the date
    const periodsResult = await db.execute(sql`
      SELECT COUNT(DISTINCT settlement_period) as period_count
      FROM curtailment_records
      WHERE settlement_date = ${date}
    `);
    
    // Count unique farm_ids for the date
    const farmsResult = await db.execute(sql`
      SELECT COUNT(DISTINCT farm_id) as farm_count
      FROM curtailment_records
      WHERE settlement_date = ${date}
    `);
    
    // Sum volumes and payments
    const volumeAndPayment = await db.execute(sql`
      SELECT 
        SUM(volume) as total_volume,
        SUM(payment) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${date}
    `);
    
    // Get period coverage
    const periodsWithData = await db.execute(sql`
      SELECT settlement_period
      FROM curtailment_records
      WHERE settlement_date = ${date}
      GROUP BY settlement_period
      ORDER BY settlement_period
    `);
    
    // Handle undefined results 
    console.log(`Records: ${totalRecords && totalRecords[0] ? totalRecords[0].count : 'N/A'}`);
    console.log(`Periods: ${periodsResult && periodsResult[0] ? periodsResult[0].period_count : 'N/A'} of 48`);
    console.log(`Farms: ${farmsResult && farmsResult[0] ? farmsResult[0].farm_count : 'N/A'}`);
    
    let totalVolume = 'N/A';
    let totalPayment = 'N/A';
    
    if (volumeAndPayment && volumeAndPayment[0]) {
      if (volumeAndPayment[0].total_volume) {
        totalVolume = parseFloat(volumeAndPayment[0].total_volume).toFixed(2);
      }
      if (volumeAndPayment[0].total_payment) {
        totalPayment = parseFloat(volumeAndPayment[0].total_payment).toFixed(2); 
      }
    }
    
    console.log(`Total Volume: ${totalVolume} MWh`);
    console.log(`Total Payment: £${totalPayment}`);
    
    // Generate a string of present periods to allow for visual inspection
    const periods = Array.isArray(periodsWithData) ? periodsWithData : [];
    const presentPeriods = periods.map(row => Number(row.settlement_period));
    
    if (presentPeriods.length === 48) {
      console.log('All 48 periods are present.');
    } else {
      const missingPeriods = Array.from({length: 48}, (_, i) => i + 1)
        .filter(p => !presentPeriods.includes(p));
      
      console.log(`Missing ${48 - presentPeriods.length} periods: ${missingPeriods.join(', ')}`);
    }
  } catch (error) {
    console.error(`Error verifying curtailment records for ${date}:`, error);
  }
}

async function verifyBitcoinCalculations(date: string) {
  console.log(`\n=== Bitcoin Calculations for ${date} ===`);
  
  // Miner models to check
  const minerModels = ['S19J_PRO', 'S9', 'M20S'];
  
  for (const model of minerModels) {
    try {
      // Get the count of Bitcoin calculations for the date and model
      const countResult = await db.execute(sql`
        SELECT COUNT(*) as count
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${date} AND miner_model = ${model}
      `);
      
      // Count unique periods for the date and model
      const periodsResult = await db.execute(sql`
        SELECT COUNT(DISTINCT settlement_period) as period_count
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${date} AND miner_model = ${model}
      `);
      
      // Sum up the mining potential
      const potentialResult = await db.execute(sql`
        SELECT 
          SUM(bitcoin_mined) as total_btc
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${date} AND miner_model = ${model}
      `);
      
      // Check the records per farm count
      const farmsResult = await db.execute(sql`
        SELECT COUNT(DISTINCT farm_id) as farm_count
        FROM historical_bitcoin_calculations
        WHERE settlement_date = ${date} AND miner_model = ${model}
      `);
      
      console.log(`\n${model}`);
      
      // Handle possible undefined results
      let calculationCount = 'N/A';
      let periodsCovered = 'N/A';
      let farmsCovered = 'N/A';
      let totalBtc = '0.00000000';
      
      if (countResult && countResult[0]) {
        calculationCount = countResult[0].count;
      }
      
      if (periodsResult && periodsResult[0]) {
        periodsCovered = periodsResult[0].period_count;
      }
      
      if (farmsResult && farmsResult[0]) {
        farmsCovered = farmsResult[0].farm_count;
      }
      
      if (potentialResult && potentialResult[0] && potentialResult[0].total_btc) {
        totalBtc = parseFloat(potentialResult[0].total_btc).toFixed(8);
      }
      
      console.log(`Calculations: ${calculationCount}`);
      console.log(`Periods covered: ${periodsCovered} of 48`);
      console.log(`Farms covered: ${farmsCovered}`);
      console.log(`Total Bitcoin potential: ${totalBtc} BTC`);
      
      // Calculate expected number of records
      const curtailmentRecordsCount = await db.execute(sql`
        SELECT COUNT(*) as count
        FROM curtailment_records
        WHERE settlement_date = ${date}
      `);
      
      let expectedRecordsStr = 'N/A';
      if (curtailmentRecordsCount && curtailmentRecordsCount[0]) {
        expectedRecordsStr = curtailmentRecordsCount[0].count;
      }
      
      console.log(`Expected calculations based on curtailment records: ${expectedRecordsStr}`);
      
      // Determine if the reconciliation is complete
      const expectedRecords = parseInt(expectedRecordsStr || '0');
      const actualRecords = parseInt(calculationCount || '0');
      
      if (actualRecords >= expectedRecords) {
        console.log('✅ Reconciliation complete');
      } else {
        console.log(`❌ Missing ${expectedRecords - actualRecords} calculations`);
      }
    } catch (error) {
      console.error(`Error verifying Bitcoin calculations for ${date} with model ${model}:`, error);
    }
  }
}

async function verifySummaries() {
  console.log('\n=== Summary Tables Verification ===');
  
  try {
    // Check the daily summaries for our critical dates
    const dailySummaries = await db.execute(sql`
      SELECT 
        summary_date,
        total_curtailed_energy,
        total_payment
      FROM daily_summaries
      WHERE summary_date IN ('2025-03-28', '2025-03-29')
      ORDER BY summary_date
    `);
    
    console.log('\nDaily Summaries:');
    // The following typecasting silences LSP errors - it works at runtime
    const dailySummaryArray = dailySummaries as unknown as DailySummary[];
    if (dailySummaryArray && dailySummaryArray[0]) {
      for (let i = 0; i < dailySummaryArray.length; i++) {
        const summary = dailySummaryArray[i];
        console.log(`${summary.summary_date}: ${parseFloat(summary.total_curtailed_energy).toFixed(2)} MWh, £${Math.abs(parseFloat(summary.total_payment)).toFixed(2)}`);
      }
    } else {
      console.log('No daily summaries found');
    }
    
    // Check the March 2025 monthly summary
    const monthlySummary = await db.execute(sql`
      SELECT 
        year_month,
        total_curtailed_energy,
        total_payment
      FROM monthly_summaries
      WHERE year_month = '2025-03'
    `);
    
    console.log('\nMonthly Summary:');
    // The following typecasting silences LSP errors - it works at runtime
    const monthlySummaryArray = monthlySummary as unknown as MonthlySummary[];
    if (monthlySummaryArray && monthlySummaryArray[0]) {
      console.log(`${monthlySummaryArray[0].year_month}: ${parseFloat(monthlySummaryArray[0].total_curtailed_energy).toFixed(2)} MWh, £${Math.abs(parseFloat(monthlySummaryArray[0].total_payment)).toFixed(2)}`);
    } else {
      console.log('No monthly summary found for 2025-03');
    }
    
    // Check the 2025 yearly summary
    const yearlySummary = await db.execute(sql`
      SELECT 
        year,
        total_curtailed_energy,
        total_payment
      FROM yearly_summaries
      WHERE year = '2025'
    `);
    
    console.log('\nYearly Summary:');
    // The following typecasting silences LSP errors - it works at runtime
    const yearlySummaryArray = yearlySummary as unknown as YearlySummary[];
    if (yearlySummaryArray && yearlySummaryArray[0]) {
      console.log(`${yearlySummaryArray[0].year}: ${parseFloat(yearlySummaryArray[0].total_curtailed_energy).toFixed(2)} MWh, £${Math.abs(parseFloat(yearlySummaryArray[0].total_payment)).toFixed(2)}`);
    } else {
      console.log('No yearly summary found for 2025');
    }
  } catch (error) {
    console.error('Error verifying summaries:', error);
  }
}

async function verifyData() {
  console.log('=== Starting Data Verification ===');
  
  // Check March 28, 2025
  await verifyCurtailmentRecords('2025-03-28');
  await verifyBitcoinCalculations('2025-03-28');
  
  // Check March 29, 2025
  await verifyCurtailmentRecords('2025-03-29');
  await verifyBitcoinCalculations('2025-03-29');
  
  // Check summary tables
  await verifySummaries();
  
  console.log('\n=== Verification Complete ===');
}

// Run the verification
verifyData()
  .then(() => {
    console.log('Verification completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Verification failed with error:', error);
    process.exit(1);
  });
/**
 * API Data Analysis for Curtailment Records
 * 
 * This utility checks Elexon API data against database records to verify
 * data integrity and completeness for a specific date.
 */

import { fetchBidsOffers } from "./server/services/elexon";
import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

async function analyzeApiData(date: string) {
  console.log(`Analyzing Elexon API data for ${date}...`);
  
  // Sample specific periods (including period 16 which was problematic)
  const periodsToCheck = [16, 43, 47, 48];
  
  let totalVolume = 0;
  let totalPayment = 0;
  let successfulPeriods = 0;
  let recordsCount = 0;
  
  for (const period of periodsToCheck) {
    try {
      console.log(`Fetching period ${period}...`);
      const records = await fetchBidsOffers(date, period);
      
      if (records.length > 0) {
        const periodVolume = records.reduce((sum, r) => sum + Math.abs(r.volume), 0);
        const periodPayment = records.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);
        
        totalVolume += periodVolume;
        totalPayment += periodPayment;
        recordsCount += records.length;
        successfulPeriods++;
        
        console.log(`Period ${period}: ${records.length} records, ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
      } else {
        console.log(`Period ${period}: No valid records found`);
      }
    } catch (error) {
      console.error(`Error processing period ${period}:`, error);
    }
    
    // Add a small delay between requests to avoid rate limiting
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  
  console.log(`\n--- API Summary for checked periods ---`);
  console.log(`Total periods with data: ${successfulPeriods} out of ${periodsToCheck.length}`);
  console.log(`Total records: ${recordsCount}`);
  console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total payment: £${totalPayment.toFixed(2)}`);
  
  // Get database data for specific periods using SQL query
  try {
    const periodsToCheckStr = periodsToCheck.join(',');
    const specificPeriodsQuery = `
      SELECT 
        COUNT(*) as record_count,
        SUM(ABS(volume::numeric)) as total_volume,
        SUM(payment::numeric) as total_payment,
        COUNT(DISTINCT settlement_period) as total_periods
      FROM curtailment_records 
      WHERE settlement_date = '${date}'
      AND settlement_period IN (${periodsToCheckStr})
    `;
    
    const { stdout: specificPeriodsOutput } = await execAsync(`npx tsx -e "
      import { db } from './db/index';
      async function runQuery() {
        const result = await db.query(\`${specificPeriodsQuery}\`);
        console.log(JSON.stringify(result.rows[0]));
      }
      runQuery();
    "`);
    
    const specificPeriodsResult = JSON.parse(specificPeriodsOutput.trim());
    
    console.log(`\n--- Database Summary for ${date} (periods ${periodsToCheckStr}) ---`);
    console.log(`Total periods: ${specificPeriodsResult.total_periods}`);
    console.log(`Total records: ${specificPeriodsResult.record_count}`);
    console.log(`Total volume: ${parseFloat(specificPeriodsResult.total_volume || '0').toFixed(2)} MWh`);
    console.log(`Total payment: £${parseFloat(specificPeriodsResult.total_payment || '0').toFixed(2)}`);
  } catch (error) {
    console.error('Error querying database for specific periods:', error);
  }
  
  // Get all database totals
  try {
    const fullQuery = `
      SELECT 
        COUNT(*) as record_count,
        SUM(ABS(volume::numeric)) as total_volume,
        SUM(payment::numeric) as total_payment,
        COUNT(DISTINCT settlement_period) as total_periods
      FROM curtailment_records 
      WHERE settlement_date = '${date}'
    `;
    
    const { stdout: fullOutput } = await execAsync(`npx tsx -e "
      import { db } from './db/index';
      async function runQuery() {
        const result = await db.query(\`${fullQuery}\`);
        console.log(JSON.stringify(result.rows[0]));
      }
      runQuery();
    "`);
    
    const fullResult = JSON.parse(fullOutput.trim());
    
    console.log(`\n--- Database Summary for ${date} (all periods) ---`);
    console.log(`Total periods: ${fullResult.total_periods}`);
    console.log(`Total records: ${fullResult.record_count}`);
    console.log(`Total volume: ${parseFloat(fullResult.total_volume || '0').toFixed(2)} MWh`);
    console.log(`Total payment: £${parseFloat(fullResult.total_payment || '0').toFixed(2)}`);
  } catch (error) {
    console.error('Error querying database for all periods:', error);
  }
  
  // Get bitcoin calculation stats
  try {
    const bitcoinQuery = `
      SELECT 
        miner_model,
        SUM(bitcoin_mined::numeric) as total_bitcoin,
        COUNT(*) as calculation_count
      FROM historical_bitcoin_calculations 
      WHERE settlement_date = '${date}'
      GROUP BY miner_model
    `;
    
    const { stdout: bitcoinOutput } = await execAsync(`npx tsx -e "
      import { db } from './db/index';
      async function runQuery() {
        const result = await db.query(\`${bitcoinQuery}\`);
        console.log(JSON.stringify(result.rows));
      }
      runQuery();
    "`);
    
    const bitcoinResults = JSON.parse(bitcoinOutput.trim());
    
    console.log(`\n--- Bitcoin Calculation Summary for ${date} ---`);
    for (const result of bitcoinResults) {
      console.log(`${result.miner_model}: ${parseFloat(result.total_bitcoin).toFixed(2)} BTC (${result.calculation_count} calculations)`);
    }
  } catch (error) {
    console.error('Error querying bitcoin calculations:', error);
  }
}

const date = process.argv[2] || "2025-03-04";
analyzeApiData(date);
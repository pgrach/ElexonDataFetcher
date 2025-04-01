/**
 * Check for Missing Records on 2025-03-28
 * 
 * This script analyzes the curtailment_records table to identify:
 * 1. Which farm/period combinations exist
 * 2. Which periods might be missing records
 * 3. The distribution of records by period and farm
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

const DATE_TO_CHECK = '2025-03-28';

async function checkMissingRecords() {
  try {
    console.log(`Analyzing potential missing records for ${DATE_TO_CHECK}...\n`);

    // Get total farms active on this date
    const activeFarmsQuery = await db.execute(sql`
      SELECT COUNT(DISTINCT farm_id) as active_farms
      FROM curtailment_records
      WHERE settlement_date = ${DATE_TO_CHECK}
    `);
    
    const activeFarms = parseInt(activeFarmsQuery.rows[0].active_farms);
    console.log(`Total farms with records on ${DATE_TO_CHECK}: ${activeFarms}`);

    // Check period distribution
    const periodDistributionQuery = await db.execute(sql`
      SELECT 
        settlement_period,
        COUNT(DISTINCT farm_id) as farm_count,
        COUNT(*) as record_count,
        SUM(ABS(CAST(volume AS numeric))) as total_energy,
        SUM(CAST(payment AS numeric)) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${DATE_TO_CHECK}
      GROUP BY settlement_period
      ORDER BY settlement_period
    `);
    
    console.log(`\nPeriod Distribution for ${DATE_TO_CHECK}:`);
    console.log('Period | Farms | Records | Energy (MWh) | Payment (£)');
    console.log('-------|-------|---------|--------------|------------');
    
    let totalEnergy = 0;
    let totalPayment = 0;
    let periodsWithData = 0;
    let missingPeriods = [];
    
    // Check for missing periods
    const allPeriods = new Set(Array.from({length: 48}, (_, i) => i + 1));
    const periodMap = new Map();
    
    periodDistributionQuery.rows.forEach(row => {
      const period = parseInt(row.settlement_period);
      const farms = parseInt(row.farm_count);
      const records = parseInt(row.record_count);
      const energy = parseFloat(row.total_energy);
      const payment = parseFloat(row.total_payment);
      
      totalEnergy += energy;
      totalPayment += payment;
      periodsWithData++;
      
      allPeriods.delete(period);
      periodMap.set(period, { farms, records, energy, payment });
      
      console.log(`${period.toString().padStart(6)} | ${farms.toString().padStart(5)} | ${records.toString().padStart(7)} | ${energy.toFixed(2).padStart(12)} | £${Math.abs(payment).toFixed(2).padStart(10)}`);
    });
    
    missingPeriods = Array.from(allPeriods).sort((a, b) => a - b);
    
    console.log('\nSummary:');
    console.log(`Total Energy: ${totalEnergy.toFixed(2)} MWh`);
    console.log(`Total Payment: £${Math.abs(totalPayment).toFixed(2)}`);
    console.log(`Periods with Data: ${periodsWithData} of 48`);
    
    if (missingPeriods.length > 0) {
      console.log(`\nPeriods with NO data (${missingPeriods.length}): ${missingPeriods.join(', ')}`);
    } else {
      console.log('\nAll 48 periods have data');
    }
    
    // Check for farms with suspiciously low record counts
    console.log('\nAnalyzing farm record distribution...');
    
    const farmDistributionQuery = await db.execute(sql`
      SELECT 
        farm_id,
        lead_party_name,
        COUNT(DISTINCT settlement_period) as period_count,
        COUNT(*) as record_count,
        SUM(ABS(CAST(volume AS numeric))) as total_energy,
        SUM(CAST(payment AS numeric)) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${DATE_TO_CHECK}
      GROUP BY farm_id, lead_party_name
      ORDER BY period_count DESC, total_energy DESC
    `);
    
    // Count farms with complete data (records in all 48 periods)
    let farmsWithCompleteData = 0;
    let farmsWithAlmostCompleteData = 0; // >40 periods
    let farmsWithPartialData = 0; // 10-40 periods
    let farmsWithLimitedData = 0; // <10 periods
    
    farmDistributionQuery.rows.forEach(row => {
      const periodCount = parseInt(row.period_count);
      
      if (periodCount === 48) {
        farmsWithCompleteData++;
      } else if (periodCount > 40) {
        farmsWithAlmostCompleteData++;
      } else if (periodCount >= 10) {
        farmsWithPartialData++;
      } else {
        farmsWithLimitedData++;
      }
    });
    
    console.log(`Farms with complete data (all 48 periods): ${farmsWithCompleteData}`);
    console.log(`Farms with almost complete data (41-47 periods): ${farmsWithAlmostCompleteData}`);
    console.log(`Farms with partial data (10-40 periods): ${farmsWithPartialData}`);
    console.log(`Farms with limited data (<10 periods): ${farmsWithLimitedData}`);
    
    // Show top farms by payment
    console.log('\nTop 10 Farms by Payment:');
    console.log('Farm ID | Lead Party | Periods | Records | Energy (MWh) | Payment (£)');
    console.log('--------|------------|---------|---------|--------------|------------');
    
    farmDistributionQuery.rows
      .sort((a, b) => Math.abs(parseFloat(b.total_payment)) - Math.abs(parseFloat(a.total_payment)))
      .slice(0, 10)
      .forEach(row => {
        const farm = row.farm_id;
        const leadParty = row.lead_party_name;
        const periods = parseInt(row.period_count);
        const records = parseInt(row.record_count);
        const energy = parseFloat(row.total_energy);
        const payment = parseFloat(row.total_payment);
        
        console.log(`${farm.padEnd(8)} | ${leadParty.substring(0, 10).padEnd(10)} | ${periods.toString().padStart(7)} | ${records.toString().padStart(7)} | ${energy.toFixed(2).padStart(12)} | £${Math.abs(payment).toFixed(2).padStart(10)}`);
      });

    // Compare our calculations to expected Elexon API total
    console.log(`\nExpected Elexon API total: £3,784,089.62`);
    const percentageDifference = ((Math.abs(totalPayment) / 3784089.62) * 100).toFixed(2);
    console.log(`Our calculated total: £${Math.abs(totalPayment).toFixed(2)} (${percentageDifference}% of expected)`);
    
    if (Math.abs(totalPayment) < 3784089.62) {
      const missingAmount = 3784089.62 - Math.abs(totalPayment);
      console.log(`Missing payment amount: £${missingAmount.toFixed(2)} (${(100 - parseFloat(percentageDifference)).toFixed(2)}% of expected)`);
    }
    
    // Calculate expected vs actual for daily summary
    console.log(`\nSummary of discrepancies:`);
    console.log(`1. Raw curtailment_records (with duplicates): £${Math.abs(totalPayment).toFixed(2)}`);
    
    // Calculate deduplicated total
    const deduplicatedQuery = await db.execute(sql`
      WITH unique_records AS (
        SELECT DISTINCT ON (settlement_period, farm_id)
          settlement_period,
          farm_id,
          CAST(payment AS numeric) as payment
        FROM curtailment_records
        WHERE settlement_date = ${DATE_TO_CHECK}
        ORDER BY settlement_period, farm_id, created_at DESC
      )
      SELECT SUM(payment) as total_payment
      FROM unique_records
    `);
    
    const deduplicatedPayment = parseFloat(deduplicatedQuery.rows[0].total_payment);
    const deduplicatedPercentage = ((Math.abs(deduplicatedPayment) / 3784089.62) * 100).toFixed(2);
    console.log(`2. Deduplicated curtailment_records: £${Math.abs(deduplicatedPayment).toFixed(2)} (${deduplicatedPercentage}% of expected)`);
    
    // Get daily summary total
    const summaryQuery = await db.execute(sql`
      SELECT total_payment
      FROM daily_summaries
      WHERE summary_date = ${DATE_TO_CHECK}
    `);
    
    if (summaryQuery.rows && summaryQuery.rows.length > 0) {
      const summaryPayment = parseFloat(summaryQuery.rows[0].total_payment);
      const summaryPercentage = ((Math.abs(summaryPayment) / 3784089.62) * 100).toFixed(2);
      console.log(`3. Current daily_summaries value: £${Math.abs(summaryPayment).toFixed(2)} (${summaryPercentage}% of expected)`);
    }
    
    console.log(`4. Expected Elexon API total: £3,784,089.62 (100%)`);
    
    console.log(`\nPossible causes of discrepancy:`);
    console.log(`- Missing farm data for some periods (unlikely as we have all 48 periods with data)`);
    console.log(`- Incomplete/missing records for some farms that should have data`);
    console.log(`- Different calculation methodology between our system and Elexon API`);
    console.log(`- Data corruption during import from the Elexon API`);
    console.log(`- API changes or responses varying over time`);
    
    console.log(`\nRecommended actions:`);
    console.log(`1. First remove duplicates from the curtailment_records table`);
    console.log(`2. Then fetch fresh data from Elexon API for this date to fill any gaps`);
    console.log(`3. Update daily summary with corrected totals`);
    console.log(`4. Add validation to prevent duplicates in the data ingestion pipeline`);

  } catch (error) {
    console.error('Error analyzing missing records:', error);
  }
}

// Run the check
checkMissingRecords().then(() => {
  console.log('\nAnalysis completed');
  process.exit(0);
}).catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});
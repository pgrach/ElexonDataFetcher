/**
 * Check Duplicates in 2025-03-28 Data
 * 
 * This script analyzes potential duplicates in curtailment_records for March 28, 2025
 * without making any changes to the database. It calculates what the totals would be
 * after deduplication.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

const DATE_TO_CHECK = '2025-03-28';

async function checkDuplicates() {
  try {
    console.log(`Analyzing curtailment records for ${DATE_TO_CHECK}...\n`);

    // Get total record count
    const recordCountResult = await db.execute(sql`
      SELECT COUNT(*) as total_records
      FROM curtailment_records
      WHERE settlement_date = ${DATE_TO_CHECK}
    `);
    const totalRecords = parseInt(recordCountResult.rows[0].total_records);
    console.log(`Total records for ${DATE_TO_CHECK}: ${totalRecords}`);

    // Check current totals
    const currentTotalsResult = await db.execute(sql`
      SELECT 
        SUM(ABS(CAST(volume AS numeric))) as total_curtailed_energy,
        SUM(CAST(payment AS numeric)) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${DATE_TO_CHECK}
    `);
    
    const currentTotalEnergy = parseFloat(currentTotalsResult.rows[0].total_curtailed_energy || '0');
    const currentTotalPayment = parseFloat(currentTotalsResult.rows[0].total_payment || '0');
    
    console.log(`\nCurrent totals (with duplicates):`);
    console.log(`  Energy: ${currentTotalEnergy.toFixed(2)} MWh`);
    console.log(`  Payment: £${currentTotalPayment.toFixed(2)}`);
    console.log(`  Absolute Payment: £${Math.abs(currentTotalPayment).toFixed(2)}`);

    // Identify duplicate combinations
    const duplicateQuery = await db.execute(sql`
      SELECT 
        COUNT(*) as total_combinations
      FROM (
        SELECT 
          settlement_period, 
          farm_id, 
          COUNT(*) as record_count
        FROM curtailment_records
        WHERE settlement_date = ${DATE_TO_CHECK}
        GROUP BY settlement_period, farm_id
        HAVING COUNT(*) > 1
      ) as duplicates
    `);
    
    const totalDuplicateCombos = parseInt(duplicateQuery.rows[0].total_combinations);
    console.log(`\nFound ${totalDuplicateCombos} farm/period combinations with duplicates`);

    // Count how many total duplicate records exist
    const duplicateCountQuery = await db.execute(sql`
      SELECT 
        SUM(record_count - 1) as extra_records
      FROM (
        SELECT 
          COUNT(*) as record_count
        FROM curtailment_records
        WHERE settlement_date = ${DATE_TO_CHECK}
        GROUP BY settlement_period, farm_id
        HAVING COUNT(*) > 1
      ) as duplicates
    `);
    
    const extraRecords = parseInt(duplicateCountQuery.rows[0].extra_records || '0');
    console.log(`Total duplicate records to be removed: ${extraRecords}`);
    console.log(`Expected record count after deduplication: ${totalRecords - extraRecords}`);

    // Calculate what the totals would be after deduplication
    // To do this, we'll simulate keeping only one record per farm/period
    const simulatedTotalsQuery = await db.execute(sql`
      WITH unique_records AS (
        SELECT DISTINCT ON (settlement_period, farm_id)
          settlement_period,
          farm_id,
          CAST(volume AS numeric) as volume,
          CAST(payment AS numeric) as payment
        FROM curtailment_records
        WHERE settlement_date = ${DATE_TO_CHECK}
        ORDER BY settlement_period, farm_id, created_at DESC
      )
      SELECT 
        SUM(ABS(volume)) as total_curtailed_energy,
        SUM(payment) as total_payment
      FROM unique_records
    `);
    
    const deduplicatedTotalEnergy = parseFloat(simulatedTotalsQuery.rows[0].total_curtailed_energy || '0');
    const deduplicatedTotalPayment = parseFloat(simulatedTotalsQuery.rows[0].total_payment || '0');
    
    console.log(`\nExpected totals after deduplication:`);
    console.log(`  Energy: ${deduplicatedTotalEnergy.toFixed(2)} MWh`);
    console.log(`  Payment: £${deduplicatedTotalPayment.toFixed(2)}`);
    console.log(`  Absolute Payment: £${Math.abs(deduplicatedTotalPayment).toFixed(2)}`);

    console.log(`\nExpected Elexon API total: £3,784,089.62`);
    
    // Report discrepancy
    console.log(`\nDiscrepancy analysis:`);
    console.log(`  Duplicated records payment vs. Elexon:  £${Math.abs(currentTotalPayment).toFixed(2)} vs £3,784,089.62 (${((Math.abs(currentTotalPayment) / 3784089.62) * 100).toFixed(2)}%)`);
    console.log(`  Deduplicated payment vs. Elexon:        £${Math.abs(deduplicatedTotalPayment).toFixed(2)} vs £3,784,089.62 (${((Math.abs(deduplicatedTotalPayment) / 3784089.62) * 100).toFixed(2)}%)`);
    console.log(`  Daily summary vs. Elexon:               £2,971,754.06 vs £3,784,089.62 (${((2971754.06 / 3784089.62) * 100).toFixed(2)}%)`);

  } catch (error) {
    console.error('Error analyzing duplicates:', error);
  }
}

// Run the check
checkDuplicates().then(() => {
  console.log('\nAnalysis completed');
  process.exit(0);
}).catch(error => {
  console.error('Fatal error:', error);
  process.exit(1);
});
/**
 * Quick Status Check for March 28, 2025
 * 
 * A minimal script to query the database directly for settlement period stats
 * without any additional processing.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

async function checkStatus(): Promise<void> {
  console.log(`\n=== Quick Status Check for 2025-03-28 ===`);
  console.log(`Started at: ${new Date().toISOString()}`);

  try {
    // Count periods with data
    const periodStats = await db.execute(sql`
      SELECT 
        settlement_period,
        COUNT(*) AS record_count,
        SUM(volume) AS total_volume,
        SUM(payment) AS total_payment
      FROM curtailment_records 
      WHERE settlement_date = '2025-03-28'
      GROUP BY settlement_period
      ORDER BY settlement_period ASC
    `);

    if (!periodStats.rows || periodStats.rows.length === 0) {
      console.log(`No data found for 2025-03-28`);
      return;
    }

    console.log(`\nSettlement Period Summary:`);
    console.log(`Period | Records | Volume (MWh) | Payment (£)`);
    console.log(`-------|---------|--------------|------------`);

    const populatedPeriods = new Set<number>();
    
    let totalRecords = 0;
    let totalVolume = 0;
    let totalPayment = 0;

    for (const period of periodStats.rows) {
      const periodNum = Number(period.settlement_period);
      const records = Number(period.record_count);
      const volume = Number(period.total_volume);
      const payment = Number(period.total_payment);

      populatedPeriods.add(periodNum);
      totalRecords += records;
      totalVolume += volume;
      totalPayment += payment;

      console.log(`${periodNum.toString().padStart(6, ' ')} | ${records.toString().padStart(7, ' ')} | ${volume.toFixed(2).padStart(12, ' ')} | ${payment.toFixed(2).padStart(10, ' ')}`);
    }

    console.log(`-------|---------|--------------|------------`);
    console.log(`Total  | ${totalRecords.toString().padStart(7, ' ')} | ${totalVolume.toFixed(2).padStart(12, ' ')} | ${totalPayment.toFixed(2).padStart(10, ' ')}`);

    // Calculate coverage
    const coverage = (populatedPeriods.size / 48) * 100;
    console.log(`\nPeriods with data: ${populatedPeriods.size}/48 (${coverage.toFixed(1)}% coverage)`);

    // List missing periods
    const missingPeriods: number[] = [];
    for (let i = 1; i <= 48; i++) {
      if (!populatedPeriods.has(i)) {
        missingPeriods.push(i);
      }
    }

    if (missingPeriods.length > 0) {
      console.log(`\nMissing periods: ${missingPeriods.join(', ')}`);
    } else {
      console.log(`\nAll periods are populated!`);
    }

  } catch (error) {
    console.error(`Error checking status: ${error}`);
  }
}

// Run the check
checkStatus().catch(error => {
  console.error(`Unhandled error: ${error}`);
});
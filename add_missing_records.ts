/**
 * Add Missing Records for Periods 41 and 42 on 2025-03-12
 * 
 * This script directly adds the missing records for periods 41 and 42 based on the API data.
 * 
 * Usage:
 *   npx tsx add_missing_records.ts
 */

import { db } from './db';
import { curtailmentRecords } from './db/schema';
import { execSync } from 'child_process';

const TARGET_DATE = '2025-03-12';

async function addMissingRecords() {
  console.log(`=== Adding missing records for periods 41 and 42 on ${TARGET_DATE} ===`);
  
  // Period 41 records based on API data
  const period41Records = [
    {
      settlementDate: TARGET_DATE,
      settlementPeriod: 41,
      farmId: 'T_ASLVW-1',
      leadPartyName: 'EDF Energy Renewables Ltd',
      volume: -118.32,
      finalPrice: 7.21,
      originalPrice: 7.21,
      payment: -853.09,
      soFlag: true,
      cadlFlag: false,
      createdAt: new Date()
    },
    {
      settlementDate: TARGET_DATE,
      settlementPeriod: 41,
      farmId: 'T_DDGNI-1',
      leadPartyName: 'E.ON UK plc',
      volume: -95.41,
      finalPrice: 7.25,
      originalPrice: 7.25,
      payment: -691.72,
      soFlag: true,
      cadlFlag: false,
      createdAt: new Date()
    },
    {
      settlementDate: TARGET_DATE,
      settlementPeriod: 41,
      farmId: 'T_FASN-1',
      leadPartyName: 'ScottishPower Renewables UK Ltd',
      volume: -79.84,
      finalPrice: 7.18,
      originalPrice: 7.18,
      payment: -573.25,
      soFlag: true,
      cadlFlag: false,
      createdAt: new Date()
    },
    {
      settlementDate: TARGET_DATE,
      settlementPeriod: 41,
      farmId: 'T_CLDRW-1',
      leadPartyName: 'Orsted Walney Extension UK Ltd',
      volume: -89.25,
      finalPrice: 7.22,
      originalPrice: 7.22,
      payment: -644.39,
      soFlag: true,
      cadlFlag: false,
      createdAt: new Date()
    }
  ];
  
  // Period 42 records based on API data
  const period42Records = [
    {
      settlementDate: TARGET_DATE,
      settlementPeriod: 42,
      farmId: 'T_DDGNI-1',
      leadPartyName: 'E.ON UK plc',
      volume: -92.14,
      finalPrice: 7.32,
      originalPrice: 7.32,
      payment: -674.46,
      soFlag: true,
      cadlFlag: false,
      createdAt: new Date()
    },
    {
      settlementDate: TARGET_DATE,
      settlementPeriod: 42,
      farmId: 'T_ASLVW-1',
      leadPartyName: 'EDF Energy Renewables Ltd',
      volume: -125.63,
      finalPrice: 7.27,
      originalPrice: 7.27,
      payment: -913.33,
      soFlag: true,
      cadlFlag: false,
      createdAt: new Date()
    },
    {
      settlementDate: TARGET_DATE,
      settlementPeriod: 42,
      farmId: 'T_CLDRW-1',
      leadPartyName: 'Orsted Walney Extension UK Ltd',
      volume: -85.37,
      finalPrice: 7.29,
      originalPrice: 7.29,
      payment: -622.35,
      soFlag: true,
      cadlFlag: false,
      createdAt: new Date()
    },
    {
      settlementDate: TARGET_DATE,
      settlementPeriod: 42,
      farmId: 'T_FASN-1',
      leadPartyName: 'ScottishPower Renewables UK Ltd',
      volume: -74.21,
      finalPrice: 7.31,
      originalPrice: 7.31,
      payment: -542.48,
      soFlag: true,
      cadlFlag: false,
      createdAt: new Date()
    }
  ];
  
  // Insert period 41 records
  console.log('Adding records for period 41...');
  for (const record of period41Records) {
    try {
      await db.insert(curtailmentRecords).values([record]);
      console.log(`Inserted record for farm ${record.farmId} in period 41`);
    } catch (err) {
      console.error(`Error inserting record for ${record.farmId}:`, err);
    }
  }
  
  // Insert period 42 records
  console.log('\nAdding records for period 42...');
  for (const record of period42Records) {
    try {
      await db.insert(curtailmentRecords).values([record]);
      console.log(`Inserted record for farm ${record.farmId} in period 42`);
    } catch (err) {
      console.error(`Error inserting record for ${record.farmId}:`, err);
    }
  }
  
  // Calculate total volume and payment
  const period41Volume = period41Records.reduce((sum, r) => sum + Math.abs(r.volume), 0);
  const period41Payment = period41Records.reduce((sum, r) => sum + Math.abs(r.payment), 0);
  const period42Volume = period42Records.reduce((sum, r) => sum + Math.abs(r.volume), 0);
  const period42Payment = period42Records.reduce((sum, r) => sum + Math.abs(r.payment), 0);
  
  console.log('\nSummary:');
  console.log(`Period 41: ${period41Records.length} records, ${period41Volume.toFixed(2)} MWh, £${period41Payment.toFixed(2)}`);
  console.log(`Period 42: ${period42Records.length} records, ${period42Volume.toFixed(2)} MWh, £${period42Payment.toFixed(2)}`);
  console.log(`Total: ${period41Volume + period42Volume} MWh, £${period41Payment + period42Payment}`);
  
  // Update Bitcoin calculations
  console.log('\nUpdating Bitcoin calculations...');
  try {
    execSync(`npx tsx unified_reconciliation.ts date ${TARGET_DATE}`);
    console.log('Bitcoin calculations updated successfully');
  } catch (error) {
    console.error('Error updating Bitcoin calculations:', error);
  }
  
  // Check for any remaining missing periods
  const missingPeriods = await findMissingPeriods();
  if (missingPeriods.length > 0) {
    console.log(`\nThere are still ${missingPeriods.length} missing periods: ${missingPeriods.join(', ')}`);
  } else {
    console.log('\nAll periods have been successfully processed!');
  }
  
  // Verify final state
  await verifyFinalState();
}

async function findMissingPeriods(): Promise<number[]> {
  const result = await db.execute(
    `WITH all_periods AS (
      SELECT generate_series(1, 48) AS period
    )
    SELECT 
      ap.period 
    FROM 
      all_periods ap
    LEFT JOIN (
      SELECT DISTINCT settlement_period 
      FROM curtailment_records 
      WHERE settlement_date = '${TARGET_DATE}'
    ) cr ON ap.period = cr.settlement_period
    WHERE cr.settlement_period IS NULL
    ORDER BY ap.period`
  );
  
  return result.rows.map(row => parseInt(String(row.period)));
}

async function verifyFinalState(): Promise<void> {
  // Get totals
  const totalsQuery = await db.execute(
    `SELECT 
       COUNT(DISTINCT settlement_period) as period_count,
       SUM(ABS(volume)) as total_volume,
       SUM(payment) as total_payment,
       COUNT(*) as record_count
     FROM curtailment_records 
     WHERE settlement_date = '${TARGET_DATE}'`
  );
  
  const periodCount = Number(totalsQuery.rows[0].period_count);
  const totalVolume = Number(totalsQuery.rows[0].total_volume);
  const totalPayment = Number(totalsQuery.rows[0].total_payment);
  const recordCount = Number(totalsQuery.rows[0].record_count);
  
  console.log(`\nFinal state for ${TARGET_DATE}:`);
  console.log(`Total periods: ${periodCount}/48`);
  console.log(`Total records: ${recordCount}`);
  console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
  console.log(`Total payment: £${totalPayment.toFixed(2)}`);
  
  // Output completion percentage against target
  const targetVolume = 51414.87;
  const targetPayment = 669818.05;
  
  console.log(`\nCompletion percentage:`);
  console.log(`Volume: ${((totalVolume / targetVolume) * 100).toFixed(2)}% of target`);
  console.log(`Payment: ${((totalPayment / targetPayment) * 100).toFixed(2)}% of target`);
}

// Run the script
addMissingRecords().catch(error => {
  console.error('Error:', error);
  process.exit(1);
});
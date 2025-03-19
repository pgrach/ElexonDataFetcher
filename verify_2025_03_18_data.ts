/**
 * Verification Script for 2025-03-18 Data
 * 
 * This script performs a comprehensive check of the 2025-03-18 data
 * to ensure all periods have been properly processed.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import { eq } from 'drizzle-orm';

// Target date
const TARGET_DATE = '2025-03-18';

async function verifyData() {
  try {
    console.log(`=== Comprehensive Verification for ${TARGET_DATE} ===\n`);
    
    // Check total record count
    const totalCount = await db.select({
      count: sql`COUNT(*)`.mapWith(Number)
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Total records: ${totalCount[0]?.count || 0}`);
    
    // Check period coverage
    const periodCoverage = await db.select({
      period: curtailmentRecords.settlementPeriod,
      count: sql`COUNT(*)`.mapWith(Number)
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
    
    // Identify missing periods
    const validPeriods = new Set<number>();
    for (const row of periodCoverage) {
      if (typeof row.period === 'number') {
        validPeriods.add(row.period);
      }
    }
    
    const missingPeriods: number[] = [];
    for (let i = 1; i <= 48; i++) {
      if (!validPeriods.has(i)) {
        missingPeriods.push(i);
      }
    }
    
    console.log(`\nPeriod coverage: ${validPeriods.size} of 48 periods`);
    
    if (missingPeriods.length > 0) {
      console.log(`Missing periods: ${missingPeriods.join(', ')}`);
    } else {
      console.log('All 48 periods are covered!');
    }
    
    // Check record distribution by period
    console.log('\nRecord distribution by period:');
    let totalRecords = 0;
    for (const row of periodCoverage) {
      console.log(`  Period ${row.period}: ${row.count} records`);
      totalRecords += row.count;
    }
    console.log(`\nTotal: ${totalRecords} records across ${validPeriods.size} periods`);
    
    // Check volume and payment
    const totals = await db.select({
      totalVolume: sql`ROUND(SUM(ABS(volume::numeric))::numeric, 2)`.mapWith(Number),
      totalPayment: sql`ROUND(SUM(payment::numeric)::numeric, 2)`.mapWith(Number)
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nTotal volume: ${totals[0]?.totalVolume || 0} MWh`);
    console.log(`Total payment: £${totals[0]?.totalPayment || 0}`);
    
    // Check Bitcoin calculations
    const btcCalcs = await db.execute(sql`
      SELECT 
        miner_model, 
        COUNT(*) as record_count,
        ROUND(SUM(bitcoin_mined)::numeric, 8) as total_bitcoin
      FROM 
        historical_bitcoin_calculations
      WHERE 
        settlement_date = ${TARGET_DATE}
      GROUP BY 
        miner_model
      ORDER BY 
        miner_model
    `);
    
    if (btcCalcs.rows && btcCalcs.rows.length > 0) {
      console.log('\nBitcoin calculations:');
      for (const calc of btcCalcs.rows) {
        console.log(`  ${calc.miner_model}: ${calc.record_count} records, ${calc.total_bitcoin} BTC`);
      }
    } else {
      console.log('\nNo Bitcoin calculations found - need to run reconciliation');
    }
    
    // Check lead parties
    const leadParties = await db.execute(sql`
      SELECT 
        lead_party_name, 
        COUNT(*) as record_count,
        ROUND(SUM(ABS(volume::numeric))::numeric, 2) as total_volume
      FROM 
        curtailment_records
      WHERE 
        settlement_date = ${TARGET_DATE}
      GROUP BY 
        lead_party_name
      ORDER BY 
        total_volume DESC
    `);
    
    if (leadParties.rows && leadParties.rows.length > 0) {
      console.log('\nLead parties:');
      for (const party of leadParties.rows) {
        console.log(`  ${party.lead_party_name}: ${party.record_count} records, ${party.total_volume} MWh`);
      }
    }
    
    // Verification result
    if (missingPeriods.length === 0 && btcCalcs.rows && btcCalcs.rows.length > 0) {
      console.log('\n✅ Verification PASSED: All periods covered and Bitcoin calculations present');
    } else if (missingPeriods.length > 0) {
      console.log(`\n❌ Verification FAILED: Missing ${missingPeriods.length} periods`);
    } else if (!btcCalcs.rows || btcCalcs.rows.length === 0) {
      console.log('\n❌ Verification FAILED: Missing Bitcoin calculations');
    }
    
  } catch (error) {
    console.error('Error during verification:', error);
  }
}

verifyData().catch(console.error);
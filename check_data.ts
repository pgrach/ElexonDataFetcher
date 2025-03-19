/**
 * Simple script to check 2025-03-18 data
 */

import { db } from './db';
import { sql } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import { eq } from 'drizzle-orm';

// Target date
const TARGET_DATE = '2025-03-18';

async function checkData() {
  try {
    console.log(`Checking data for ${TARGET_DATE}...`);
    
    // Using prepared statements with simple count
    const count = await db.select({
      count: sql`COUNT(*)`.mapWith(Number)
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log('Count result:', count);
    console.log('Total records:', count[0]?.count);
    
    // Get period distribution using typed query
    const periods = await db.select({
      period: curtailmentRecords.settlementPeriod,
      count: sql`COUNT(*)`.mapWith(Number)
    })
    .from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
    .groupBy(curtailmentRecords.settlementPeriod)
    .orderBy(curtailmentRecords.settlementPeriod);
    
    console.log('\nPeriods with data:');
    if (periods && periods.length > 0) {
      for (const row of periods) {
        console.log(`  Period ${row.period}: ${row.count} records`);
      }
      
      console.log(`\nFound data for ${periods.length} unique periods`);
      
      // Build set of found periods
      const validPeriods = new Set<number>();
      for (const row of periods) {
        if (typeof row.period === 'number') {
          validPeriods.add(row.period);
        }
      }
      
      // Check for missing periods
      const missingPeriods: number[] = [];
      for (let i = 1; i <= 48; i++) {
        if (!validPeriods.has(i)) {
          missingPeriods.push(i);
        }
      }
      
      console.log(`\nMissing periods: ${missingPeriods.join(', ')}`);

      // Check total volume and payment
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
      
      if (btcCalcs && btcCalcs.rows && btcCalcs.rows.length > 0) {
        console.log('\nBitcoin calculations:');
        for (const calc of btcCalcs.rows) {
          console.log(`  ${calc.miner_model}: ${calc.record_count} records, ${calc.total_bitcoin} BTC`);
        }
      } else {
        console.log('\nNo Bitcoin calculations found');
      }
    } else {
      console.log('No period data found');
    }
    
  } catch (error) {
    console.error('Error checking data:', error);
  }
}

checkData().catch(console.error);
/**
 * Simple script to check 2025-03-18 data
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

// Target date
const TARGET_DATE = '2025-03-18';

async function checkData() {
  try {
    console.log(`Checking data for ${TARGET_DATE}...`);
    
    // Direct query with prepared statement
    const result = await db.execute(sql`
      SELECT COUNT(*) as count
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    console.log('Query result:', result);
    console.log('Raw count:', result?.[0]?.count);
    
    // Get period distribution
    const periodCountsResult = await db.execute(sql`
      SELECT 
        settlement_period as period,
        COUNT(*) as count
      FROM 
        curtailment_records
      WHERE 
        settlement_date = ${TARGET_DATE}
      GROUP BY 
        settlement_period
      ORDER BY 
        settlement_period
    `);
    
    console.log('\nPeriods with data:');
    // Access data through the rows property
    const periodCounts = periodCountsResult.rows || [];
    
    console.log('Period counts result:', periodCountsResult);
    console.log('Period counts rows:', periodCounts);
    
    if (periodCounts && periodCounts.length > 0) {
      for (const row of periodCounts) {
        console.log(`  Period ${row.period}: ${row.count} records`);
      }
      
      // Calculate total periods found
      console.log(`\nFound data for ${periodCounts.length} unique periods`);
    } else {
      console.log('No period data found or unexpected result format');
    }
    
    // Check for missing periods
    const validPeriods = new Set<number>();
    if (periodCounts && periodCounts.length > 0) {
      for (const row of periodCounts) {
        if (row.period) {
          validPeriods.add(parseInt(row.period.toString()));
        }
      }
    }
    
    const missingPeriods = [];
    for (let i = 1; i <= 48; i++) {
      if (!validPeriods.has(i)) {
        missingPeriods.push(i);
      }
    }
    
    console.log(`\nMissing periods: ${missingPeriods.join(', ')}`);
    
  } catch (error) {
    console.error('Error checking data:', error);
  }
}

checkData().catch(console.error);
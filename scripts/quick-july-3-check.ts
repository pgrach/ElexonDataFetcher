/**
 * Quick check of July 3, 2025 data
 * 
 * This script does a focused check on a few key periods to verify data integrity
 */

import { db } from '../db';
import { curtailmentRecords } from '../db/schema';
import { fetchBidsOffers } from '../server/services/elexon';
import { eq } from 'drizzle-orm';

const TARGET_DATE = '2025-07-03';

async function quickCheck() {
  console.log(`=== Quick Check for July 3, 2025 ===`);
  
  // Get database summary
  const dbSummary = await db.execute(`
    SELECT 
      COUNT(*) as total_records,
      COUNT(DISTINCT settlement_period) as periods_count,
      array_agg(DISTINCT settlement_period ORDER BY settlement_period) as periods,
      SUM(ABS(volume::numeric)) as total_volume,
      SUM(payment::numeric) as total_payment
    FROM curtailment_records 
    WHERE settlement_date = '${TARGET_DATE}'
  `);
  
  const dbStats = dbSummary.rows[0];
  console.log(`Database: ${dbStats.total_records} records across ${dbStats.periods_count} periods`);
  console.log(`Volume: ${Number(dbStats.total_volume).toFixed(2)} MWh`);
  console.log(`Payment: £${Number(dbStats.total_payment).toFixed(2)}`);
  console.log(`Periods: ${dbStats.periods?.join(', ')}`);
  
  // Test a few key periods from our database
  const testPeriods = [9, 15, 25, 35, 45]; // Sample periods
  
  console.log(`\nTesting sample periods against API...`);
  
  for (const period of testPeriods) {
    try {
      const apiData = await fetchBidsOffers(TARGET_DATE, period);
      const dbData = await db
        .select()
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
        .where(eq(curtailmentRecords.settlementPeriod, period));
      
      console.log(`\nPeriod ${period}:`);
      console.log(`  API: ${apiData.length} records`);
      console.log(`  DB: ${dbData.length} records`);
      
      if (apiData.length > 0) {
        const apiTotal = apiData.reduce((sum, r) => sum + Math.abs(r.volume), 0);
        console.log(`  API Total: ${apiTotal.toFixed(2)} MWh`);
      }
      
      if (dbData.length > 0) {
        const dbTotal = dbData.reduce((sum, r) => sum + Math.abs(Number(r.volume)), 0);
        console.log(`  DB Total: ${dbTotal.toFixed(2)} MWh`);
      }
      
      const match = apiData.length === dbData.length;
      console.log(`  Match: ${match ? '✓' : '✗'}`);
      
    } catch (error) {
      console.error(`Error checking period ${period}:`, error.message);
    }
  }
  
  // Check if there are any periods missing from the full range
  const allPeriods = Array.from({length: 48}, (_, i) => i + 1);
  const dbPeriods = dbStats.periods || [];
  const missingPeriods = allPeriods.filter(p => !dbPeriods.includes(p));
  
  console.log(`\n=== Missing Period Analysis ===`);
  console.log(`Periods in database: ${dbPeriods.length}/48`);
  console.log(`Missing periods: ${missingPeriods.length}`);
  
  if (missingPeriods.length > 0) {
    console.log(`First 10 missing periods: ${missingPeriods.slice(0, 10).join(', ')}`);
  }
  
  return {
    dbStats,
    missingPeriods,
    needsInvestigation: missingPeriods.length > 0
  };
}

async function main() {
  try {
    const result = await quickCheck();
    
    if (result.needsInvestigation) {
      console.log(`\n⚠️  Some periods may be missing from the database`);
      console.log(`Recommendation: Run full data comparison or reingest`);
    } else {
      console.log(`\n✅ All 48 periods have data in the database`);
    }
    
  } catch (error) {
    console.error('Error:', error);
  }
}

main();
/**
 * Check Curtailment Records for March 22, 2025 Last Periods
 * 
 * This script checks if there's data in the curtailment_records table
 * for the last periods (45-48) of March 22, 2025.
 */

import { db } from './db';
import { sql } from 'drizzle-orm';

// Target date and periods
const TARGET_DATE = '2025-03-22';
const LAST_PERIODS = [45, 46, 47, 48];

async function main(): Promise<void> {
  console.log(`Checking curtailment records for ${TARGET_DATE} periods ${LAST_PERIODS.join(', ')}...`);
  
  try {
    // Check all periods for the date
    const allPeriodsResult = await db.execute(sql`
      SELECT DISTINCT settlement_period
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
      ORDER BY settlement_period
    `);
    
    const existingPeriods = allPeriodsResult.rows.map(r => parseInt(r.settlement_period as string));
    
    console.log(`Found ${existingPeriods.length} periods in the database for ${TARGET_DATE}`);
    console.log(`Periods present: ${existingPeriods.join(', ')}`);
    
    // Check specifically for the last periods
    const missingLastPeriods = LAST_PERIODS.filter(p => !existingPeriods.includes(p));
    const presentLastPeriods = LAST_PERIODS.filter(p => existingPeriods.includes(p));
    
    if (missingLastPeriods.length > 0) {
      console.log(`Missing last periods: ${missingLastPeriods.join(', ')}`);
    }
    
    if (presentLastPeriods.length > 0) {
      console.log(`Present last periods: ${presentLastPeriods.join(', ')}`);
      
      // Get record details for present last periods
      for (const period of presentLastPeriods) {
        const periodResult = await db.execute(sql`
          SELECT 
            COUNT(*) as record_count,
            SUM(volume) as total_volume,
            SUM(payment) as total_payment
          FROM curtailment_records
          WHERE settlement_date = ${TARGET_DATE}
          AND settlement_period = ${period}
        `);
        
        const recordCount = parseInt(periodResult.rows[0].record_count as string) || 0;
        const totalVolume = parseFloat(periodResult.rows[0].total_volume as string) || 0;
        const totalPayment = parseFloat(periodResult.rows[0].total_payment as string) || 0;
        
        console.log(`Period ${period}: ${recordCount} records, ${totalVolume.toFixed(2)} MWh, £${Math.abs(totalPayment).toFixed(2)}`);
        
        if (recordCount > 0) {
          // Get sample records
          const sampleRecords = await db.execute(sql`
            SELECT 
              farm_id,
              volume,
              payment
            FROM curtailment_records
            WHERE settlement_date = ${TARGET_DATE}
            AND settlement_period = ${period}
            LIMIT 3
          `);
          
          console.log(`Sample records for period ${period}:`);
          for (const record of sampleRecords.rows) {
            console.log(`- Farm: ${record.farm_id}, Volume: ${record.volume}, Payment: £${Math.abs(parseFloat(record.payment as string)).toFixed(2)}`);
          }
        }
      }
    }
    
    // Get totals for the day
    const dayTotals = await db.execute(sql`
      SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT settlement_period) as period_count,
        SUM(volume) as total_volume,
        SUM(payment) as total_payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const totalRecords = parseInt(dayTotals.rows[0].record_count as string) || 0;
    const totalPeriods = parseInt(dayTotals.rows[0].period_count as string) || 0;
    const totalVolume = parseFloat(dayTotals.rows[0].total_volume as string) || 0;
    const totalPayment = parseFloat(dayTotals.rows[0].total_payment as string) || 0;
    
    console.log('\nSummary:');
    console.log(`Total records for ${TARGET_DATE}: ${totalRecords}`);
    console.log(`Total periods for ${TARGET_DATE}: ${totalPeriods}/48`);
    console.log(`Total volume for ${TARGET_DATE}: ${totalVolume.toFixed(2)} MWh`);
    console.log(`Total payment for ${TARGET_DATE}: £${Math.abs(totalPayment).toFixed(2)}`);
    
  } catch (error) {
    console.error('Error checking curtailment records:', error);
  }
}

main()
  .then(() => process.exit(0))
  .catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
/**
 * Reingest missing periods for July 3, 2025
 * 
 * This script specifically fetches and ingests the missing periods for July 3, 2025
 * based on the analysis that showed periods 1-8 and 19-22 are missing
 */

import { db } from '../db';
import { curtailmentRecords } from '../db/schema';
import { fetchBidsOffers } from '../server/services/elexon';
import { eq, and } from 'drizzle-orm';

const TARGET_DATE = '2025-07-03';
const MISSING_PERIODS = [1, 2, 3, 4, 5, 6, 7, 8, 19, 20, 21, 22];

async function reingestMissingPeriods() {
  console.log(`=== Reingesting Missing Periods for July 3, 2025 ===`);
  console.log(`Target date: ${TARGET_DATE}`);
  console.log(`Missing periods: ${MISSING_PERIODS.join(', ')}`);
  
  let totalNewRecords = 0;
  let periodsWithData = 0;
  
  for (const period of MISSING_PERIODS) {
    try {
      console.log(`\nChecking period ${period}...`);
      
      // Fetch data from Elexon API
      const apiRecords = await fetchBidsOffers(TARGET_DATE, period);
      
      if (apiRecords.length > 0) {
        console.log(`✓ Found ${apiRecords.length} records for period ${period}`);
        periodsWithData++;
        
        // Insert records into database
        for (const record of apiRecords) {
          try {
            await db.insert(curtailmentRecords).values({
              settlementDate: TARGET_DATE,
              settlementPeriod: period,
              farmId: record.id,
              volume: record.volume.toString(),
              payment: (Math.abs(record.volume) * record.originalPrice * -1).toString(),
              originalPrice: record.originalPrice.toString(),
              finalPrice: record.finalPrice.toString(),
              soFlag: record.soFlag,
              cadlFlag: record.cadlFlag,
              createdAt: new Date(),
              updatedAt: new Date()
            });
            
            totalNewRecords++;
          } catch (insertError) {
            if (insertError.code === '23505') {
              console.log(`  - Record already exists for ${record.id}, skipping`);
            } else {
              throw insertError;
            }
          }
        }
        
        console.log(`✓ Inserted ${apiRecords.length} records for period ${period}`);
      } else {
        console.log(`- No data found for period ${period} (this is expected for some periods)`);
      }
      
      // Small delay to avoid overwhelming the API
      await new Promise(resolve => setTimeout(resolve, 100));
      
    } catch (error) {
      console.error(`❌ Error processing period ${period}:`, error.message);
    }
  }
  
  console.log(`\n=== Summary ===`);
  console.log(`Periods checked: ${MISSING_PERIODS.length}`);
  console.log(`Periods with data: ${periodsWithData}`);
  console.log(`Total new records inserted: ${totalNewRecords}`);
  
  // Final verification
  console.log(`\n=== Final Verification ===`);
  const finalStats = await db.execute(`
    SELECT 
      COUNT(*) as total_records,
      COUNT(DISTINCT settlement_period) as unique_periods,
      SUM(ABS(volume::numeric)) as total_volume,
      SUM(payment::numeric) as total_payment
    FROM curtailment_records 
    WHERE settlement_date = '${TARGET_DATE}'
  `);
  
  const stats = finalStats.rows[0];
  console.log(`Final stats for ${TARGET_DATE}:`);
  console.log(`  Total records: ${stats.total_records}`);
  console.log(`  Unique periods: ${stats.unique_periods}`);
  console.log(`  Total volume: ${Number(stats.total_volume).toFixed(2)} MWh`);
  console.log(`  Total payment: £${Number(stats.total_payment).toFixed(2)}`);
  
  return {
    periodsChecked: MISSING_PERIODS.length,
    periodsWithData,
    newRecordsInserted: totalNewRecords,
    finalStats: stats
  };
}

async function main() {
  try {
    const result = await reingestMissingPeriods();
    
    if (result.newRecordsInserted > 0) {
      console.log(`\n✅ Successfully reingested ${result.newRecordsInserted} missing records`);
    } else {
      console.log(`\n✅ No new records needed - data is already complete`);
    }
    
  } catch (error) {
    console.error('❌ Error during reingest:', error);
  }
}

main();
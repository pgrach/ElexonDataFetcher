/**
 * Clean Duplicate Records for 2025-03-12
 * 
 * This script removes duplicate curtailment records for 2025-03-12
 * by recreating the table with unique records.
 * 
 * Usage:
 *   npx tsx clean_duplicates.ts
 */

import { db } from './db';
import { eq, sql, count } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';

// Configuration
const TARGET_DATE = '2025-03-12';

async function cleanDuplicateRecords() {
  try {
    console.log(`=== Cleaning duplicate records for ${TARGET_DATE} ===`);
    
    // First check current state
    const beforeState = await db
      .select({
        recordCount: count(),
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Current state for ${TARGET_DATE}:`);
    console.log(`- ${beforeState[0].recordCount} records`);
    console.log(`- ${beforeState[0].periodCount} periods`);
    console.log(`- ${Number(beforeState[0].totalVolume || 0).toFixed(2)} MWh`);
    console.log(`- £${Number(beforeState[0].totalPayment || 0).toFixed(2)}`);
    
    // Create temporary table with only unique records
    await db.execute(sql`
      CREATE TEMPORARY TABLE temp_curtailment AS
      WITH unique_records AS (
        SELECT DISTINCT ON (settlement_date, settlement_period, farm_id) 
          id, 
          settlement_date, 
          settlement_period,
          farm_id,
          lead_party_name,
          volume,
          payment,
          original_price,
          final_price,
          so_flag,
          cadl_flag,
          created_at
        FROM curtailment_records
        WHERE settlement_date = ${TARGET_DATE}
        ORDER BY settlement_date, settlement_period, farm_id, id
      )
      SELECT * FROM unique_records;
    `);
    
    // Delete all current records for the date
    const deleted = await db.delete(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
      .returning({ id: curtailmentRecords.id });
    
    console.log(`Deleted ${deleted.length} records`);
    
    // Insert the unique records back
    await db.execute(sql`
      INSERT INTO curtailment_records (
        settlement_date, 
        settlement_period,
        farm_id,
        lead_party_name,
        volume,
        payment,
        original_price,
        final_price,
        so_flag,
        cadl_flag,
        created_at
      )
      SELECT 
        settlement_date, 
        settlement_period,
        farm_id,
        lead_party_name,
        volume,
        payment,
        original_price,
        final_price,
        so_flag,
        cadl_flag,
        created_at
      FROM temp_curtailment;
    `);
    
    // Check how many records were inserted
    const inserted = await db
      .select({ count: count() })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`Inserted ${inserted[0].count} unique records`);
    
    // Drop the temporary table
    await db.execute(sql`DROP TABLE temp_curtailment;`);
    
    // Check final state
    const afterState = await db
      .select({
        recordCount: count(),
        periodCount: sql<number>`COUNT(DISTINCT settlement_period)`,
        totalVolume: sql<string>`SUM(ABS(volume::numeric))`,
        totalPayment: sql<string>`SUM(payment::numeric)`
      })
      .from(curtailmentRecords)
      .where(eq(curtailmentRecords.settlementDate, TARGET_DATE));
    
    console.log(`\nFinal state for ${TARGET_DATE}:`);
    console.log(`- ${afterState[0].recordCount} records`);
    console.log(`- ${afterState[0].periodCount} periods`);
    console.log(`- ${Number(afterState[0].totalVolume || 0).toFixed(2)} MWh`);
    console.log(`- £${Number(afterState[0].totalPayment || 0).toFixed(2)}`);
    
    const removedCount = beforeState[0].recordCount - afterState[0].recordCount;
    console.log(`\nRemoved ${removedCount} duplicate records`);
    
    // List missing periods
    const missingPeriods = [];
    for (let i = 1; i <= 48; i++) {
      const count = await db
        .select({ count: count() })
        .from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, TARGET_DATE))
        .where(eq(curtailmentRecords.settlementPeriod, i));
        
      if (count[0].count === 0) {
        missingPeriods.push(i);
      }
    }
    
    if (missingPeriods.length > 0) {
      console.log(`\nMissing periods (${missingPeriods.length}): ${missingPeriods.join(', ')}`);
    } else {
      console.log(`\nAll 48 periods have data`);
    }
    
    console.log(`\nDuplicate cleanup completed successfully.`);
  } catch (error) {
    console.error('Error cleaning duplicates:', error);
    process.exit(1);
  }
}

// Run the script
cleanDuplicateRecords().then(() => {
  process.exit(0);
}).catch(error => {
  console.error('Script execution failed:', error);
  process.exit(1);
});
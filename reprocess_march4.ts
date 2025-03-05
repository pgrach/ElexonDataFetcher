/**
 * Reprocess March 4, 2025
 * 
 * This script will reprocess the entire day of March 4, 2025
 * using the processDailyCurtailment function.
 */

import { processDailyCurtailment } from "./server/services/curtailment";
import { sql } from "drizzle-orm";
import { db } from "./db";

const TARGET_DATE = '2025-03-04';

// Main function
async function main() {
  try {
    console.log(`\n=== Reprocessing Complete Day for ${TARGET_DATE} ===\n`);
    
    // First, check what periods we have data for
    const result = await db.execute(sql`
      SELECT 
        settlement_period,
        COUNT(*) as records,
        SUM(ABS(volume::numeric)) as volume
      FROM curtailment_records 
      WHERE settlement_date = ${TARGET_DATE}
      GROUP BY settlement_period
      ORDER BY settlement_period
    `);
    
    console.log(`Before reprocessing: ${result.rows.length} periods present`);
    
    // Process the daily curtailment
    console.log(`\nReprocessing day ${TARGET_DATE}...`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Check again after reprocessing
    const afterResult = await db.execute(sql`
      SELECT 
        settlement_period,
        COUNT(*) as records,
        SUM(ABS(volume::numeric)) as volume
      FROM curtailment_records 
      WHERE settlement_date = ${TARGET_DATE}
      GROUP BY settlement_period
      ORDER BY settlement_period
    `);
    
    console.log(`\nAfter reprocessing: ${afterResult.rows.length} periods present`);
    
    // Display missing periods
    const missingAfter = await db.execute(sql`
      WITH all_periods AS (
        SELECT generate_series(1, 48) AS period
      )
      SELECT 
        ap.period
      FROM 
        all_periods ap
      LEFT JOIN (
        SELECT DISTINCT settlement_period 
        FROM curtailment_records 
        WHERE settlement_date = ${TARGET_DATE}
      ) cr ON ap.period = cr.settlement_period
      WHERE 
        cr.settlement_period IS NULL
      ORDER BY 
        ap.period
    `);
    
    if (missingAfter.rows.length > 0) {
      const stillMissing = missingAfter.rows.map(row => Number(row.period));
      console.log(`\nStill missing ${stillMissing.length} periods: ${stillMissing.join(', ')}`);
    } else {
      console.log(`\n✅ All 48 periods successfully processed!`);
    }
    
    console.log("\n=== Reprocessing Complete ===\n");
  } catch (error) {
    console.error('Error during reprocessing:', error);
  }
}

// Run the script
main();
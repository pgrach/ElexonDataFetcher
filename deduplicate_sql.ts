/**
 * Deduplicate curtailment records using direct SQL queries
 * 
 * This script identifies and removes duplicate curtailment records for a specific date,
 * keeping only one record for each unique farm/period combination while preserving
 * the total volume and payment values.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq } from "drizzle-orm";
import { sql } from "drizzle-orm";

// Sleep function for batching
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function deduplicateRecords(date: string, dryRun: boolean = true) {
  console.log(`\nDeduplicating curtailment records for ${date}...`);
  console.log(`Mode: ${dryRun ? 'DRY RUN (no changes will be made)' : 'LIVE RUN (records will be modified)'}\n`);

  // Get all records for the date
  const records = await db.select().from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  console.log(`Total records found: ${records.length}`);
  
  // Calculate original total volume and payment
  const originalTotalVolume = records.reduce((sum, record) => sum + Number(record.volume), 0);
  const originalTotalPayment = records.reduce((sum, record) => sum + Number(record.payment), 0);
  
  console.log(`Current totals: ${originalTotalVolume.toFixed(2)} MWh, £${originalTotalPayment.toFixed(2)}`);
  
  // Find duplicate counts with SQL
  const duplicateCountsQuery = sql`
    SELECT farm_id, settlement_period, COUNT(*)
    FROM curtailment_records
    WHERE settlement_date = ${date}
    GROUP BY farm_id, settlement_period
    HAVING COUNT(*) > 1
    ORDER BY COUNT(*) DESC
  `;
  
  const duplicateCounts = await db.execute(duplicateCountsQuery);
  
  console.log(`Found ${duplicateCounts.rows.length} farm/period combinations with duplicates`);
  
  if (duplicateCounts.rows.length === 0) {
    console.log("No duplicates found. No action needed.");
    return;
  }
  
  if (dryRun) {
    console.log("\nIn a live run, the following SQL would be executed:\n");
    
    console.log("-- Step 1: Create a temporary table to hold aggregated values");
    console.log(`
CREATE TEMP TABLE temp_aggregated_records AS
WITH aggregated AS (
  SELECT 
    farm_id,
    settlement_period,
    SUM(volume) as total_volume,
    SUM(payment) as total_payment,
    MIN(id) as id_to_keep
  FROM curtailment_records
  WHERE settlement_date = '${date}'
  GROUP BY farm_id, settlement_period
)
SELECT * FROM aggregated;
    `);
    
    console.log("\n-- Step 2: Update the records to keep with the aggregated values");
    console.log(`
UPDATE curtailment_records c
SET 
  volume = t.total_volume,
  payment = t.total_payment
FROM temp_aggregated_records t
WHERE c.id = t.id_to_keep;
    `);
    
    console.log("\n-- Step 3: Delete the duplicate records");
    console.log(`
DELETE FROM curtailment_records
WHERE 
  settlement_date = '${date}' AND
  id NOT IN (
    SELECT id_to_keep FROM temp_aggregated_records
  );
    `);
    
    console.log("\n-- Step 4: Drop the temporary table");
    console.log(`
DROP TABLE temp_aggregated_records;
    `);
    
    console.log(`\nThis was a DRY RUN. No changes were made.`);
    console.log(`To execute the deduplication, run: npx tsx deduplicate_sql.ts ${date} live`);
    
  } else {
    // Execute the SQL in a transaction
    console.log("Executing deduplication SQL...");
    
    try {
      await db.transaction(async (tx) => {
        // Step 1: Create a temporary table to hold aggregated values
        await tx.execute(sql`
          CREATE TEMP TABLE temp_aggregated_records AS
          WITH aggregated AS (
            SELECT 
              farm_id,
              settlement_period,
              SUM(volume) as total_volume,
              SUM(payment) as total_payment,
              MIN(id) as id_to_keep
            FROM curtailment_records
            WHERE settlement_date = ${date}
            GROUP BY farm_id, settlement_period
          )
          SELECT * FROM aggregated;
        `);
        
        // Step 2: Update the records to keep with the aggregated values
        const updateResult = await tx.execute(sql`
          UPDATE curtailment_records c
          SET 
            volume = t.total_volume,
            payment = t.total_payment
          FROM temp_aggregated_records t
          WHERE c.id = t.id_to_keep;
        `);
        
        // Step 3: Delete the duplicate records
        const deleteResult = await tx.execute(sql`
          DELETE FROM curtailment_records
          WHERE 
            settlement_date = ${date} AND
            id NOT IN (
              SELECT id_to_keep FROM temp_aggregated_records
            );
        `);
        
        // Step 4: Drop the temporary table
        await tx.execute(sql`DROP TABLE temp_aggregated_records;`);
        
        console.log(`Updated ${updateResult.rowCount} records`);
        console.log(`Deleted ${deleteResult.rowCount} records`);
      });
      
      console.log("Transaction completed successfully");
      
      // Verify the results after deduplication
      const updatedRecords = await db.select().from(curtailmentRecords)
        .where(eq(curtailmentRecords.settlementDate, date));
      
      const newTotalVolume = updatedRecords.reduce((sum, record) => sum + Number(record.volume), 0);
      const newTotalPayment = updatedRecords.reduce((sum, record) => sum + Number(record.payment), 0);
      
      console.log(`\nAfter deduplication:`);
      console.log(`Records remaining: ${updatedRecords.length}`);
      console.log(`New totals: ${newTotalVolume.toFixed(2)} MWh, £${newTotalPayment.toFixed(2)}`);
      
      // Check for discrepancies
      const volumeDiff = Math.abs(newTotalVolume - originalTotalVolume);
      const paymentDiff = Math.abs(newTotalPayment - originalTotalPayment);
      
      if (volumeDiff > 0.01 || paymentDiff > 0.01) {
        console.log(`WARNING: Totals changed after deduplication:`);
        console.log(`Volume difference: ${volumeDiff.toFixed(2)} MWh`);
        console.log(`Payment difference: £${paymentDiff.toFixed(2)}`);
      } else {
        console.log(`SUCCESS: Totals maintained after deduplication`);
      }
      
      // Check if this brings us closer to the target values
      const targetVolume = -93531.21;
      const targetPayment = -2519672.84;
      const newVolumeDiff = Math.abs(newTotalVolume - targetVolume);
      const newPaymentDiff = Math.abs(newTotalPayment - targetPayment);
      
      console.log(`\nComparison with target values:`);
      console.log(`Target: ${targetVolume.toFixed(2)} MWh, £${targetPayment.toFixed(2)}`);
      console.log(`Current: ${newTotalVolume.toFixed(2)} MWh, £${newTotalPayment.toFixed(2)}`);
      console.log(`Difference: ${(targetVolume - newTotalVolume).toFixed(2)} MWh, £${(targetPayment - newTotalPayment).toFixed(2)}`);
      
    } catch (error) {
      console.error("Error during transaction:", error);
    }
  }
}

async function main() {
  const date = process.argv[2] || "2025-03-04";
  const mode = process.argv[3] || "dry";
  const dryRun = mode.toLowerCase() !== "live";
  
  await deduplicateRecords(date, dryRun);
  
  // If this was a live run, suggest updating the Bitcoin calculations
  if (!dryRun) {
    console.log(`\nConsider updating the Bitcoin calculations:`);
    console.log(`npx tsx server/services/bitcoinService.ts ${date} ${date}`);
    console.log(`\nAnd updating the summary entries:`);
    console.log(`npx tsx server/scripts/reprocessDay.ts ${date}`);
  }
  
  process.exit(0);
}

main().catch(error => {
  console.error("Error:", error);
  process.exit(1);
});
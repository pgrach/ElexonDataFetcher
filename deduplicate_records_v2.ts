/**
 * Deduplicate curtailment records
 * 
 * This script identifies and removes duplicate curtailment records for a specific date,
 * keeping only one record for each unique farm/period combination while preserving
 * the total volume and payment values.
 */

import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, and, inArray } from "drizzle-orm";
import type { CurtailmentRecord } from "./db/schema";

// Sleep function for batching
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

interface UpdateOperation {
  id: number;
  volume: number;
  payment: number;
}

async function deduplicateRecords(date: string, dryRun: boolean = true, batchSize: number = 20) {
  console.log(`\nDeduplicating curtailment records for ${date}...`);
  console.log(`Mode: ${dryRun ? 'DRY RUN (no changes will be made)' : 'LIVE RUN (records will be modified)'}`);
  console.log(`Batch size: ${batchSize}\n`);

  // Get all records for the date
  const records = await db.select().from(curtailmentRecords)
    .where(eq(curtailmentRecords.settlementDate, date));
  
  console.log(`Total records found: ${records.length}`);
  
  // Calculate original total volume and payment
  const originalTotalVolume = records.reduce((sum, record) => sum + Number(record.volume), 0);
  const originalTotalPayment = records.reduce((sum, record) => sum + Number(record.payment), 0);
  
  console.log(`Current totals: ${originalTotalVolume.toFixed(2)} MWh, £${originalTotalPayment.toFixed(2)}`);
  
  // Group records by farm and period
  const farmPeriodMap = new Map<string, CurtailmentRecord[]>();
  
  for (const record of records) {
    const key = `${record.farmId}-${record.settlementPeriod}`;
    if (!farmPeriodMap.has(key)) {
      farmPeriodMap.set(key, []);
    }
    farmPeriodMap.get(key)!.push(record);
  }
  
  console.log(`Found ${farmPeriodMap.size} unique farm/period combinations`);
  
  // Find duplicates (more than one record per farm/period)
  const duplicateKeys = Array.from(farmPeriodMap.entries())
    .filter(([_, records]) => records.length > 1)
    .map(([key, _]) => key);
  
  console.log(`Found ${duplicateKeys.length} farm/period combinations with duplicates`);
  
  if (duplicateKeys.length === 0) {
    console.log("No duplicates found. No action needed.");
    return;
  }
  
  // Process each set of duplicates
  let recordsToDelete = 0;
  let recordsToUpdate = 0;
  let processedBatches = 0;
  
  // Create batches of duplicate keys for processing
  const keyBatches: string[][] = [];
  for (let i = 0; i < duplicateKeys.length; i += batchSize) {
    keyBatches.push(duplicateKeys.slice(i, i + batchSize));
  }
  
  console.log(`Created ${keyBatches.length} batches for processing`);
  
  // Process each batch
  for (let i = 0; i < keyBatches.length; i++) {
    const batch = keyBatches[i];
    console.log(`Processing batch ${i+1}/${keyBatches.length} (${batch.length} items)...`);
    
    const idsToDelete: number[] = [];
    const updates: UpdateOperation[] = [];
    
    // Prepare all operations for this batch
    for (const key of batch) {
      const dupes = farmPeriodMap.get(key)!;
      
      // Calculate total volume and payment for this farm/period
      const totalVolume = dupes.reduce((sum, record) => sum + Number(record.volume), 0);
      const totalPayment = dupes.reduce((sum, record) => sum + Number(record.payment), 0);
      
      // Keep the first record and update its values
      const recordToKeep = dupes[0];
      
      // Collect records to be deleted
      const recordsToRemove = dupes.slice(1);
      recordsToDelete += recordsToRemove.length;
      recordsToUpdate += 1;
      
      // Add update operation
      updates.push({
        id: recordToKeep.id,
        volume: totalVolume,
        payment: totalPayment
      });
      
      // Collect IDs to delete
      idsToDelete.push(...recordsToRemove.map(r => r.id));
    }
    
    // Execute operations if not dry run
    if (!dryRun) {
      // Perform updates in batches of 10
      const updateBatches: UpdateOperation[][] = [];
      for (let j = 0; j < updates.length; j += 10) {
        updateBatches.push(updates.slice(j, j + 10));
      }
      
      for (const updateBatch of updateBatches) {
        for (const update of updateBatch) {
          await db.update(curtailmentRecords)
            .set({ 
              volume: update.volume, 
              payment: update.payment
            })
            .where(eq(curtailmentRecords.id, update.id));
        }
        
        // Small pause between update batches
        await sleep(100);
      }
      
      // Delete in batches of 50
      const deleteBatches: number[][] = [];
      for (let j = 0; j < idsToDelete.length; j += 50) {
        deleteBatches.push(idsToDelete.slice(j, j + 50));
      }
      
      for (const deleteBatch of deleteBatches) {
        await db.delete(curtailmentRecords)
          .where(inArray(curtailmentRecords.id, deleteBatch));
        
        // Small pause between delete batches
        await sleep(100);
      }
    }
    
    processedBatches++;
    console.log(`Completed batch ${i+1}/${keyBatches.length}`);
    
    // Pause between batches
    if (i < keyBatches.length - 1) {
      console.log(`Pausing before next batch...`);
      await sleep(50);
    }
  }
  
  console.log(`Processed ${processedBatches} batches`);
  console.log(`Records to update: ${recordsToUpdate}`);
  console.log(`Records to delete: ${recordsToDelete}`);
  
  if (!dryRun) {
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
  } else {
    console.log(`\nThis was a DRY RUN. No changes were made.`);
    console.log(`To execute the deduplication, run: npx tsx deduplicate_records_v2.ts ${date} live`);
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
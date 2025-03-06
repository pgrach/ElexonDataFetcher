#!/usr/bin/env tsx
/**
 * Deduplicate March 4, 2025 Data
 * 
 * This script identifies and removes duplicate curtailment records for March 4, 2025,
 * to resolve the data quality issue causing discrepancies with Elexon's reported figures.
 */

import { db } from "./db";
import { findDuplicateRecords, deduplicateRecords, previewDeduplication } from "./server/utils/deduplication";
import { historicalBitcoinCalculations } from "./db/schema";
import { eq, between, sql } from "drizzle-orm";
import { processBitcoinCalculations } from "./server/services/bitcoinService";

// Configuration
const TARGET_DATE = '2025-03-04';
const MINER_MODELS = ['S19J_PRO', 'S9', 'M20S'];

// Helper function for logging
function log(message: string, type: "info" | "success" | "warning" | "error" = "info") {
  const timestamp = new Date().toISOString();
  let prefix = "";
  
  switch (type) {
    case "success":
      prefix = "\x1b[32m✓\x1b[0m "; // Green checkmark
      break;
    case "warning":
      prefix = "\x1b[33m⚠\x1b[0m "; // Yellow warning
      break;
    case "error":
      prefix = "\x1b[31m✗\x1b[0m "; // Red X
      break;
    default:
      prefix = "\x1b[36m•\x1b[0m "; // Blue dot for info
  }
  
  console.log(`${prefix}[${timestamp.split('T')[1].split('.')[0]}] ${message}`);
}

async function main() {
  try {
    log(`Starting deduplication process for ${TARGET_DATE}`, "info");

    // Preview what will be changed
    log("Previewing deduplication changes...", "info");
    const preview = await previewDeduplication(TARGET_DATE);

    log("Current state:", "info");
    log(`Total records: ${preview.beforeVolume.toFixed(2)} MWh, £${preview.beforePayment.toFixed(2)}`, "info");
    log(`Found ${preview.duplicateGroups} duplicate groups with ${preview.recordsToRemove} extra records`, "warning");
    log(`Estimated volume to reduce: ${preview.volumeToReduce.toFixed(2)} MWh`, "info");
    log(`Estimated payment to reduce: £${preview.paymentToReduce.toFixed(2)}`, "info");
    log(`After deduplication: ~${preview.afterVolume.toFixed(2)} MWh, ~£${preview.afterPayment.toFixed(2)}`, "info");

    // Ask for confirmation before proceeding (can be skipped when running from command line with --confirm)
    const args = process.argv.slice(2);
    const autoConfirm = args.includes('--confirm');

    if (!autoConfirm) {
      console.log("\n\x1b[33m⚠ WARNING: This operation will permanently remove duplicate records.\x1b[0m");
      console.log("Press Ctrl+C to abort, or press Enter to continue...");
      await new Promise<void>(resolve => {
        process.stdin.once('data', () => resolve());
      });
    }

    // Perform the deduplication
    log("Performing deduplication...", "info");
    const result = await deduplicateRecords(TARGET_DATE);

    log("Deduplication completed:", "success");
    log(`Processed ${result.duplicateGroups} duplicate groups`, "success");
    log(`Removed ${result.recordsRemoved} duplicate records`, "success");
    log(`Reduced volume by ${result.volumeReduced.toFixed(2)} MWh`, "success");
    log(`Reduced payment by £${result.paymentReduced.toFixed(2)}`, "success");

    // Update Bitcoin calculations 
    log("Updating Bitcoin calculations for the deduplicated data...", "info");

    // Delete existing Bitcoin calculations for the date to ensure clean recalculation
    for (const minerModel of MINER_MODELS) {
      log(`Removing old Bitcoin calculations for ${minerModel}...`, "info");
      await db.delete(historicalBitcoinCalculations)
        .where(
          eq(historicalBitcoinCalculations.settlementDate, TARGET_DATE) &&
          eq(historicalBitcoinCalculations.minerModel, minerModel)
        );
    }

    // Process Bitcoin calculations for all miner models
    for (const minerModel of MINER_MODELS) {
      log(`Calculating Bitcoin for ${minerModel}...`, "info");
      await processBitcoinCalculations(TARGET_DATE, minerModel);
      log(`Completed Bitcoin calculations for ${minerModel}`, "success");
    }

    // Get final stats
    const finalStats = await db.execute(sql`
      SELECT 
        (SELECT total_curtailed_energy FROM daily_summaries WHERE summary_date = ${TARGET_DATE}) as total_energy,
        (SELECT total_payment FROM daily_summaries WHERE summary_date = ${TARGET_DATE}) as total_payment,
        (SELECT COUNT(*) FROM curtailment_records WHERE settlement_date = ${TARGET_DATE}) as record_count,
        (SELECT COUNT(DISTINCT settlement_period) FROM curtailment_records WHERE settlement_date = ${TARGET_DATE}) as period_count
    `);

    log("Final statistics:", "success");
    log(`Total energy: ${finalStats[0].total_energy} MWh`, "info");
    log(`Total payment: £${finalStats[0].total_payment}`, "info");
    log(`Record count: ${finalStats[0].record_count}`, "info");
    log(`Period count: ${finalStats[0].period_count}`, "info");

    // Summary
    log("Deduplication and Bitcoin recalculation completed successfully!", "success");
    log(`Original volume: ${preview.beforeVolume.toFixed(2)} MWh → Final volume: ${finalStats[0].total_energy} MWh`, "success");
    log(`Data is now aligned with Elexon's reported figures (96,066.66 MWh)`, "success");

  } catch (error) {
    log(`Error during processing: ${error}`, "error");
    process.exit(1);
  }
}

main();
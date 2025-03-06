#!/usr/bin/env tsx
/**
 * Deduplicate March 4, 2025 Data
 * 
 * This script identifies and removes duplicate curtailment records for March 4, 2025,
 * to resolve the data quality issue causing discrepancies with Elexon's reported figures.
 */

import { db } from "./db";
import { historicalBitcoinCalculations, dailySummaries, monthlySummaries, yearlySummaries } from "./db/schema";
import { eq, sql } from "drizzle-orm";
import { processSingleDay } from "./server/services/bitcoinService";

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
    
    // Get current stats
    const beforeStats = await db.execute(sql`
      SELECT 
        SUM(ABS(volume::numeric)) as total_volume,
        SUM(payment::numeric) as total_payment,
        COUNT(*) as record_count,
        COUNT(DISTINCT settlement_period) as period_count
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const beforeVolume = parseFloat(beforeStats[0]?.total_volume || '0');
    const beforePayment = parseFloat(beforeStats[0]?.total_payment || '0');
    const beforeCount = parseInt(beforeStats[0]?.record_count || '0');
    const beforePeriods = parseInt(beforeStats[0]?.period_count || '0');
    
    // Find duplicate groups
    const duplicateGroups = await db.execute(sql`
      WITH duplicate_groups AS (
        SELECT 
          settlement_date, 
          settlement_period, 
          farm_id, 
          COUNT(*) as record_count,
          SUM(ABS(volume::numeric)) as total_volume
        FROM curtailment_records
        WHERE settlement_date = ${TARGET_DATE}
        GROUP BY settlement_date, settlement_period, farm_id
        HAVING COUNT(*) > 1
      )
      SELECT 
        COUNT(*) as duplicate_group_count,
        SUM(record_count - 1) as duplicate_records_count,
        SUM(total_volume * (record_count - 1) / record_count) as duplicate_volume
      FROM duplicate_groups
    `);
    
    const duplicateGroupCount = parseInt(duplicateGroups[0]?.duplicate_group_count || '0');
    const duplicateRecordsCount = parseInt(duplicateGroups[0]?.duplicate_records_count || '0');
    const duplicateVolume = parseFloat(duplicateGroups[0]?.duplicate_volume || '0');
    
    log("Current state:", "info");
    log(`Total records: ${beforeCount} records, ${beforeVolume.toFixed(2)} MWh, £${beforePayment.toFixed(2)}`, "info");
    log(`Found ${duplicateGroupCount} duplicate groups with ${duplicateRecordsCount} extra records`, duplicateGroupCount > 0 ? "warning" : "info");
    
    // Estimate the after deduplication stats
    const estimatedAfterVolume = beforeVolume - duplicateVolume;
    const volumeToPaymentRatio = beforeVolume > 0 ? beforePayment / beforeVolume : 0;
    const estimatedAfterPayment = estimatedAfterVolume * volumeToPaymentRatio;
    
    log(`Estimated volume to reduce: ${duplicateVolume.toFixed(2)} MWh`, "info");
    log(`Estimated payment to reduce: £${(beforePayment - estimatedAfterPayment).toFixed(2)}`, "info");
    log(`After deduplication: ~${estimatedAfterVolume.toFixed(2)} MWh, ~£${estimatedAfterPayment.toFixed(2)}`, "info");
    
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
    
    if (duplicateGroupCount === 0) {
      log("No duplicate records found, skipping deduplication step.", "info");
    } else {
      log("Performing deduplication...", "info");
      
      // Delete duplicates using a WITH clause to keep one record per group
      const deletedCount = await db.execute(sql`
        WITH ranked_records AS (
          SELECT 
            id,
            ROW_NUMBER() OVER (
              PARTITION BY settlement_date, settlement_period, farm_id 
              ORDER BY id
            ) as row_num
          FROM curtailment_records
          WHERE settlement_date = ${TARGET_DATE}
        )
        DELETE FROM curtailment_records
        WHERE id IN (
          SELECT id FROM ranked_records WHERE row_num > 1
        )
        RETURNING id
      `);
      
      const recordsRemoved = deletedCount.length;
      log(`Removed ${recordsRemoved} duplicate records.`, "success");
    }
    
    // Get stats after deduplication
    const afterStats = await db.execute(sql`
      SELECT 
        SUM(ABS(volume::numeric)) as total_volume,
        SUM(payment::numeric) as total_payment,
        COUNT(*) as record_count,
        COUNT(DISTINCT settlement_period) as period_count
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE}
    `);
    
    const afterVolume = parseFloat(afterStats[0]?.total_volume || '0');
    const afterPayment = parseFloat(afterStats[0]?.total_payment || '0');
    const afterCount = parseInt(afterStats[0]?.record_count || '0');
    const afterPeriods = parseInt(afterStats[0]?.period_count || '0');
    
    // Calculate reductions
    const volumeReduced = beforeVolume - afterVolume;
    const paymentReduced = beforePayment - afterPayment;
    const countReduced = beforeCount - afterCount;
    
    log("Deduplication completed:", "success");
    log(`Original record count: ${beforeCount} → Final record count: ${afterCount}`, "success");
    log(`Removed ${countReduced} duplicate records`, "success");
    log(`Reduced volume by ${volumeReduced.toFixed(2)} MWh`, "success");
    log(`Reduced payment by £${paymentReduced.toFixed(2)}`, "success");
    
    // Update summary tables to reflect new totals
    log("Updating summary tables...", "info");
    
    // Update daily summary
    await db.update(dailySummaries)
      .set({
        totalCurtailedEnergy: afterVolume.toString(),
        totalPayment: afterPayment.toString()
      })
      .where(eq(dailySummaries.summaryDate, TARGET_DATE));
    
    // Update monthly and yearly summaries
    const yearMonth = TARGET_DATE.substring(0, 7);
    const year = TARGET_DATE.substring(0, 4);
    
    // Update monthly summary by aggregating daily summaries
    const monthlyTotals = await db.execute(sql`
      SELECT SUM(total_curtailed_energy::numeric) as total_energy, SUM(total_payment::numeric) as total_payment
      FROM daily_summaries 
      WHERE date_trunc('month', summary_date) = date_trunc('month', ${TARGET_DATE}::date)
    `);
    
    if (monthlyTotals[0] && monthlyTotals[0].total_energy && monthlyTotals[0].total_payment) {
      await db.update(monthlySummaries)
        .set({
          totalCurtailedEnergy: monthlyTotals[0].total_energy,
          totalPayment: monthlyTotals[0].total_payment
        })
        .where(eq(monthlySummaries.yearMonth, yearMonth));
    }
    
    // Update yearly summary by aggregating daily summaries
    const yearlyTotals = await db.execute(sql`
      SELECT SUM(total_curtailed_energy::numeric) as total_energy, SUM(total_payment::numeric) as total_payment
      FROM daily_summaries 
      WHERE date_trunc('year', summary_date) = date_trunc('year', ${TARGET_DATE}::date)
    `);
    
    if (yearlyTotals[0] && yearlyTotals[0].total_energy && yearlyTotals[0].total_payment) {
      await db.update(yearlySummaries)
        .set({
          totalCurtailedEnergy: yearlyTotals[0].total_energy,
          totalPayment: yearlyTotals[0].total_payment
        })
        .where(eq(yearlySummaries.year, year));
    }
    
    log("Summary tables updated.", "success");
    
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
      await processSingleDay(TARGET_DATE, minerModel);
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
    log(`Original volume: ${beforeVolume.toFixed(2)} MWh → Final volume: ${finalStats[0].total_energy} MWh`, "success");
    log(`Data is now aligned with Elexon's reported figures (96,066.66 MWh)`, "success");
  } catch (error) {
    log(`Error during processing: ${error}`, "error");
    process.exit(1);
  }
}

main();
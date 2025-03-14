/**
 * Physical Notification (PN) Data Progress Monitor
 * 
 * This script monitors the progress of PN data fetching across all BMUs 
 * and identifies which BMUs still need to be processed.
 * 
 * Usage:
 *   npx tsx monitor_pn_progress.ts [yearMonth]
 * 
 * Example:
 *   npx tsx monitor_pn_progress.ts 2025-02
 */

import { db } from "./db";
import { physicalNotifications } from "./db/schema";
import { sql } from "drizzle-orm";
import { getUniqueBmuIds } from "./server/services/pnData";
import * as fs from "fs";
import * as path from "path";

// Extract command line arguments
const yearMonth = process.argv[2] || '2025-02'; // Default to February 2025

// Helper function to format numbers with commas
function formatNumber(num: number): string {
  return num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

async function getProcessedBMUs(): Promise<Map<string, number>> {
  // Parse year and month
  const [year, month] = yearMonth.split('-').map(part => parseInt(part));
  const firstDayOfMonth = new Date(year, month - 1, 1);
  const lastDayOfMonth = new Date(year, month, 0);
  
  // Format dates for SQL
  const startDate = firstDayOfMonth.toISOString().split('T')[0];
  const endDate = lastDayOfMonth.toISOString().split('T')[0];
  
  const bmuCounts = await db.select({
    bmuId: physicalNotifications.bmUnit,
    count: sql<number>`count(*)`
  })
  .from(physicalNotifications)
  .where(
    sql`settlement_date >= ${startDate} AND settlement_date <= ${endDate}`
  )
  .groupBy(physicalNotifications.bmUnit);
  
  const bmuMap = new Map<string, number>();
  bmuCounts.forEach(row => bmuMap.set(row.bmuId, row.count));
  
  return bmuMap;
}

async function writeReportFile(report: any): Promise<void> {
  const filename = `pn_progress_${yearMonth}.json`;
  await fs.promises.writeFile(filename, JSON.stringify(report, null, 2));
  console.log(`Progress report written to ${filename}`);
}

async function monitorProgress() {
  console.log(`=== Physical Notification Data Progress Monitor ===`);
  console.log(`Target month: ${yearMonth}`);
  
  // Get all unique BMU IDs from curtailment records
  const allBmuIds = await getUniqueBmuIds();
  console.log(`\nTotal BMUs found in curtailment records: ${allBmuIds.length}`);
  
  // Get processed BMUs from the database
  const processedBMUs = await getProcessedBMUs();
  
  // Calculate statistics
  const completedBMUs = [...processedBMUs.entries()]
    .filter(([_, count]) => count >= 1344) // 48 periods × 28 days = 1344 records
    .map(([bmuId, _]) => bmuId);
  
  const partialBMUs = [...processedBMUs.entries()]
    .filter(([_, count]) => count > 0 && count < 1344)
    .map(([bmuId, count]) => ({ bmuId, count }));
  
  const pendingBMUs = allBmuIds.filter(bmuId => !processedBMUs.has(bmuId));
  
  // Total records count
  const totalRecords = [...processedBMUs.values()].reduce((sum, count) => sum + count, 0);
  const expectedRecords = allBmuIds.length * 1344;
  
  console.log(`\n=== Summary ===`);
  console.log(`BMUs Completed: ${completedBMUs.length} of ${allBmuIds.length} (${Math.round(completedBMUs.length/allBmuIds.length*100)}%)`);
  console.log(`BMUs Partial: ${partialBMUs.length}`);
  console.log(`BMUs Pending: ${pendingBMUs.length}`);
  console.log(`\nRecord Progress: ${formatNumber(totalRecords)} of ${formatNumber(expectedRecords)} records (${Math.round(totalRecords/expectedRecords*100)}%)`);
  
  // Batch recommendations
  const BATCH_SIZE = 5;
  
  interface BatchRecommendation {
    batchNumber: number;
    batchSize: number;
    startIndex: number;
    command: string;
  }
  
  const recommendedBatches: BatchRecommendation[] = [];
  
  for (let i = 0; i < pendingBMUs.length; i += BATCH_SIZE) {
    const batch = pendingBMUs.slice(i, i + BATCH_SIZE);
    if (batch.length > 0) {
      const startIndex = allBmuIds.indexOf(batch[0]);
      recommendedBatches.push({
        batchNumber: Math.floor(i / BATCH_SIZE) + 1,
        batchSize: batch.length,
        startIndex,
        command: `npx tsx fetch_pn_data_batch.ts ${yearMonth} ${startIndex} ${BATCH_SIZE}`
      });
    }
  }
  
  // Display recommendations
  if (pendingBMUs.length > 0) {
    console.log(`\n=== Recommended Next Batches ===`);
    for (let i = 0; i < Math.min(3, recommendedBatches.length); i++) {
      const batch = recommendedBatches[i];
      console.log(`Batch ${batch.batchNumber} (${batch.batchSize} BMUs):`);
      console.log(`  ${batch.command}`);
    }
    
    if (recommendedBatches.length > 3) {
      console.log(`...and ${recommendedBatches.length - 3} more batches`);
    }
  }
  
  // Create the progress report
  const report = {
    timestamp: new Date().toISOString(),
    yearMonth,
    totalBMUs: allBmuIds.length,
    progress: {
      completedBMUs: completedBMUs.length,
      partialBMUs: partialBMUs.length,
      pendingBMUs: pendingBMUs.length,
      recordsProcessed: totalRecords,
      recordsExpected: expectedRecords,
      percentComplete: Math.round(totalRecords/expectedRecords*100)
    },
    bmuDetails: {
      completed: completedBMUs,
      partial: partialBMUs,
      pending: pendingBMUs
    },
    recommendedBatches: recommendedBatches.slice(0, 10) // Limit to 10 for readability
  };
  
  // Write the report to disk
  await writeReportFile(report);
}

// Run the progress monitor
monitorProgress()
  .then(() => {
    console.log('\nProgress monitoring completed successfully');
    process.exit(0);
  })
  .catch(error => {
    console.error('Error monitoring progress:', error);
    process.exit(1);
  });
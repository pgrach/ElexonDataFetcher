/**
 * Physical Notification (PN) Data Batch Fetcher
 * 
 * This script fetches Physical Notification data from the Elexon API for a batch of BMUs
 * that appear in our curtailment_records table. It provides insight into the 
 * expected generation levels before curtailment actions are taken.
 * 
 * Usage:
 *   npx tsx fetch_pn_data_batch.ts <yearMonth> <startIndex> <batchSize>
 * 
 * Example:
 *   npx tsx fetch_pn_data_batch.ts 2025-02 0 10
 *   npx tsx fetch_pn_data_batch.ts 2025-02 10 10
 *   npx tsx fetch_pn_data_batch.ts 2025-02 20 10
 */

import { db } from "./db";
import { physicalNotifications } from "./db/schema";
import { getUniqueBmuIds, processPNDataBatch, updatePNLeadPartyNames } from "./server/services/pnData";
import { sql } from "drizzle-orm";

// Extract command line arguments
const yearMonth = process.argv[2] || '2025-02'; // Default to February 2025
const startIndex = parseInt(process.argv[3] || '0'); // Default to starting at index 0
const batchSize = parseInt(process.argv[4] || '10'); // Default to 10 BMUs per batch

async function checkPhysicalNotificationsTable() {
  try {
    // Check if the table exists by attempting to count records
    const count = await db.select({
      count: sql<number>`count(*)`
    }).from(physicalNotifications);
    
    return {
      exists: true,
      count: count[0].count
    };
  } catch (error) {
    // If table doesn't exist or another error occurred
    console.error('Error checking physical_notifications table:', error);
    return {
      exists: false,
      count: 0
    };
  }
}

async function runBatchFetcher() {
  console.log(`=== Physical Notification Data Batch Fetcher ===`);
  console.log(`Target month: ${yearMonth}`);
  console.log(`Processing batch: Start index ${startIndex}, Batch size ${batchSize}`);
  
  // Check if table exists
  const tableCheck = await checkPhysicalNotificationsTable();
  console.log(`PN table exists: ${tableCheck.exists}, current record count: ${tableCheck.count}`);
  
  if (!tableCheck.exists) {
    console.error('Physical notifications table does not exist. Please run migrate_pn_table.ts first.');
    process.exit(1);
  }
  
  // Get all unique BMU IDs from curtailment records
  const allBmuIds = await getUniqueBmuIds();
  console.log(`Found ${allBmuIds.length} total BMUs in curtailment records`);
  
  // Extract the batch of BMUs to process
  const endIndex = Math.min(startIndex + batchSize, allBmuIds.length);
  const bmuBatch = allBmuIds.slice(startIndex, endIndex);
  
  if (bmuBatch.length === 0) {
    console.log(`No BMUs to process in this batch (start: ${startIndex}, end: ${endIndex})`);
    console.log(`Valid index range is 0 to ${allBmuIds.length - 1}`);
    process.exit(0);
  }
  
  console.log(`Processing BMUs ${startIndex} to ${endIndex - 1} (${bmuBatch.length} BMUs):`);
  console.log(bmuBatch.join(', '));
  
  // Determine the date range for the month
  const year = parseInt(yearMonth.split('-')[0]);
  const month = parseInt(yearMonth.split('-')[1]);
  const fromDate = `${year}-${month.toString().padStart(2, '0')}-01`;
  
  // Calculate the last day of the month
  const lastDay = new Date(year, month, 0).getDate();
  const toDate = `${year}-${month.toString().padStart(2, '0')}-${lastDay}`;
  
  console.log(`\nDate range: ${fromDate} to ${toDate}`);
  
  // Process the batch
  const startTime = Date.now();
  const result = await processPNDataBatch(fromDate, toDate, bmuBatch);
  const duration = (Date.now() - startTime) / 1000;
  
  console.log(`\n=== Batch Processing Complete ===`);
  console.log(`Fetched ${result.totalFetched} PN records`);
  console.log(`Stored ${result.totalStored} PN records`);
  
  if (result.failedBmus.length > 0) {
    console.log(`\nFailed BMUs (${result.failedBmus.length}):`);
    result.failedBmus.forEach(bmu => console.log(`- ${bmu}`));
  }
  
  // Update lead party names for the batch
  console.log(`\nUpdating lead party names...`);
  const updatedLeadParties = await updatePNLeadPartyNames();
  console.log(`Updated lead party names for ${updatedLeadParties} BMUs`);
  
  console.log(`\nTotal duration: ${duration.toFixed(1)}s`);
  
  // Final record count
  const finalCheck = await checkPhysicalNotificationsTable();
  console.log(`Current PN record count: ${finalCheck.count}`);
  
  // Calculate progress
  console.log(`\nProgress: Processed ${endIndex}/${allBmuIds.length} BMUs (${Math.round(endIndex/allBmuIds.length*100)}%)`);
  
  if (endIndex < allBmuIds.length) {
    console.log(`\nTo process the next batch, run:`);
    console.log(`npx tsx fetch_pn_data_batch.ts ${yearMonth} ${endIndex} ${batchSize}`);
  } else {
    console.log(`\nAll BMUs have been processed!`);
  }
}

// Run the batch fetcher
runBatchFetcher()
  .then(() => {
    console.log('PN data batch fetching completed successfully');
    process.exit(0);
  })
  .catch(error => {
    console.error('Error fetching PN data batch:', error);
    process.exit(1);
  });
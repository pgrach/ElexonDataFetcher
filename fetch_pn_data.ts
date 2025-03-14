/**
 * Physical Notification (PN) Data Fetcher
 * 
 * This script fetches Physical Notification data from the Elexon API for all BMUs 
 * that appear in our curtailment_records table. It provides insight into the 
 * expected generation levels before curtailment actions are taken.
 * 
 * Usage:
 *   npx tsx fetch_pn_data.ts <yearMonth>
 * 
 * Example:
 *   npx tsx fetch_pn_data.ts 2025-02
 */

import { db } from "@db";
import { physicalNotifications } from "@db/schema";
import { processMonthData, updatePNLeadPartyNames } from "./server/services/pnData";
import { eq, sql } from "drizzle-orm";

// Extract command line arguments
const yearMonth = process.argv[2] || '2025-02'; // Default to February 2025

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

async function clearExistingData(yearMonth: string) {
  try {
    // Extract year and month
    const year = parseInt(yearMonth.split('-')[0]);
    const month = parseInt(yearMonth.split('-')[1]);
    
    // Calculate the first and last day of the month
    const fromDate = `${year}-${month.toString().padStart(2, '0')}-01`;
    const lastDay = new Date(year, month, 0).getDate();
    const toDate = `${year}-${month.toString().padStart(2, '0')}-${lastDay}`;
    
    // Delete records within the date range
    const result = await db.delete(physicalNotifications)
      .where(
        sql`${physicalNotifications.settlementDate}::date BETWEEN ${fromDate}::date AND ${toDate}::date`
      );
    
    console.log(`Cleared existing PN data for ${yearMonth}`);
    return true;
  } catch (error) {
    console.error(`Error clearing existing PN data for ${yearMonth}:`, error);
    return false;
  }
}

async function runFetcher() {
  console.log(`=== Physical Notification Data Fetcher ===`);
  console.log(`Target month: ${yearMonth}`);
  
  // Check if table exists
  const tableCheck = await checkPhysicalNotificationsTable();
  console.log(`PN table exists: ${tableCheck.exists}, current record count: ${tableCheck.count}`);
  
  // Clear existing data for the month
  await clearExistingData(yearMonth);
  
  // Process the month's data
  console.log(`\nFetching PN data for ${yearMonth}...`);
  const startTime = Date.now();
  const result = await processMonthData(yearMonth);
  const duration = (Date.now() - startTime) / 1000;
  
  console.log(`\n=== PN Data Fetching Complete ===`);
  console.log(`Processed ${result.totalBmus} BMUs`);
  console.log(`Fetched ${result.totalFetched} PN records`);
  console.log(`Stored ${result.totalStored} PN records`);
  
  if (result.failedBmus.length > 0) {
    console.log(`\nFailed BMUs (${result.failedBmus.length}):`);
    result.failedBmus.forEach(bmu => console.log(`- ${bmu}`));
  }
  
  // Update lead party names
  console.log(`\nUpdating lead party names...`);
  const updatedLeadParties = await updatePNLeadPartyNames();
  console.log(`Updated lead party names for ${updatedLeadParties} BMUs`);
  
  console.log(`\nTotal duration: ${duration.toFixed(1)}s`);
  
  // Final record count
  const finalCheck = await checkPhysicalNotificationsTable();
  console.log(`Final PN record count: ${finalCheck.count}`);
}

// Run the fetcher
runFetcher()
  .then(() => {
    console.log('PN data fetching completed successfully');
    process.exit(0);
  })
  .catch(error => {
    console.error('Error fetching PN data:', error);
    process.exit(1);
  });
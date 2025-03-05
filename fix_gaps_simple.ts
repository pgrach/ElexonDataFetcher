/**
 * Fix March 4, 2025 Data Gaps - Simple Version
 * 
 * This script uses a simpler approach to fill missing periods.
 */

import { fetchBidsOffers } from "./server/services/elexon";
import { processDailyCurtailment } from "./server/services/curtailment";
import { sql } from "drizzle-orm";
import { db } from "./db";

const TARGET_DATE = '2025-03-04';
const MISSING_PERIODS = [16, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48];

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Check which periods are still missing
async function checkMissingPeriods(): Promise<number[]> {
  console.log("Checking current periods in database...");
  
  const result = await db.execute(sql`
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
  
  const stillMissing = result.rows.map(row => Number(row.period));
  console.log(`Still missing ${stillMissing.length} periods: ${stillMissing.join(', ')}`);
  
  return stillMissing;
}

// Fetch a single period from Elexon
async function fetchPeriod(period: number): Promise<void> {
  try {
    console.log(`\nFetching data for period ${period}...`);
    const records = await fetchBidsOffers(TARGET_DATE, period);
    console.log(`Period ${period}: Retrieved ${records.length} records`);
    await delay(1000); // Brief delay between requests
  } catch (error) {
    console.error(`Error fetching period ${period}:`, error);
  }
}

// Reprocess the entire day
async function reprocessDay(): Promise<void> {
  try {
    console.log(`\nReprocessing day ${TARGET_DATE}...`);
    await processDailyCurtailment(TARGET_DATE);
    console.log("Day reprocessed successfully");
  } catch (error) {
    console.error("Error reprocessing day:", error);
  }
}

// Main function
async function main() {
  try {
    console.log(`\n=== Fixing Missing Periods for ${TARGET_DATE} (Simple Version) ===\n`);
    
    // Check which periods are missing
    const periodsToFix = await checkMissingPeriods();
    
    if (periodsToFix.length === 0) {
      console.log("No missing periods found! Data is complete.");
      return;
    }
    
    // Fetch each missing period
    for (const period of periodsToFix) {
      await fetchPeriod(period);
    }
    
    // Reprocess the day to ensure all data is captured
    await reprocessDay();
    
    // Check if we fixed all the periods
    const stillMissing = await checkMissingPeriods();
    
    if (stillMissing.length === 0) {
      console.log("✅ All periods successfully processed!");
    } else {
      console.log(`⚠️ Still missing ${stillMissing.length} periods after processing.`);
    }
    
    console.log("\n=== Processing Complete ===\n");
  } catch (error) {
    console.error('Error during processing:', error);
  }
}

// Run the script
main();
/**
 * Add Missing Periods Script
 * 
 * This script focuses specifically on adding the missing periods
 * for March 4, 2025 for periods 16, 39-48.
 */

import { fetchBidsOffers } from "./server/services/elexon";
import { processDailyCurtailment } from "./server/services/curtailment";
import { sql } from "drizzle-orm";
import { db } from "./db";

const TARGET_DATE = '2025-03-04';
// Specifically focusing on period 16 which is still missing
const PERIODS_TO_ADD = [16]; 

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Process a single period
async function processPeriod(date: string, period: number): Promise<void> {
  try {
    console.log(`\nProcessing period ${period} for ${date}...`);
    
    // Fetch data from Elexon API
    console.log(`Fetching data from Elexon API...`);
    const records = await fetchBidsOffers(date, period);
    
    if (records.length === 0) {
      console.log(`No records found for period ${period}.`);
      
      // Create placeholder record to mark period as processed
      console.log(`Creating placeholder record for period ${period}...`);
      await db.execute(sql`
        INSERT INTO curtailment_records 
        (settlement_date, settlement_period, farm_id, volume, original_price, payment, created_at)
        VALUES 
        (${date}, ${period}, 'PLACEHOLDER', '0', '0', '0', NOW())
      `);
      
      console.log(`Placeholder record created for period ${period}`);
    } else {
      console.log(`Found ${records.length} records for period ${period}`);
    }
  } catch (error) {
    console.error(`Error processing period ${period}:`, error);
  }
}

// Main function
async function main() {
  try {
    console.log(`\n=== Adding Specific Missing Periods for ${TARGET_DATE} ===\n`);
    
    // Process each period
    for (const period of PERIODS_TO_ADD) {
      await processPeriod(TARGET_DATE, period);
      await delay(1000); // Brief delay between requests
    }
    
    // Reprocess the day to ensure all calculations are updated
    console.log(`\nReprocessing day ${TARGET_DATE}...`);
    await processDailyCurtailment(TARGET_DATE);
    
    // Check if we fixed all the periods
    const stillMissing = await db.execute(sql`
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
    
    if (stillMissing.rows.length === 0) {
      console.log(`\n✅ Success! All 48 periods are now present for ${TARGET_DATE}.`);
    } else {
      const missing = stillMissing.rows.map(row => Number(row.period));
      console.log(`\n⚠️ Still missing ${missing.length} periods: ${missing.join(', ')}`);
    }
    
    console.log("\n=== Processing Complete ===\n");
  } catch (error) {
    console.error('Error during processing:', error);
  }
}

// Run the script
main();
/**
 * This script finds and fixes missing Bitcoin calculations by reprocessing the data.
 * It leverages the historicalReconciliation service to perform the calculations.
 */

import { db } from "@db";
import { curtailmentRecords, historicalBitcoinCalculations } from "@db/schema";
import { sql, and, eq, between } from "drizzle-orm";
import { format, parseISO, eachDayOfInterval } from "date-fns";
import { reconcileDay, reprocessDay, auditAndFixBitcoinCalculations } from "@/server/services/historicalReconciliation";
import pLimit from "p-limit";

// Configuration
const DEFAULT_MINER_MODEL = "S19J_PRO";
const CONCURRENCY_LIMIT = 5; // Number of dates to process concurrently

/**
 * Find dates with missing Bitcoin calculations
 */
async function findDatesWithMissingCalculations(startDate: string, endDate: string): Promise<string[]> {
  const result = await db.execute(sql`
    WITH daily_curtailment AS (
      SELECT DISTINCT settlement_date
      FROM curtailment_records
      WHERE settlement_date BETWEEN ${startDate} AND ${endDate}
    ),
    daily_bitcoin AS (
      SELECT DISTINCT settlement_date
      FROM historical_bitcoin_calculations
      WHERE settlement_date BETWEEN ${startDate} AND ${endDate}
      AND miner_model = ${DEFAULT_MINER_MODEL}
    ),
    missing_dates AS (
      SELECT c.settlement_date
      FROM daily_curtailment c
      LEFT JOIN daily_bitcoin b ON c.settlement_date = b.settlement_date
      WHERE b.settlement_date IS NULL
    )
    SELECT settlement_date
    FROM missing_dates
    ORDER BY settlement_date
  `);

  return result.rows.map(row => row.settlement_date);
}

/**
 * Find dates with incomplete Bitcoin calculations (has curtailment records but missing some calculations)
 */
async function findDatesWithIncompleteCalculations(startDate: string, endDate: string): Promise<string[]> {
  const result = await db.execute(sql`
    WITH daily_stats AS (
      SELECT 
        c.settlement_date,
        COUNT(DISTINCT c.settlement_period || '-' || c.farm_id) as curtailment_keys,
        COUNT(DISTINCT b.settlement_period || '-' || b.farm_id) as bitcoin_keys
      FROM curtailment_records c
      LEFT JOIN historical_bitcoin_calculations b ON 
        c.settlement_date = b.settlement_date AND
        c.settlement_period = b.settlement_period AND
        c.farm_id = b.farm_id AND
        b.miner_model = ${DEFAULT_MINER_MODEL}
      WHERE c.settlement_date BETWEEN ${startDate} AND ${endDate}
      GROUP BY c.settlement_date
    )
    SELECT settlement_date
    FROM daily_stats
    WHERE curtailment_keys > bitcoin_keys
    ORDER BY settlement_date
  `);

  return result.rows.map(row => row.settlement_date);
}

/**
 * Fix missing Bitcoin calculations by reprocessing data for specific dates
 */
async function fixMissingCalculations(startDate: string, endDate: string) {
  console.log(`Fixing missing Bitcoin calculations from ${startDate} to ${endDate}`);
  
  // Find dates with completely missing calculations
  console.log("\nFinding dates with missing calculations...");
  const missingDates = await findDatesWithMissingCalculations(startDate, endDate);
  console.log(`Found ${missingDates.length} dates with no calculations at all`);
  
  // Find dates with incomplete calculations
  console.log("\nFinding dates with incomplete calculations...");
  const incompleteDates = await findDatesWithIncompleteCalculations(startDate, endDate);
  console.log(`Found ${incompleteDates.length} dates with incomplete calculations`);
  
  // Combine and deduplicate
  const allDatesToFix = [...new Set([...missingDates, ...incompleteDates])];
  console.log(`\nTotal dates to fix: ${allDatesToFix.length}`);
  
  if (allDatesToFix.length === 0) {
    console.log("No dates need fixing. All calculations are up to date.");
    return;
  }
  
  // Confirm before proceeding
  console.log("\nSample dates that will be reprocessed:");
  allDatesToFix.slice(0, 5).forEach(date => console.log(`- ${date}`));
  
  if (allDatesToFix.length > 5) {
    console.log(`... and ${allDatesToFix.length - 5} more`);
  }
  
  // Process dates with concurrency limit to avoid overwhelming the system
  console.log(`\nBeginning reprocessing with concurrency limit of ${CONCURRENCY_LIMIT}...`);
  const limit = pLimit(CONCURRENCY_LIMIT);
  
  const results = await Promise.all(
    allDatesToFix.map(date => 
      limit(async () => {
        console.log(`Reprocessing date: ${date}`);
        const startTime = Date.now();
        try {
          // Use audit and fix function from historicalReconciliation service
          const result = await auditAndFixBitcoinCalculations(date);
          const duration = ((Date.now() - startTime) / 1000).toFixed(1);
          console.log(`✅ Completed ${date} in ${duration}s - Fixed: ${result.fixed}, Verified: ${result.verified}`);
          return { date, success: true, message: `Fixed: ${result.fixed}, Verified: ${result.verified}` };
        } catch (error) {
          const duration = ((Date.now() - startTime) / 1000).toFixed(1);
          console.error(`❌ Failed ${date} after ${duration}s:`, error);
          return { date, success: false, message: error instanceof Error ? error.message : String(error) };
        }
      })
    )
  );
  
  // Summarize results
  const successful = results.filter(r => r.success).length;
  const failed = results.filter(r => !r.success).length;
  
  console.log("\n=== Reprocessing Summary ===");
  console.log(`Total dates processed: ${results.length}`);
  console.log(`Successfully fixed: ${successful}`);
  console.log(`Failed to fix: ${failed}`);
  
  if (failed > 0) {
    console.log("\nDates that failed to process:");
    results.filter(r => !r.success).forEach(result => {
      console.log(`- ${result.date}: ${result.message}`);
    });
  }
}

/**
 * Main function
 */
async function main() {
  const args = process.argv.slice(2);
  let startDate = "2022-01-01";
  let endDate = format(new Date(), "yyyy-MM-dd");
  
  // Parse command line arguments if provided
  if (args.length >= 1) {
    startDate = args[0];
  }
  
  if (args.length >= 2) {
    endDate = args[1];
  }
  
  try {
    await fixMissingCalculations(startDate, endDate);
    process.exit(0);
  } catch (error) {
    console.error("Error fixing missing calculations:", error);
    process.exit(1);
  }
}

// Run the script if executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  main();
}

export { fixMissingCalculations, findDatesWithMissingCalculations, findDatesWithIncompleteCalculations };
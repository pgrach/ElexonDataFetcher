/**
 * Daily Reconciliation Check
 * 
 * This script runs automatically to check the reconciliation status for the current and previous day.
 * If any issues are detected, it attempts to fix them automatically.
 * 
 * Usage:
 *   npx tsx daily_reconciliation_check.ts
 */

import { format, subDays } from "date-fns";
import pg from "pg";
import { fixDate } from "./simple_reconcile";

const RECENT_DAYS_TO_CHECK = 2; // Check today and yesterday

// Create database connection
const dbUrl = process.env.DATABASE_URL;
if (!dbUrl) {
  console.error('DATABASE_URL environment variable is not set');
  process.exit(1);
}

const pool = new pg.Pool({
  connectionString: dbUrl,
});

async function checkDateReconciliationStatus(date: string) {
  const client = await pool.connect();
  
  try {
    const query = `
      WITH required_combinations AS (
        SELECT 
          count(DISTINCT (settlement_period || '-' || farm_id)) * 3 AS expected_count
        FROM 
          curtailment_records
        WHERE 
          settlement_date = $1
      ),
      actual_calculations AS (
        SELECT 
          COUNT(*) AS actual_count
        FROM 
          historical_bitcoin_calculations
        WHERE 
          settlement_date = $1
      )
      SELECT 
        $1 AS date,
        COALESCE((SELECT expected_count FROM required_combinations), 0) AS expected_count,
        COALESCE((SELECT actual_count FROM actual_calculations), 0) AS actual_count,
        CASE 
          WHEN (SELECT expected_count FROM required_combinations) = 0 THEN 100
          ELSE ROUND(((SELECT actual_count FROM actual_calculations)::numeric / 
                    (SELECT expected_count FROM required_combinations)) * 100, 2)
        END AS completion_percentage
    `;
    
    const result = await client.query(query, [date]);
    return {
      date,
      expected: parseInt(result.rows[0].expected_count || '0'),
      actual: parseInt(result.rows[0].actual_count || '0'),
      completionPercentage: parseFloat(result.rows[0].completion_percentage || '0')
    };
  } finally {
    client.release();
  }
}

async function runDailyCheck() {
  console.log("=== Starting Daily Reconciliation Check ===\n");
  const today = new Date();
  
  // Check recent days (today and yesterday)
  console.log(`Checking the last ${RECENT_DAYS_TO_CHECK} days for reconciliation issues...`);
  
  const dates: string[] = [];
  for (let i = 0; i < RECENT_DAYS_TO_CHECK; i++) {
    const date = subDays(today, i);
    dates.push(format(date, "yyyy-MM-dd"));
  }
  
  console.log(`Dates to check: ${dates.join(", ")}`);
  
  // Check reconciliation status for each date
  const dateStatuses = await Promise.all(dates.map(date => checkDateReconciliationStatus(date)));
  
  // Filter for dates with missing calculations
  const missingDates = dateStatuses.filter(status => 
    status.completionPercentage < 100 && status.expected > 0
  );
  
  if (missingDates.length === 0) {
    console.log(`\n✅ All checked dates are fully reconciled. No action needed.`);
    await pool.end();
    return {
      dates,
      missingDates: [],
      fixedDates: [],
      status: "fully_reconciled"
    };
  }
  
  console.log(`\nFound ${missingDates.length} dates with missing calculations:`);
  missingDates.forEach(d => {
    console.log(`- ${d.date}: ${d.actual}/${d.expected} calculations (${d.completionPercentage}%)`);
  });
  
  // Fix each date with missing calculations
  console.log(`\nAttempting to fix missing calculations...`);
  
  const results = [];
  const fixedDates = [];
  
  for (const { date } of missingDates) {
    console.log(`\nProcessing ${date}...`);
    try {
      const result = await fixDate(date);
      results.push(result);
      
      if (result.success) {
        fixedDates.push(date);
        console.log(`✅ Successfully processed ${date}`);
      } else {
        console.log(`⚠️ Could not completely fix ${date}: ${result.message}`);
      }
    } catch (error) {
      console.error(`Error processing ${date}:`, error);
    }
  }
  
  // Verify the final status of each date
  const finalStatuses = await Promise.all(
    missingDates.map(({ date }) => checkDateReconciliationStatus(date))
  );
  
  const successfullyReconciled = finalStatuses.filter(
    status => status.completionPercentage === 100
  );
  
  // Summary
  console.log(`\n=== Daily Reconciliation Check Summary ===`);
  console.log(`Dates Checked: ${dates.join(", ")}`);
  console.log(`Dates with Issues: ${missingDates.length}`);
  console.log(`Dates Fixed: ${successfullyReconciled.length}`);
  
  await pool.end();
  
  if (successfullyReconciled.length === missingDates.length) {
    console.log(`\n✅ All issues successfully fixed!`);
    return {
      dates,
      missingDates: missingDates.map(d => d.date),
      fixedDates: successfullyReconciled.map(d => d.date),
      status: "all_fixed"
    };
  } else {
    console.log(`\n⚠️ Some dates could not be fully fixed. Manual intervention may be required.`);
    return {
      dates,
      missingDates: missingDates.map(d => d.date),
      fixedDates: successfullyReconciled.map(d => d.date),
      status: "partial_fix"
    };
  }
}

// Run the reconciliation check if this script is executed directly
if (import.meta.url === `file://${process.argv[1]}`) {
  runDailyCheck()
    .then(() => {
      console.log("\n=== Daily Reconciliation Check Complete ===");
      process.exit(0);
    })
    .catch(error => {
      console.error("Fatal error during daily reconciliation check:", error);
      process.exit(1);
    });
}

export { runDailyCheck };
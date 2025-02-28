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
import { findDatesWithMissingCalculations, fixDateReconciliation } from "./comprehensive_reconcile";

const RECENT_DAYS_TO_CHECK = 2; // Check today and yesterday

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
  
  // Find any dates with missing calculations
  const startDate = dates[dates.length - 1]; // Earliest date
  const endDate = dates[0]; // Most recent date
  
  const missingDates = await findDatesWithMissingCalculations(startDate, endDate);
  
  if (missingDates.length === 0) {
    console.log(`\n✅ All checked dates are fully reconciled. No action needed.`);
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
  
  for (const date of missingDates.map(d => d.date)) {
    console.log(`\nProcessing ${date}...`);
    const result = await fixDateReconciliation(date);
    results.push(result);
    
    if (result.finalStatus.calculations.reconciliationPercentage === 100) {
      fixedDates.push(date);
      console.log(`✅ Successfully fixed ${date}`);
    } else {
      console.log(`⚠️ Could not completely fix ${date} (${result.finalStatus.calculations.reconciliationPercentage}%)`);
    }
  }
  
  // Summary
  console.log(`\n=== Daily Reconciliation Check Summary ===`);
  console.log(`Dates Checked: ${dates.join(", ")}`);
  console.log(`Dates with Issues: ${missingDates.length}`);
  console.log(`Dates Fixed: ${fixedDates.length}`);
  
  if (fixedDates.length === missingDates.length) {
    console.log(`\n✅ All issues successfully fixed!`);
    return {
      dates,
      missingDates: missingDates.map(d => d.date),
      fixedDates,
      status: "all_fixed"
    };
  } else {
    console.log(`\n⚠️ Some dates could not be fully fixed. Manual intervention may be required.`);
    return {
      dates,
      missingDates: missingDates.map(d => d.date),
      fixedDates,
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
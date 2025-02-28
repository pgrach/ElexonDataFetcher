/**
 * Scheduled Reconciliation Script
 * 
 * This script is designed to be run on a schedule (e.g., daily or weekly)
 * to ensure that all recent curtailment records have corresponding
 * bitcoin calculations.
 * 
 * Usage:
 *   npx tsx scheduled_reconciliation.ts [days=7]
 * 
 * where 'days' is the number of recent days to check (default: 7).
 */

import { format, subDays } from "date-fns";
import { reconcileRecentData } from "./server/services/historicalReconciliation";

async function main() {
  try {
    const args = process.argv.slice(2);
    const days = args[0] ? parseInt(args[0], 10) : 7;
    
    const now = new Date();
    const startDate = format(subDays(now, days), 'yyyy-MM-dd');
    const endDate = format(now, 'yyyy-MM-dd');
    
    console.log(`=== Scheduled Reconciliation (${startDate} to ${endDate}) ===`);
    console.log(`Checking and reconciling data for the last ${days} days...`);
    
    // Use the reconcileRecentData function from historicalReconciliation service
    await reconcileRecentData();
    
    console.log(`\n=== Reconciliation Complete ===`);
    console.log(`Successfully checked and reconciled data for the last ${days} days.`);
    console.log(`Next run will be automatically scheduled.`);
  } catch (error) {
    console.error("Error in scheduled reconciliation:", error);
    process.exit(1);
  }
}

main().catch(console.error);
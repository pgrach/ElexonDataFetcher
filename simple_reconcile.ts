/**
 * Simple Reconciliation Tool
 * 
 * Uses the historicalReconciliation service directly to fix a specific date.
 */

import { auditAndFixBitcoinCalculations, reconcileDay } from "./server/services/historicalReconciliation";

async function main() {
  const date = process.argv[2] || '2025-02-28';
  console.log(`Reconciling data for ${date}...`);
  
  try {
    // First try the simplest approach - reconcileDay
    console.log(`Calling reconcileDay for ${date}...`);
    await reconcileDay(date);
    console.log(`reconcileDay completed for ${date}`);
    
    // Then verify with auditAndFix
    console.log(`\nVerifying calculations with auditAndFixBitcoinCalculations for ${date}...`);
    const result = await auditAndFixBitcoinCalculations(date);
    
    if (result.success) {
      console.log(`✅ Successfully processed ${date}: ${result.message}`);
    } else {
      console.log(`❌ Failed to process ${date}: ${result.message}`);
    }
  } catch (error) {
    console.error(`Error reconciling ${date}:`, error);
  }
}

main().catch(console.error);
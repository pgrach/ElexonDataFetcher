/**
 * Unified Reconciliation Script
 * 
 * This script is a simple wrapper around update_summaries.ts for compatibility.
 * It ensures that all calculations for a specific date are up-to-date.
 */

// Import functions directly from update_summaries.ts
import * as updateSummariesModule from './update_summaries';

// Re-export the functions from update_summaries.ts
export const updateSummaries = updateSummariesModule.updateSummaries;
export const updateBitcoinCalculations = updateSummariesModule.updateBitcoinCalculations;

async function main() {
  const args = process.argv.slice(2);
  let date: string | undefined;
  
  // Handle 'date <DATE>' format
  if (args.length >= 2 && args[0] === 'date') {
    date = args[1];
  } 
  // Handle single date argument
  else if (args.length === 1) {
    date = args[0];
  }
  
  if (!date) {
    console.error('No date provided. Usage: npx tsx unified_reconciliation.ts date YYYY-MM-DD');
    process.exit(1);
  }
  
  console.log(`Running unified reconciliation for ${date}`);
  
  try {
    // Update summaries first
    await updateSummaries(date);
    
    // Then update Bitcoin calculations
    await updateBitcoinCalculations(date);
    
    console.log(`Reconciliation completed for ${date}`);
    process.exit(0);
  } catch (error) {
    console.error(`Error in reconciliation: ${error}`);
    process.exit(1);
  }
}

main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
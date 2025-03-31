/**
 * Fix Missing Bitcoin Calculations for 2025-03-29
 * 
 * This script will directly focus on fixing the missing Bitcoin calculations
 * for March 29, 2025 using the existing reconciliation services.
 */

import { auditAndFixBitcoinCalculations } from './server/services/historicalReconciliation';
import { processSingleDay } from './server/services/bitcoinService';

// Date to process
const date = '2025-03-29';
const MINER_MODEL_LIST = ['S19J_PRO', 'S9', 'M20S'];

// Main function to fix Bitcoin calculations
async function fixBitcoinCalculations() {
  console.log(`Starting Bitcoin calculation fix for ${date}...`);
  
  try {
    // First try using the comprehensive audit and fix function
    console.log(`Using auditAndFixBitcoinCalculations for ${date}...`);
    const result = await auditAndFixBitcoinCalculations(date);
    
    if (result.success) {
      console.log(`Bitcoin calculations updated successfully: ${result.message}`);
    } else {
      console.log(`Error with automated fix: ${result.message}`);
      
      // Try manual approach with each miner model
      console.log(`Trying manual approach for each miner model...`);
      
      for (const minerModel of MINER_MODEL_LIST) {
        try {
          console.log(`Processing ${minerModel} for ${date}...`);
          await processSingleDay(date, minerModel);
          console.log(`Successfully processed ${minerModel} for ${date}`);
        } catch (error) {
          console.error(`Error processing ${minerModel} for ${date}:`, error);
        }
      }
    }
    
    console.log(`Bitcoin calculation fixes complete for ${date}`);
  } catch (error) {
    console.error(`Fatal error:`, error);
    process.exit(1);
  }
}

// Run the script
fixBitcoinCalculations()
  .then(() => {
    console.log('Script execution complete');
    process.exit(0);
  })
  .catch(error => {
    console.error('Unhandled error:', error);
    process.exit(1);
  });
/**
 * Simple runner script to execute the 2025-03-24 data reingestion
 * 
 * This script only updates the curtailment_records table for 2025-03-24
 * without affecting summary tables or Bitcoin calculations.
 */

import { reingestCurtailmentRecords } from "./reingest_2025_03_24";

console.log('\n============================================');
console.log('STARTING CURTAILMENT REINGESTION (2025-03-24)');
console.log('============================================\n');

// Execute the reingestion
(async () => {
  try {
    const startTime = Date.now();
    await reingestCurtailmentRecords();
    const endTime = Date.now();
    
    console.log('\n============================================');
    console.log('CURTAILMENT REINGESTION COMPLETED');
    console.log(`Duration: ${((endTime - startTime) / 1000).toFixed(2)} seconds`);
    console.log('============================================\n');
    console.log('NOTE: This process only updated the curtailment_records table.');
    console.log('To update summary tables and Bitcoin calculations, run:');
    console.log('   npx tsx server/scripts/update_2025_03_24_complete.ts');
    
    process.exit(0);
  } catch (error) {
    console.error('\nREINGESTION FAILED:', error);
    process.exit(1);
  }
})();
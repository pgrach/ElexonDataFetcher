/**
 * Process Curtailment Data for a Specific Date
 * 
 * This script processes curtailment data for a specific date,
 * then updates the Bitcoin calculations for all miner models.
 */

import { processDailyCurtailment } from './server/services/curtailment';
import { format } from 'date-fns';

async function main() {
  try {
    // Get the date from command-line arguments or use default
    const dateToProcess = process.argv[2] || format(new Date(), 'yyyy-MM-dd');
    
    console.log(`\n=== Starting Curtailment Processing for ${dateToProcess} ===\n`);
    
    // Step 1: Process the curtailment data
    await processDailyCurtailment(dateToProcess);
    
    console.log(`\n=== Curtailment Processing Complete for ${dateToProcess} ===\n`);
    
    console.log(`Next steps:`);
    console.log(`1. Process Bitcoin calculations for all miner models:`);
    console.log(`   npx tsx server/services/bitcoinService.ts process-date ${dateToProcess} S19J_PRO`);
    console.log(`   npx tsx server/services/bitcoinService.ts process-date ${dateToProcess} S9`);
    console.log(`   npx tsx server/services/bitcoinService.ts process-date ${dateToProcess} M20S`);
    console.log(`2. Update monthly summary:`);
    console.log(`   npx tsx server/services/bitcoinService.ts recalculate-monthly ${dateToProcess.substring(0, 7)}`);
    console.log(`3. Update yearly summary:`);
    console.log(`   npx tsx server/services/bitcoinService.ts recalculate-yearly ${dateToProcess.substring(0, 4)}`);
  } catch (error) {
    console.error('Error processing curtailment data:', error);
    process.exit(1);
  }
}

main();
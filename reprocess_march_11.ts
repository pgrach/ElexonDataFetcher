/**
 * Reprocess March 11 2025 Data
 * 
 * This script uses the existing optimized_critical_date_processor.ts functionality
 * to reingest and clean data for 2025-03-11
 */

import { spawn } from 'child_process';

// The date we want to process
const DATE = '2025-03-11';

console.log(`===== Starting Reingestion Process for ${DATE} =====`);
console.log('This will reingest all data from Elexon API and reconcile Bitcoin calculations');

// Execute the optimized critical date processor for our target date
// This will handle the reingestion and reconciliation process
const processor = spawn('npx', ['tsx', 'optimized_critical_date_processor.ts', DATE]);

// Handle output from the processor
processor.stdout.on('data', (data) => {
  console.log(`${data}`);
});

processor.stderr.on('data', (data) => {
  console.error(`${data}`);
});

// Handle process completion
processor.on('close', (code) => {
  if (code === 0) {
    console.log(`\n===== Reingestion completed successfully for ${DATE} =====`);
    
    // After reingestion is complete, run reconciliation to update Bitcoin calculations
    console.log('\nRunning Bitcoin calculations reconciliation...');
    
    const reconciliation = spawn('npx', ['tsx', 'unified_reconciliation.ts', 'date', DATE]);
    
    reconciliation.stdout.on('data', (data) => {
      console.log(`${data}`);
    });
    
    reconciliation.stderr.on('data', (data) => {
      console.error(`${data}`);
    });
    
    reconciliation.on('close', (reconcileCode) => {
      if (reconcileCode === 0) {
        console.log(`\n===== Reconciliation completed successfully for ${DATE} =====`);
        console.log('All data has been processed and Bitcoin calculations have been updated.');
      } else {
        console.error(`\n===== Reconciliation failed with code ${reconcileCode} =====`);
        console.error('Please check the logs for more information.');
      }
    });
  } else {
    console.error(`\n===== Reingestion failed with code ${code} =====`);
    console.error('Please check the logs for more information.');
  }
});
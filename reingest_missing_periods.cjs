/**
 * Script to reingest missing periods for 2025-03-07
 * Uses the raw child_process approach to avoid ES module issues
 */

const { spawn, execSync } = require('child_process');
const date = '2025-03-07';

// Define the missing period ranges
const missingRanges = [
  { start: 19, end: 41 },
  { start: 48, end: 48 }  // Just period 48
];

function log(message) {
  console.log(`[${new Date().toLocaleTimeString()}] ${message}`);
}

async function executeCommand(command) {
  return new Promise((resolve, reject) => {
    log(`Executing: ${command}`);
    
    try {
      const output = execSync(command, { stdio: 'inherit' });
      resolve();
    } catch (error) {
      log(`Error executing command: ${error.message}`);
      reject(error);
    }
  });
}

async function processRanges() {
  for (const range of missingRanges) {
    log(`Processing range ${range.start}-${range.end}`);
    
    // Use the optimized_critical_date_processor.ts for specific period ranges
    const command = `npx tsx optimized_critical_date_processor.ts ${date} ${range.start} ${range.end}`;
    
    try {
      await executeCommand(command);
      log(`Successfully processed range ${range.start}-${range.end}`);
    } catch (error) {
      log(`Failed to process range ${range.start}-${range.end}: ${error.message}`);
    }
  }
  
  log('Processing complete. Running reconciliation...');
  
  // Use the unified_reconciliation.ts to update Bitcoin calculations
  try {
    await executeCommand('npx tsx unified_reconciliation.ts date 2025-03-07');
    log('Reconciliation complete');
  } catch (error) {
    log(`Failed to run reconciliation: ${error.message}`);
  }
  
  // Verify results
  try {
    await executeCommand('npx tsx unified_reconciliation.ts status');
    log('Status check complete');
  } catch (error) {
    log(`Failed to check status: ${error.message}`);
  }
}

// Run the script
log('Starting reingestion of missing periods for 2025-03-07');
processRanges().then(() => {
  log('Script execution completed');
}).catch(error => {
  log(`Script failed: ${error.message}`);
  process.exit(1);
});
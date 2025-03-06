#!/usr/bin/env tsx

/**
 * Reconciliation Script Runner
 * 
 * This script provides a centralized way to run any of the reconciliation 
 * scripts with appropriate arguments.
 * 
 * Usage:
 *   npm run reconcile -- [script] [args]
 * 
 * Available Scripts:
 *   daily      - Run the daily reconciliation check
 *   complete   - Run the complete reingestion process
 *   unified    - Run the unified reconciliation system
 * 
 * Examples:
 *   npm run reconcile -- daily 2
 *   npm run reconcile -- complete 2025-03-06
 *   npm run reconcile -- unified status
 */

import { execSync } from 'child_process';
import { join } from 'path';

const scripts = {
  daily: './scripts/reconciliation/daily_reconciliation_check.ts',
  complete: './scripts/reconciliation/complete_reingestion_process.ts',
  unified: './scripts/reconciliation/unified_reconciliation.ts'
};

function printUsage() {
  console.log(`
Bitcoin Mining Analytics - Reconciliation Runner

Usage:
  npm run reconcile -- [script] [args]

Available Scripts:
  daily      - Run the daily reconciliation check
  complete   - Run the complete reingestion process
  unified    - Run the unified reconciliation system

Examples:
  npm run reconcile -- daily 2
  npm run reconcile -- complete 2025-03-06
  npm run reconcile -- unified status
  `);
}

function main() {
  const args = process.argv.slice(2);
  
  if (args.length === 0 || args[0] === '--help' || args[0] === '-h') {
    printUsage();
    process.exit(0);
  }
  
  const scriptName = args[0];
  const scriptArgs = args.slice(1);
  
  if (!Object.keys(scripts).includes(scriptName)) {
    console.error(`Error: Unknown script "${scriptName}"`);
    printUsage();
    process.exit(1);
  }
  
  const scriptPath = scripts[scriptName];
  const command = `npx tsx ${scriptPath} ${scriptArgs.join(' ')}`;
  
  console.log(`Running: ${command}`);
  try {
    execSync(command, { stdio: 'inherit' });
  } catch (error) {
    console.error(`Error running script: ${error.message}`);
    process.exit(1);
  }
}

main();
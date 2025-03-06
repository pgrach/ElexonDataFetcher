#!/usr/bin/env tsx

/**
 * Data Processing Script Runner
 * 
 * This script provides a centralized way to run any of the data processing
 * scripts with appropriate arguments.
 * 
 * Usage:
 *   npm run process-data -- [script] [args]
 * 
 * Available Scripts:
 *   reingest   - Reingest data for a specific date
 * 
 * Examples:
 *   npm run process-data -- reingest 2025-03-06
 *   npm run process-data -- reingest 2025-03-06 --verbose
 */

import { execSync } from 'child_process';
import { join } from 'path';

const scripts = {
  reingest: './scripts/data-processing/reingest-data.ts'
};

function printUsage() {
  console.log(`
Bitcoin Mining Analytics - Data Processing Runner

Usage:
  npm run process-data -- [script] [args]

Available Scripts:
  reingest   - Reingest data for a specific date

Examples:
  npm run process-data -- reingest 2025-03-06
  npm run process-data -- reingest 2025-03-06 --verbose
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
#!/usr/bin/env tsx
/**
 * Helper script to trigger data reingestion for a specific date
 * 
 * This script is a simplified wrapper around the main reingest-data.ts
 * that allows easier execution through our tools.
 */

import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

async function main() {
  const date = '2025-03-04';
  console.log(`Starting data reingestion for ${date}...`);
  
  try {
    const { stdout, stderr } = await execAsync(`npx tsx reingest-data.ts ${date}`);
    
    if (stderr) {
      console.error(`Error output:`);
      console.error(stderr);
    }
    
    console.log(stdout);
    console.log(`Data reingestion for ${date} completed successfully.`);
  } catch (error) {
    console.error(`Failed to reingest data: ${error.message}`);
    if (error.stdout) console.log(error.stdout);
    if (error.stderr) console.error(error.stderr);
  }
}

main().catch(err => {
  console.error('Unhandled error:', err);
  process.exit(1);
});
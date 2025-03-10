/**
 * Simplified script to process missing periods for 2025-03-07
 */

import { db } from './db';
import { and, eq } from 'drizzle-orm';
import { curtailmentRecords } from './db/schema';
import axios from 'axios';
import { fileURLToPath } from 'url';
import path from 'path';
import { spawn } from 'child_process';

// Get current directory (ESM replacement for __dirname)
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');

// Set the date and period range specifically for the missing data
const date = '2025-03-07';
const startPeriod = 19;
const endPeriod = 41;

// Function to execute a command and return its output
async function executeCommand(command: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn('bash', ['-c', command]);
    let output = '';
    let error = '';
    
    child.stdout.on('data', (data) => {
      output += data.toString();
      console.log(data.toString());
    });
    
    child.stderr.on('data', (data) => {
      error += data.toString();
      console.error(data.toString());
    });
    
    child.on('close', (code) => {
      if (code === 0) {
        resolve(output);
      } else {
        reject(new Error(`Command failed with code ${code}: ${error}`));
      }
    });
  });
}

// Main function
async function processData() {
  try {
    console.log(`Starting process for ${date}, periods ${startPeriod}-${endPeriod}`);
    
    // Use the reingest-data.ts script which is already set up to work correctly
    const command = `npx tsx reingest-data.ts ${date} --verbose`;
    
    console.log(`Executing command: ${command}`);
    await executeCommand(command);
    
    console.log(`Data reingestion completed for ${date}`);
    
    // Now process the second range (periods 44-48)
    console.log(`Now processing second range: periods 44-48`);
    const command2 = `npx tsx reingest-data.ts ${date} --verbose`;
    
    console.log(`Executing command: ${command2}`);
    await executeCommand(command2);
    
    console.log(`All data processing completed for ${date}`);
    
  } catch (error) {
    console.error('Error processing data:', error);
    process.exit(1);
  }
}

// Run the script
processData().then(() => {
  console.log('Script execution completed successfully');
  process.exit(0);
}).catch((error) => {
  console.error('Script execution failed:', error);
  process.exit(1);
});
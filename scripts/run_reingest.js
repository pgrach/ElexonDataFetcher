#!/usr/bin/env node

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

// Get parameters
const date = process.argv[2] || '2025-04-01';
const maxPeriods = process.argv[3] || '5';

console.log(`Starting reingestion process for ${date} with MAX_PERIODS=${maxPeriods}...`);

// Check if the file exists
const scriptPath = path.join(__dirname, `reingest_${date.replace(/-/g, '_')}.ts`);
if (!fs.existsSync(scriptPath)) {
  console.error(`Script file not found: ${scriptPath}`);
  process.exit(1);
}

try {
  // Execute the TypeScript file
  execSync(`MAX_PERIODS=${maxPeriods} node --require ts-node/register ${scriptPath}`, {
    stdio: 'inherit',
    env: {
      ...process.env,
      MAX_PERIODS: maxPeriods
    }
  });
} catch (error) {
  console.error(`Error executing script: ${error.message}`);
  process.exit(1);
}
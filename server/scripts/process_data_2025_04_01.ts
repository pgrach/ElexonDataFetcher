/**
 * Command-line script to reingest data for April 1, 2025
 * 
 * This script uses the existing API endpoint for reingestion
 * which properly handles all tables and verification.
 */

import axios from 'axios';

// Configuration
const API_URL = 'http://localhost:3000/api/ingest/2025-04-01';
const TARGET_DATE = '2025-04-01';

async function main() {
  console.log('\n============================================');
  console.log(`STARTING COMPLETE REINGESTION FOR ${TARGET_DATE}`);
  console.log('============================================\n');
  
  try {
    const startTime = Date.now();
    
    // Use the built-in reingestion API endpoint
    const response = await axios.post(API_URL);
    
    // Display the results
    console.log('\n=== Reingestion Results ===');
    console.log(`Status: ${response.status} ${response.statusText}`);
    console.log('Response:', JSON.stringify(response.data, null, 2));
    
    const endTime = Date.now();
    console.log('\n============================================');
    console.log('REINGESTION COMPLETED SUCCESSFULLY');
    console.log(`Duration: ${((endTime - startTime) / 1000).toFixed(2)} seconds`);
    console.log('============================================\n');
    
    process.exit(0);
  } catch (error) {
    console.error('\nREINGESTION FAILED:');
    if (axios.isAxiosError(error)) {
      console.error('API Error:', error.response?.data || error.message);
    } else {
      console.error(error);
    }
    process.exit(1);
  }
}

// Run the script
main();
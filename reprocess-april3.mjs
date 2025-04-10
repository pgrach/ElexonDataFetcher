/**
 * Data Reprocessing Script for 2025-04-03
 * 
 * This script uses the application's built-in API endpoint for data reingestion
 * to reprocess all data for 2025-04-03.
 */

import fetch from 'node-fetch';

const TARGET_DATE = "2025-04-03";
const BASE_URL = "http://localhost:3000";

async function reprocessDate() {
  console.log(`\n=== Starting Reprocessing for ${TARGET_DATE} ===`);
  
  try {
    // Step 1: Use the re-ingest API endpoint
    console.log(`Triggering reingestion for ${TARGET_DATE}...`);
    const response = await fetch(`${BASE_URL}/api/ingest/${TARGET_DATE}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      }
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`API returned error ${response.status}: ${errorText}`);
    }
    
    const result = await response.json();
    
    console.log(`\n=== Reprocessing Complete ===`);
    console.log(`Date: ${TARGET_DATE}`);
    console.log(`Status: Success`);
    console.log(`Stats:`, result.stats);
    console.log(`Completed at: ${new Date().toISOString()}`);
    
  } catch (error) {
    console.error("Error during reprocessing:", error.message);
    process.exit(1);
  }
}

// Execute the reprocessing
reprocessDate().catch(error => {
  console.error("Fatal error:", error);
  process.exit(1);
});
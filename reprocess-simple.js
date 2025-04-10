/**
 * Simple Data Reprocessing Script for 2025-04-03
 * 
 * This script reprocesses all 48 settlement periods for 2025-04-03 using the
 * existing application logic.
 */

// Import the Express app to access the API endpoints
const axios = require('axios');

const TARGET_DATE = "2025-04-03";

async function runReprocessing() {
  console.log(`\n=== Starting Reprocessing for ${TARGET_DATE} ===`);
  
  try {
    // Use the existing API endpoint for data re-ingestion
    console.log(`Sending re-ingest request for ${TARGET_DATE}...`);
    
    // Make API call to the internal re-ingest endpoint
    const response = await axios.post(`http://localhost:3000/api/ingest/${TARGET_DATE}`);
    
    // Check the response
    if (response.status === 200) {
      console.log(`\n=== Reprocessing Successful ===`);
      console.log(`Date: ${TARGET_DATE}`);
      console.log(`Response:`, response.data);
      console.log(`Completed at: ${new Date().toISOString()}`);
    } else {
      console.error(`Reprocessing failed with status ${response.status}`);
      console.error(`Response:`, response.data);
    }
  } catch (error) {
    console.error("Error during reprocessing:", error.message);
    if (error.response) {
      console.error("Response data:", error.response.data);
    }
  }
}

// Execute the reprocessing
runReprocessing().catch(error => {
  console.error("Fatal error:", error);
});
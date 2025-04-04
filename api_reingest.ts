/**
 * API-based Data Reingest Script for March 21, 2025
 * 
 * This script uses the existing API endpoint to reingest data for March
 * 21, 2025 which is much faster than directly manipulating the database.
 */

import axios from 'axios';

const TARGET_DATE = '2025-03-21';

// Function to reingest data
async function reingestData(date: string): Promise<void> {
  console.log(`Starting data reingest for ${date} via API...`);
  
  try {
    // Call the API endpoint
    const response = await axios.post(`http://localhost:3000/api/ingest/${date}`);
    
    console.log('API Response:');
    console.log(JSON.stringify(response.data, null, 2));
    
    if (response.data.stats) {
      console.log('\nReingest Summary:');
      console.log(`- Total Records: ${response.data.stats.records}`);
      console.log(`- Settlement Periods: ${response.data.stats.periods}`);
      console.log(`- Total Volume: ${response.data.stats.volume} MWh`);
      console.log(`- Total Payment: £${Math.abs(parseFloat(response.data.stats.payment)).toFixed(2)}`);
    }
    
    console.log('\nData reingest completed successfully.');
  } catch (error) {
    console.error('Error during data reingest:', error);
    throw error;
  }
}

// Run the reingest
reingestData(TARGET_DATE);
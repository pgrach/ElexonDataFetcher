/**
 * Direct check for Beatrice wind farm using specific Elexon API endpoints
 * This script uses a more direct approach to query the Elexon API
 */

import axios from "axios";

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMUS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Check a specific BMU for a given date
async function checkSpecificBMU(bmuId: string, date: string): Promise<void> {
  try {
    console.log(`Checking BMU ${bmuId} for date ${date}...`);
    
    // Check if the BMU had any actions on this date using a specialized endpoint
    const url = `${ELEXON_BASE_URL}/datasets/BALNGMG?bmUnit=${bmuId}&settlementDate=${date}`;
    
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });

    if (response.data && response.data.data && response.data.data.length > 0) {
      console.log(`FOUND ${response.data.data.length} actions for BMU ${bmuId} on ${date}`);
      console.log(`Details: ${JSON.stringify(response.data.data[0], null, 2)}`);
    } else {
      console.log(`No actions found for BMU ${bmuId} on ${date}`);
    }
  } catch (error: any) {
    if (error.response && error.response.status === 404) {
      console.log(`No data available for BMU ${bmuId} on ${date} (404 Not Found)`);
    } else {
      console.error(`Error checking BMU ${bmuId} on ${date}:`, error.message);
    }
  }
}

async function main(): Promise<void> {
  // Check critical dates in February 2025 - first, middle, and last days
  const criticalDates = ['2025-02-01', '2025-02-15', '2025-02-28'];
  
  console.log(`Starting focused direct check for Beatrice wind farm BMUs`);
  console.log(`Checking ${BEATRICE_BMUS.length} BMUs on ${criticalDates.length} key dates in February 2025`);
  
  let totalChecks = 0;
  let notFoundCount = 0;
  let foundCount = 0;
  
  for (const date of criticalDates) {
    console.log(`\n=== Checking date: ${date} ===`);
    
    for (const bmu of BEATRICE_BMUS) {
      totalChecks++;
      
      try {
        console.log(`Checking BMU ${bmu} for date ${date}...`);
        
        // Try a more reliable endpoint specifically for Bids/Offers
        const url = `${ELEXON_BASE_URL}/datasets/BOAV?bmUnit=${bmu}&settlementDate=${date}`;
        
        const response = await axios.get(url, {
          headers: { 'Accept': 'application/json' },
          timeout: 30000
        });

        if (response.data && response.data.data && response.data.data.length > 0) {
          foundCount++;
          console.log(`FOUND ${response.data.data.length} bid/offer data points for BMU ${bmu} on ${date}`);
          console.log(`Details: ${JSON.stringify(response.data.data[0], null, 2)}`);
        } else {
          notFoundCount++;
          console.log(`No bid/offer data found for BMU ${bmu} on ${date}`);
        }
      } catch (error: any) {
        if (error.response && error.response.status === 404) {
          notFoundCount++;
          console.log(`No data available for BMU ${bmu} on ${date} (404 Not Found)`);
        } else {
          console.error(`Error checking BMU ${bmu} on ${date}:`, error.message);
        }
      }
      
      await delay(2000); // Longer delay to avoid rate limiting
    }
    
    console.log(`Completed checks for ${date}`);
    console.log('-'.repeat(50));
    await delay(3000); // Additional delay between dates
  }
  
  console.log(`\nSummary of Beatrice wind farm checks for February 2025:`);
  console.log(`Total checks performed: ${totalChecks}`);
  console.log(`Records found: ${foundCount}`);
  console.log(`Records not found (404): ${notFoundCount}`);
  
  if (foundCount === 0) {
    console.log(`CONCLUSION: No curtailment records found for Beatrice wind farm in February 2025`);
  } else {
    console.log(`CONCLUSION: Found ${foundCount} curtailment records for Beatrice wind farm in February 2025`);
  }
}

main().then(() => {
  console.log("Direct check execution completed");
}).catch(error => {
  console.error("Error in main execution:", error);
});
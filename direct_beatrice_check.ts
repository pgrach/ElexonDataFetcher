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

// Check for a specific BMU directly
async function checkSpecificBMU(bmuId: string, date: string): Promise<void> {
  try {
    console.log(`Directly checking ${bmuId} for date ${date}...`);

    // Using the direct BMU endpoint for active bids
    const url = `${ELEXON_BASE_URL}/balancing/bid-offer/bmu-id/${bmuId}/active?settlementDate=${date}`;
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });

    if (response.data && response.data.data && response.data.data.length > 0) {
      console.log(`Found ${response.data.data.length} active curtailment records for ${bmuId} on ${date}`);
      console.log(response.data.data);
    } else {
      console.log(`No active curtailment records for ${bmuId} on ${date}`);
    }
  } catch (error) {
    console.error(`Error checking ${bmuId} for ${date}:`, error.message);
  }
}

async function main(): Promise<void> {
  const dates = ['2025-01-15', '2025-02-15', '2025-03-01', '2025-03-13'];
  
  for (const bmuId of BEATRICE_BMUS) {
    for (const date of dates) {
      await checkSpecificBMU(bmuId, date);
      await delay(2000); // Give time between requests
    }
  }
}

main().then(() => {
  console.log("Direct Beatrice wind farm check completed");
}).catch(error => {
  console.error("Error running direct check:", error);
});
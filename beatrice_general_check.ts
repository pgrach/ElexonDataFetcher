/**
 * General check for Beatrice wind farm activity in Elexon
 * Using more general endpoints to check for any data
 */

import axios from "axios";

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMUS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Check for a specific date using the general balancing actions endpoint
async function checkBalancingActions(date: string): Promise<void> {
  try {
    console.log(`Checking balancing actions for date ${date}...`);

    // Using the balancing actions endpoint which shows all actions for a date
    const url = `${ELEXON_BASE_URL}/balancing/actions?settlementDate=${date}`;
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });

    if (response.data && response.data.data && response.data.data.length > 0) {
      console.log(`Found ${response.data.data.length} total balancing actions on ${date}`);
      
      // Filter for Beatrice BMUs
      const beatriceActions = response.data.data.filter(
        (action: any) => BEATRICE_BMUS.includes(action.bmUnitId)
      );
      
      if (beatriceActions.length > 0) {
        console.log(`Found ${beatriceActions.length} Beatrice actions on ${date}:`);
        beatriceActions.forEach((action: any, index: number) => {
          console.log(`Action ${index + 1}: BMU ${action.bmUnitId}, Volume: ${action.volume}, Type: ${action.actionType}`);
        });
      } else {
        console.log(`No Beatrice actions found among ${response.data.data.length} balancing actions for ${date}`);
      }
    } else {
      console.log(`No balancing actions found for ${date}`);
    }
  } catch (error) {
    console.error(`Error checking balancing actions for ${date}:`, error.message);
  }
}

async function main(): Promise<void> {
  const dates = ['2025-01-15', '2025-02-15', '2025-03-01', '2025-03-13'];
  
  for (const date of dates) {
    await checkBalancingActions(date);
    await delay(2000); // Give time between requests
  }
}

main().then(() => {
  console.log("General Beatrice wind farm check completed");
}).catch(error => {
  console.error("Error running general check:", error);
});
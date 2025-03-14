/**
 * Specific check for Beatrice in December 2024 when we know there was curtailment
 */

import axios from "axios";

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];

// Multiple dates in December to try
const DECEMBER_DATES = ["2024-12-01", "2024-12-05", "2024-12-10", "2024-12-15", "2024-12-20"];

/**
 * Check API for Beatrice records
 */
async function checkDate(date: string, period: number): Promise<void> {
  console.log(`\nChecking ${date}, period ${period}`);
  
  try {
    // Check bid stack
    const bidResponse = await axios.get(
      `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`, 
      { headers: { 'Accept': 'application/json' } }
    );
    
    // Check offer stack
    const offerResponse = await axios.get(
      `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`,
      { headers: { 'Accept': 'application/json' } }
    );
    
    // Combine the data
    const bidData = bidResponse.data?.data || [];
    const offerData = offerResponse.data?.data || [];
    const allData = [...bidData, ...offerData];
    
    console.log(`  Retrieved ${allData.length} total records (${bidData.length} bids, ${offerData.length} offers)`);
    
    // Look for Beatrice BMUs with no filters
    const beatriceRecords = allData.filter(record => BEATRICE_BMU_IDS.includes(record.id));
    
    if (beatriceRecords.length > 0) {
      console.log(`  FOUND ${beatriceRecords.length} BEATRICE RECORDS!`);
      beatriceRecords.forEach(record => {
        console.log(`    - BMU: ${record.id}, Volume: ${record.volume}, soFlag: ${record.soFlag}`);
      });
    } else {
      console.log(`  No Beatrice BMU records found`);
    }
    
    // Show some sample BMUs that were returned
    const uniqueBmus = [...new Set(allData.map(record => record.id))];
    console.log(`  Sample BMUs in response: ${uniqueBmus.slice(0, 5).join(', ')}...`);
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error(`  ERROR: ${error.message}`);
      if (error.response) {
        console.error(`  Status: ${error.response.status}`);
        console.error(`  Response: ${JSON.stringify(error.response.data)}`);
      }
    } else {
      console.error(`  ERROR: ${error instanceof Error ? error.message : String(error)}`);
    }
  }
}

/**
 * Main function
 */
async function main() {
  console.log("\n=== CHECKING FOR BEATRICE IN DECEMBER 2024 ===");
  
  for (const date of DECEMBER_DATES) {
    // Check multiple periods to increase chances of finding data
    for (const period of [1, 24, 48]) {
      await checkDate(date, period);
    }
  }
  
  console.log("\n=== CHECKING FEBRUARY 2025 AGAIN FOR COMPARISON ===");
  await checkDate("2025-02-15", 24);
}

main().catch(console.error);
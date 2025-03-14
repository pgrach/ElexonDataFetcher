/**
 * Direct check for Beatrice records in February 2025 using only the balancing settlement stack API
 * No additional filtering - just examining the raw API response for Beatrice BMUs
 */

import axios from "axios";

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];
const TARGET_DATES = ["2025-02-01", "2025-02-15", "2025-02-28"]; 
const REFERENCE_DATE = "2024-12-01"; // Known good date for comparison

/**
 * Check for any Beatrice BMU records in the raw API response
 */
async function checkForBeatrice(date: string, period: number): Promise<void> {
  console.log(`\nChecking ${date}, period ${period}`);
  
  try {
    // Get both bid and offer data
    const [bidResponse, offerResponse] = await Promise.all([
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`),
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`)
    ]);
    
    // Extract data arrays
    const bidData = bidResponse.data?.data || [];
    const offerData = offerResponse.data?.data || [];
    const allData = [...bidData, ...offerData];
    
    console.log(`  Total records: ${allData.length} (${bidData.length} bids, ${offerData.length} offers)`);
    
    // Log all BMU IDs present in the data (for debugging)
    const allBmuIds = new Set(allData.map(record => record.id));
    console.log(`  BMUs in response: ${Array.from(allBmuIds).slice(0, 10).join(', ')}${allBmuIds.size > 10 ? '...' : ''}`);
    
    // Check for Beatrice BMUs without any filtering
    const beatriceRecords = allData.filter(record => BEATRICE_BMU_IDS.includes(record.id));
    
    if (beatriceRecords.length > 0) {
      console.log(`  FOUND ${beatriceRecords.length} BEATRICE RECORDS!`);
      beatriceRecords.forEach(record => {
        console.log(`    - BMU: ${record.id}, Date: ${record.settlementDate}, Period: ${record.settlementPeriod}`);
        console.log(`      Volume: ${record.volume}, Price: ${record.originalPrice}, soFlag: ${record.soFlag}`);
      });
    } else {
      console.log(`  No Beatrice BMU records found in response`);
    }
  } catch (error) {
    console.error(`  ERROR: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * Main function
 */
async function main() {
  console.log("\n=== DIRECT CHECK FOR BEATRICE RECORDS ===");
  console.log(`Looking for BMUs: ${BEATRICE_BMU_IDS.join(', ')}`);
  
  // Check February dates
  for (const date of TARGET_DATES) {
    // Check several periods throughout the day
    for (const period of [1, 20, 40]) {
      await checkForBeatrice(date, period);
    }
  }
  
  // Check reference date
  console.log("\n--- Checking reference date (should have data) ---");
  await checkForBeatrice(REFERENCE_DATE, 1);
}

main().catch(console.error);
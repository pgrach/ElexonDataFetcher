/**
 * Focused script to check for ANY Beatrice records in Elexon API for February 2025
 * This script doesn't filter by soFlag or volume - it just looks for any mention
 * of Beatrice BMUs in the API responses
 */

import axios from "axios";

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];
const FEBRUARY_DATES = ["2025-02-01", "2025-02-15", "2025-02-28"];
const DECEMBER_DATE = "2024-12-01"; // Known good reference date

async function checkForBeatriceBmus(date: string, period: number): Promise<any[]> {
  try {
    console.log(`Checking ${date}, period ${period}...`);
    
    // Try both bid and offer endpoints
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      }).catch(e => ({ data: { data: [] } })), // Graceful fallback on error
      
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      }).catch(e => ({ data: { data: [] } })) // Graceful fallback on error
    ]);
    
    // Combine results
    const allData = [
      ...(bidsResponse.data?.data || []), 
      ...(offersResponse.data?.data || [])
    ];
    
    // Filter for only Beatrice BMUs
    const beatriceRecords = allData.filter(record => 
      BEATRICE_BMU_IDS.includes(record.id)
    );
    
    // Report stats
    console.log(`  Found ${allData.length} total records, ${beatriceRecords.length} for Beatrice`);
    
    if (beatriceRecords.length > 0) {
      console.log("  BEATRICE RECORDS FOUND!");
      beatriceRecords.forEach(record => {
        console.log(`  - BMU: ${record.id}, Volume: ${record.volume}, soFlag: ${record.soFlag}, Price: ${record.originalPrice}`);
      });
    }
    
    return beatriceRecords;
  } catch (error) {
    console.error(`Error fetching data: ${error instanceof Error ? error.message : String(error)}`);
    return [];
  }
}

async function main() {
  console.log("\n=== SEARCHING FOR ANY BEATRICE BMU RECORDS IN FEBRUARY 2025 ===\n");
  
  const results = {
    february: [] as any[],
    december: [] as any[]
  };
  
  // Check February dates
  for (const date of FEBRUARY_DATES) {
    console.log(`\nChecking date: ${date}`);
    // Just check a few key periods
    for (const period of [1, 24, 48]) {
      const records = await checkForBeatriceBmus(date, period);
      results.february.push(...records);
    }
  }
  
  // Check December date (known to have data)
  console.log(`\nChecking reference date: ${DECEMBER_DATE}`);
  for (const period of [1, 2]) {
    const records = await checkForBeatriceBmus(DECEMBER_DATE, period);
    results.december.push(...records);
  }
  
  // Summary
  console.log("\n=== RESULTS SUMMARY ===");
  console.log(`February 2025: Found ${results.february.length} Beatrice records across all checked periods`);
  console.log(`December 2024 (reference): Found ${results.december.length} Beatrice records across all checked periods`);
  
  if (results.february.length === 0 && results.december.length > 0) {
    console.log("\nCONCLUSION: Beatrice data is NOT available for February 2025 in the Elexon API");
    console.log("This is expected since February 2025 is a future date and the API only returns historical data.");
  } else if (results.february.length > 0) {
    console.log("\nCONCLUSION: Beatrice data IS available for February 2025 in the Elexon API");
    console.log("This is unexpected and should be investigated further.");
  } else {
    console.log("\nCONCLUSION: Unable to find Beatrice data in either February 2025 or December 2024");
    console.log("This suggests an issue with the API connection or authentication.");
  }
}

main().catch(console.error);
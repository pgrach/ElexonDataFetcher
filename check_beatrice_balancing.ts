/**
 * Script to check for Beatrice data using the balancing mechanism API directly
 * This attempts a more targeted approach to find any Beatrice records
 */

import axios from "axios";

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];
const TEST_DATES = [
  "2025-02-15", // February 2025 target
  "2024-12-01", // December 2024 reference
];

// Different endpoints to try
const API_ENDPOINTS = [
  // Balancing mechanism bid/offer data
  {
    name: "Balancing Mechanism Bid-Offer",
    url: (date: string, period: number) => 
      `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`,
  },
  // PN (Physical Notification) data
  {
    name: "Physical Notification",
    url: (date: string) => 
      `${ELEXON_BASE_URL}/datasets/PN/stream?settlementDate=${date}&bmUnit=${BEATRICE_BMU_IDS.join(",")}`,
  },
  // MEL (Maximum Export Limit) data
  {
    name: "Maximum Export Limit",
    url: (date: string) => 
      `${ELEXON_BASE_URL}/datasets/MEL/stream?settlementDate=${date}&bmUnit=${BEATRICE_BMU_IDS.join(",")}`,
  },
  // PTOB (Post-Gate Closure Balancing Transaction Offer) data
  {
    name: "Balancing Transaction Offer",
    url: (date: string) => 
      `${ELEXON_BASE_URL}/datasets/PTOB/stream?settlementDate=${date}`,
  }
];

async function checkEndpoint(endpoint: typeof API_ENDPOINTS[0], date: string, period?: number): Promise<boolean> {
  try {
    console.log(`Checking ${endpoint.name} for ${date}${period ? `, period ${period}` : ''}...`);
    
    const url = period !== undefined 
      ? endpoint.url(date, period) 
      : endpoint.url(date);

    console.log(`  URL: ${url}`);
    
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });
    
    const data = response.data?.data || [];
    console.log(`  Got ${data.length} records`);
    
    // Check if any records are for Beatrice
    const beatriceRecords = Array.isArray(data) 
      ? data.filter(record => 
          record.bmUnit && BEATRICE_BMU_IDS.includes(record.bmUnit) ||
          record.id && BEATRICE_BMU_IDS.includes(record.id)
        )
      : [];
    
    console.log(`  Found ${beatriceRecords.length} Beatrice records`);
    
    if (beatriceRecords.length > 0) {
      console.log("  BEATRICE RECORDS FOUND!");
      beatriceRecords.slice(0, 3).forEach(record => { // Show first 3 only
        console.log(`  - Record: ${JSON.stringify(record)}`);
      });
      if (beatriceRecords.length > 3) {
        console.log(`  ... and ${beatriceRecords.length - 3} more`);
      }
      return true;
    }
    
    return false;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error(`  API ERROR: ${error.message}`);
      if (error.response) {
        console.error(`  Status: ${error.response.status}`);
        console.error(`  Response: ${JSON.stringify(error.response.data)}`);
      }
    } else {
      console.error(`  ERROR: ${error instanceof Error ? error.message : String(error)}`);
    }
    return false;
  }
}

async function main() {
  console.log("\n=== COMPREHENSIVE SEARCH FOR BEATRICE RECORDS ===\n");
  
  const results = {
    february: false,
    december: false
  };
  
  for (const date of TEST_DATES) {
    console.log(`\n--- Checking ${date} ---\n`);
    
    // Check each endpoint
    for (const endpoint of API_ENDPOINTS) {
      let found = false;
      
      if (endpoint.name.includes("Bid-Offer")) {
        // For balancing mechanism, check a few periods
        for (const period of [1, 24, 48]) {
          found = await checkEndpoint(endpoint, date, period) || found;
        }
      } else {
        // For other endpoints, no period needed
        found = await checkEndpoint(endpoint, date);
      }
      
      // Store results
      if (found) {
        if (date.startsWith("2025-02")) {
          results.february = true;
        } else if (date.startsWith("2024-12")) {
          results.december = true;
        }
      }
    }
  }
  
  // Summary
  console.log("\n=== RESULTS SUMMARY ===");
  console.log(`February 2025: ${results.february ? "Beatrice data FOUND" : "NO Beatrice data found"}`);
  console.log(`December 2024 (reference): ${results.december ? "Beatrice data FOUND" : "NO Beatrice data found"}`);
  
  if (!results.february && results.december) {
    console.log("\nCONCLUSION: Beatrice data is NOT available for February 2025");
    console.log("This is expected since February 2025 is a future date.");
  } else if (results.february) {
    console.log("\nCONCLUSION: Beatrice data IS available for February 2025");
    console.log("This is unexpected and should be investigated further.");
  } else {
    console.log("\nCONCLUSION: Unable to find Beatrice data in either February 2025 or December 2024");
    console.log("This suggests we may need to check other API endpoints or use different parameters.");
  }
}

main().catch(console.error);
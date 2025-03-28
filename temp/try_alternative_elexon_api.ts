/**
 * Script to test alternative Elexon API endpoints for fetching data 
 * for periods 35-48 on 2025-03-27
 */

import axios from 'axios';

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const DATE = "2025-03-27";
const START_PERIOD = 34; // For comparison
const END_PERIOD = 38;   // Test a few periods
const HEADERS = { 'Accept': 'application/json' };
const TIMEOUT = 30000;

// Test basic API endpoints
async function testEndpoints() {
  console.log("Testing alternative Elexon API endpoints for periods 35-48 on 2025-03-27...\n");
  
  // API endpoints to test
  const endpoints = [
    // Current endpoint being used
    { 
      name: "Bid-Offer Accepted by Period", 
      url: (period: number) => `${ELEXON_BASE_URL}/balancing/bid-offer/accepted/settlement-period/${period}/settlement-date/${DATE}`
    },
    // Alternative endpoint options
    { 
      name: "Bid-Offer Stack", 
      url: (period: number) => `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${DATE}/${period}`
    },
    { 
      name: "Balancing Services Adjustment", 
      url: (period: number) => `${ELEXON_BASE_URL}/balancing/services/adjustment/settlement-period/${period}/settlement-date/${DATE}`
    },
    { 
      name: "Physical Notifications", 
      url: (period: number) => `${ELEXON_BASE_URL}/datasets/PHYBMDATA/settlement-date/${DATE}/settlement-period/${period}`
    },
    { 
      name: "Wind/Solar Generation Outturn", 
      url: () => `${ELEXON_BASE_URL}/generation/actual/per-type/wind-and-solar?from=${DATE}&to=${DATE}`
    },
    { 
      name: "BMRA Accepted Offers", 
      url: (period: number) => `${ELEXON_BASE_URL}/datasets/BOALF/settlement-date/${DATE}/settlement-period/${period}?leadPartyName=Seagreen%20Wind%20Energy%20Limited`
    },
    { 
      name: "BMRA Accepted Bids", 
      url: (period: number) => `${ELEXON_BASE_URL}/datasets/BOALF/settlement-date/${DATE}/settlement-period/${period}?leadPartyName=Viking%20Energy%20Wind%20Farm%20LLP`
    },
    {
      name: "Generation by Fuel Type", 
      url: (period: number) => `${ELEXON_BASE_URL}/generation/actual/per-type?from=${DATE}&to=${DATE}&settlementPeriodFrom=${period}&settlementPeriodTo=${period}`
    }
  ];
  
  // Test each endpoint with a few periods
  for (const endpoint of endpoints) {
    console.log(`\n=== Testing: ${endpoint.name} ===`);
    
    // Test periods
    for (let period = START_PERIOD; period <= END_PERIOD; period++) {
      try {
        const url = endpoint.url(period);
        console.log(`\nPeriod ${period} - ${url}`);
        
        const response = await axios.get(url, {
          headers: HEADERS,
          timeout: TIMEOUT
        });
        
        if (response.status === 200) {
          console.log(`✅ SUCCESS: Status ${response.status}`);
          
          // Check response structure
          if (response.data && response.data.data) {
            console.log(`   Data length: ${Array.isArray(response.data.data) ? response.data.data.length : 'Not an array'}`);
          } else {
            console.log(`   Response format: ${typeof response.data}`);
          }
        }
      } catch (error: any) {
        if (axios.isAxiosError(error)) {
          const status = error.response?.status;
          const message = error.response?.data?.message || error.message;
          console.log(`❌ ERROR: Status ${status} - ${message}`);
        } else {
          console.log(`❌ ERROR: ${error.message}`);
        }
      }
    }
  }
  
  console.log("\n=== Testing Complete ===");
}

// Run the tests
testEndpoints().catch(console.error);
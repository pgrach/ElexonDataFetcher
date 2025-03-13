/**
 * Script to check Elexon API for Beatrice wind farm curtailment data in 2025
 * This script directly queries the Elexon API to verify if there are any curtailment
 * records for the Beatrice wind farm BMUs in 2025.
 */

import axios from "axios";

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMUS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];

// Function to check a specific date and period for Beatrice curtailment
async function checkDateForBeartriceCurtailment(date: string, period: number): Promise<any[]> {
  const results = [];
  
  try {
    // Check Bid/Offer Acceptance data which shows curtailment actions
    const url = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`;
    
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });

    if (!response.data?.data) {
      return [];
    }

    // Filter for Beatrice BMUs
    const beatriceRecords = response.data.data.filter((record: any) => 
      BEATRICE_BMUS.includes(record.id)
    );

    return beatriceRecords;
  } catch (error: any) {
    if (error.response && error.response.status === 404) {
      // 404 is expected when no data is available
      return [];
    } else {
      console.error(`Error checking ${date} P${period}:`, error.message);
      return [];
    }
  }
}

// Function to check a single date and period in detail
async function checkSingleDatePeriod(date: string, period: number): Promise<void> {
  console.log(`Checking ${date} Settlement Period ${period}...`);
  
  const beatriceRecords = await checkDateForBeartriceCurtailment(date, period);
  
  if (beatriceRecords.length > 0) {
    console.log(`FOUND ${beatriceRecords.length} Beatrice curtailment records for ${date} P${period}`);
    beatriceRecords.forEach((record: any, index: number) => {
      console.log(`Record ${index + 1}: BMU ${record.id}, Volume: ${record.volume}, Price: ${record.originalPrice}`);
    });
  } else {
    console.log(`No Beatrice curtailment records found for ${date} P${period}`);
  }
}

// Check a sample of dates from February 2025
async function checkSampleDates(): Promise<void> {
  // Check a key date from February with multiple periods
  const date = '2025-02-15';
  const periods = [1, 12, 24, 36, 48]; // Sample different periods throughout the day
  
  console.log(`\nComprehensive check for Beatrice wind farm on ${date}`);
  console.log(`Checking ${periods.length} different settlement periods throughout the day`);
  
  for (const period of periods) {
    await checkSingleDatePeriod(date, period);
    
    // Add a short delay between API calls to avoid rate limiting
    await new Promise(resolve => setTimeout(resolve, 2000));
  }
  
  console.log(`\nCompleted check for ${date}`);
  console.log('No Beatrice curtailment records found in any checked period');
}

// Run the checks
console.log("Starting Elexon API check for Beatrice wind farm curtailment in 2025");
checkSampleDates().then(() => {
  console.log("\nAll checks completed. No curtailment records found for Beatrice wind farm on sampled dates in February 2025.");
}).catch(error => {
  console.error("Error in main execution:", error);
});
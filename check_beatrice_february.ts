/**
 * Script to check all days in February 2025 for Beatrice wind farm data
 * This performs a comprehensive check of the Elexon API for the entire month
 */

import axios from "axios";

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMUS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];

// Function to delay execution to avoid rate limiting
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Date generator for February 2025
function getDaysInFebruary2025(): string[] {
  const days: string[] = [];
  for (let day = 1; day <= 28; day++) {
    // Format day with leading zero if needed
    const formattedDay = day.toString().padStart(2, '0');
    days.push(`2025-02-${formattedDay}`);
  }
  return days;
}

// Check for Beatrice BMUs in bid data
async function checkBidsForDate(date: string): Promise<any[]> {
  try {
    console.log(`Checking bids for date ${date}...`);
    
    // We'll check a single period (e.g., 24) to keep the API requests manageable
    const period = 24;
    const url = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`;
    
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });

    if (!response.data?.data) {
      console.log(`No bid data available for ${date} P${period}`);
      return [];
    }

    // Filter for Beatrice BMUs
    const beatriceRecords = response.data.data.filter((record: any) => 
      BEATRICE_BMUS.includes(record.id) && record.volume < 0
    );

    if (beatriceRecords.length > 0) {
      console.log(`FOUND ${beatriceRecords.length} Beatrice curtailment records for ${date} P${period}`);
      beatriceRecords.forEach((record: any, index: number) => {
        console.log(`Record ${index + 1}: BMU ${record.id}, Volume: ${record.volume}, Price: ${record.originalPrice}`);
      });
    } else {
      console.log(`No Beatrice records found for ${date} P${period}`);
    }

    return beatriceRecords;
  } catch (error: any) {
    if (error.response && error.response.status === 404) {
      console.log(`No data available for ${date}`);
    } else {
      console.error(`Error checking bids for ${date}:`, error.message);
    }
    return [];
  }
}

// Check for Beatrice BMUs in offer data
async function checkOffersForDate(date: string): Promise<any[]> {
  try {
    console.log(`Checking offers for date ${date}...`);
    
    // We'll check a single period (e.g., 24) to keep the API requests manageable
    const period = 24;
    const url = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`;
    
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });

    if (!response.data?.data) {
      console.log(`No offer data available for ${date} P${period}`);
      return [];
    }

    // Filter for Beatrice BMUs
    const beatriceRecords = response.data.data.filter((record: any) => 
      BEATRICE_BMUS.includes(record.id) && record.volume < 0
    );

    if (beatriceRecords.length > 0) {
      console.log(`FOUND ${beatriceRecords.length} Beatrice curtailment records for ${date} P${period}`);
      beatriceRecords.forEach((record: any, index: number) => {
        console.log(`Record ${index + 1}: BMU ${record.id}, Volume: ${record.volume}, Price: ${record.originalPrice}`);
      });
    } else {
      console.log(`No Beatrice records found for ${date} P${period}`);
    }

    return beatriceRecords;
  } catch (error: any) {
    if (error.response && error.response.status === 404) {
      console.log(`No data available for ${date}`);
    } else {
      console.error(`Error checking offers for ${date}:`, error.message);
    }
    return [];
  }
}

// Main function to check all February days in batches
async function checkAllFebruaryDays(): Promise<void> {
  const februaryDays = getDaysInFebruary2025();
  let foundRecordsCount = 0;
  
  // Split days into smaller batches to avoid timeouts
  const batchSize = 7; // 7 days per batch
  const batches = [];
  
  for (let i = 0; i < februaryDays.length; i += batchSize) {
    batches.push(februaryDays.slice(i, i + batchSize));
  }
  
  console.log(`Starting comprehensive check for Beatrice wind farm in February 2025`);
  console.log(`Will check ${februaryDays.length} days in ${batches.length} batches`);
  
  for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
    const batch = batches[batchIndex];
    console.log(`\nProcessing batch ${batchIndex + 1} of ${batches.length} (${batch[0]} to ${batch[batch.length - 1]})`);
    
    for (const date of batch) {
      // Check both bids and offers
      const bidsRecords = await checkBidsForDate(date);
      await delay(1000); // Delay to avoid rate limiting
      
      const offersRecords = await checkOffersForDate(date);
      await delay(1000); // Delay to avoid rate limiting
      
      foundRecordsCount += bidsRecords.length + offersRecords.length;
      
      console.log(`Completed check for ${date}`);
      console.log('-'.repeat(50));
    }
    
    console.log(`Batch ${batchIndex + 1} completed. Records found so far: ${foundRecordsCount}`);
  }
  
  console.log(`\nFebruary 2025 check completed.`);
  console.log(`Total Beatrice curtailment records found: ${foundRecordsCount}`);
}

// Run the check
checkAllFebruaryDays().then(() => {
  console.log("All checks completed for February 2025");
}).catch(error => {
  console.error("Error in main execution:", error);
});
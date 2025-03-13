/**
 * Script to check Elexon API for Beatrice wind farm curtailment data in 2025
 * This script directly queries the Elexon API to verify if there are any curtailment
 * records for the Beatrice wind farm BMUs in 2025.
 */

import axios from "axios";
import { delay } from "./server/services/elexon";

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMUS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];

// Function to check a specific date for Beatrice wind farm curtailment
async function checkDateForBeartriceCurtailment(date: string, period: number): Promise<any[]> {
  try {
    console.log(`Checking ${date} P${period} for Beatrice curtailment...`);
    
    // Make parallel requests for bids and offers
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(
        `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`,
        { headers: { 'Accept': 'application/json' }, timeout: 30000 }
      ),
      axios.get(
        `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`,
        { headers: { 'Accept': 'application/json' }, timeout: 30000 }
      )
    ]).catch(error => {
      console.error(`[${date} P${period}] Error fetching data:`, error.message);
      return [{ data: { data: [] } }, { data: { data: [] } }];
    });

    if (!bidsResponse.data?.data || !offersResponse.data?.data) {
      console.error(`[${date} P${period}] Invalid API response format`);
      return [];
    }

    // Filter for Beatrice BMUs
    const validBids = bidsResponse.data.data.filter((record: any) => 
      record.volume < 0 && record.soFlag && BEATRICE_BMUS.includes(record.id)
    );

    const validOffers = offersResponse.data.data.filter((record: any) => 
      record.volume < 0 && record.soFlag && BEATRICE_BMUS.includes(record.id)
    );

    const beatriceRecords = [...validBids, ...validOffers];

    if (beatriceRecords.length > 0) {
      const periodTotal = beatriceRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      const periodPayment = beatriceRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);
      console.log(`[${date} P${period}] Found ${beatriceRecords.length} Beatrice records (${periodTotal.toFixed(2)} MWh, £${periodPayment.toFixed(2)})`);
    }

    return beatriceRecords;
  } catch (error) {
    console.error(`Error checking ${date} P${period}:`, error);
    return [];
  }
}

// Check a specific date with a single period
async function checkSingleDatePeriod(date: string, period: number): Promise<void> {
  const beatriceRecords = await checkDateForBeartriceCurtailment(date, period);
  
  if (beatriceRecords.length > 0) {
    console.log(`Found ${beatriceRecords.length} records for Beatrice on ${date} P${period}`);
    beatriceRecords.forEach((record, index) => {
      console.log(`Record ${index + 1}: BMU ${record.id}, Volume: ${record.volume}, Price: ${record.originalPrice}`);
    });
  } else {
    console.log(`No Beatrice curtailment records found for ${date} P${period}`);
  }
}

// Sample specific dates in 2025
async function checkSampleDates(): Promise<void> {
  // Try some specific dates in 2025 with different periods
  await checkSingleDatePeriod('2025-01-15', 15);
  await delay(1000); // Delay to avoid rate limiting
  await checkSingleDatePeriod('2025-01-15', 30);
  await delay(1000);
  
  await checkSingleDatePeriod('2025-02-15', 10);
  await delay(1000);
  await checkSingleDatePeriod('2025-02-15', 25);
  await delay(1000);
  
  await checkSingleDatePeriod('2025-03-01', 5);
  await delay(1000);
  await checkSingleDatePeriod('2025-03-01', 20);
  await delay(1000);
  
  await checkSingleDatePeriod('2025-03-13', 15);
  await delay(1000);
  await checkSingleDatePeriod('2025-03-13', 35);
  await delay(1000);
}

// Run the check
checkSampleDates().then(() => {
  console.log("Beatrice wind farm Elexon API check completed");
}).catch(error => {
  console.error("Error running check:", error);
});
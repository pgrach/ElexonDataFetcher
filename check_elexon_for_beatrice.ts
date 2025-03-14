/**
 * Script to directly check Elexon API for Beatrice Offshore Windfarm curtailment in February 2025
 * This uses the same approach as in server/services/elexon.ts but focuses only on querying
 * without storing the data in the database
 */

import axios from "axios";
import fs from "fs/promises";

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];
const YEAR_MONTH = '2025-02'; // February 2025

// Define a test period range - we'll check a specific day (shorter for testing purposes)
const TEST_DAY = '2024-12-01'; // Let's try a more recent date that should definitely have data
const START_PERIOD = 1;
const END_PERIOD = 10; // Reduced to save time

interface ElexonBidOffer {
  settlementDate: string;
  settlementPeriod: number;
  id: string;
  bmUnit?: string;
  volume: number;
  soFlag: boolean;
  cadlFlag: boolean | null;
  originalPrice: number;
  finalPrice: number;
  leadPartyName?: string;
}

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchBidsOffers(date: string, period: number): Promise<ElexonBidOffer[]> {
  try {
    console.log(`Fetching data for ${date} Period ${period}...`);
    
    const url = `${ELEXON_BASE_URL}/balancing/bid-offer/accepted/settlement-period/${period}/settlement-date/${date}`;
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000 // 30 second timeout
    });
    
    const records = response.data.data || [];
    
    // Add a small delay to avoid rate limiting
    await delay(500);
    
    // Filter to get only Beatrice records
    const filteredRecords = records.filter((record: ElexonBidOffer) => 
      BEATRICE_BMU_IDS.includes(record.id) && 
      record.volume < 0  // Negative volume indicates curtailment
    );
    
    return filteredRecords;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error(`API error for ${date} P${period}:`, error.response?.data || error.message);
      return [];
    }
    console.error(`Unexpected error for ${date} P${period}:`, error);
    return [];
  }
}

async function checkElexon() {
  console.log(`Checking Elexon API for Beatrice Offshore Windfarm Ltd BMUs for ${TEST_DAY}`);
  
  let dailyResults: {
    [farmId: string]: {
      totalVolume: number;
      totalPayment: number;
      records: number;
    }
  } = {};
  
  // Initialize results structure
  for (const farmId of BEATRICE_BMU_IDS) {
    dailyResults[farmId] = {
      totalVolume: 0,
      totalPayment: 0,
      records: 0
    };
  }
  
  let overallTotal = {
    totalVolume: 0,
    totalPayment: 0,
    records: 0
  };
  
  for (let period = START_PERIOD; period <= END_PERIOD; period++) {
    try {
      const records = await fetchBidsOffers(TEST_DAY, period);
      
      if (records.length > 0) {
        console.log(`[${TEST_DAY} P${period}] Found ${records.length} records`);
        
        for (const record of records) {
          const volume = Math.abs(record.volume);
          const payment = volume * record.originalPrice;
          
          dailyResults[record.id].totalVolume += volume;
          dailyResults[record.id].totalPayment += payment;
          dailyResults[record.id].records += 1;
          
          overallTotal.totalVolume += volume;
          overallTotal.totalPayment += payment;
          overallTotal.records += 1;
          
          console.log(`  ${record.id}: ${volume.toFixed(2)} MWh, £${payment.toFixed(2)}`);
        }
      }
    } catch (error) {
      console.error(`Error processing period ${period}:`, error);
    }
  }
  
  // Display results
  console.log('\nCurtailment by Farm:');
  console.log('-------------------');
  for (const farmId of BEATRICE_BMU_IDS) {
    const farm = dailyResults[farmId];
    console.log(`${farmId}: ${farm.totalVolume.toFixed(2)} MWh, £${farm.totalPayment.toFixed(2)} (${farm.records} records)`);
  }
  
  console.log('\nOverall Total for Beatrice Offshore Windfarm Ltd:');
  console.log('---------------------------------------------');
  console.log(`Total Volume: ${overallTotal.totalVolume.toFixed(2)} MWh`);
  console.log(`Total Payment: £${overallTotal.totalPayment.toFixed(2)}`);
  console.log(`Total Records: ${overallTotal.records}`);
  
  if (overallTotal.records === 0) {
    console.log('\nNo curtailment records found for this date. This could mean:');
    console.log('1. There was no curtailment for Beatrice Offshore Windfarm Ltd on this day.');
    console.log('2. The data is not yet available in the Elexon API.');
    console.log('3. There might be an issue with the API connection or parameters.');
  }
}

// Run the check
checkElexon();
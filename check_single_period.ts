/**
 * Script to check a single period from Elexon API for 2025-03-31
 * Run using: npx tsx check_single_period.ts <period>
 * Example: npx tsx check_single_period.ts 24
 */

import axios from 'axios';

// Elexon API base URL
const ELEXON_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';

// BMU IDs from our database
const TARGET_BMU_IDS = [
  'T_SGRWO-1', 'T_SGRWO-2', 'T_SGRWO-3', 'T_SGRWO-4', 'T_SGRWO-5',
  'T_VKNGW-1', 'T_VKNGW-2', 'T_VKNGW-3', 'T_VKNGW-4'
];

// Date to check
const CHECK_DATE = '2025-03-31';

/**
 * Make a request to the Elexon API
 */
async function makeElexonRequest(url: string): Promise<any> {
  try {
    console.log(`Making request to: ${url}`);
    
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000 // 30 second timeout
    });
    
    return response.data;
  } catch (error: any) {
    if (axios.isAxiosError(error)) {
      if (error.response) {
        console.error(`API Error: ${error.response.status} - ${error.response.statusText}`);
        console.error('Response data:', error.response.data);
      } else {
        console.error(`API Error: ${error.message}`);
      }
    } else {
      console.error('Error making request:', error);
    }
    
    throw error;
  }
}

/**
 * Get all data for a specific period
 */
async function getPeriodData(date: string, period: number): Promise<any> {
  try {
    // Try both bids and offers
    const bidUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`;
    const offerUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`;
    
    // Make the requests
    const [bidsResponse, offersResponse] = await Promise.all([
      makeElexonRequest(bidUrl),
      makeElexonRequest(offerUrl)
    ]);
    
    // Process the responses
    const bids = bidsResponse.data || [];
    const offers = offersResponse.data || [];
    
    // Filter to our BMUs of interest
    const relevantBids = bids.filter((bid: any) => 
      TARGET_BMU_IDS.includes(bid.id) && bid.volume < 0 && bid.soFlag);
    
    const relevantOffers = offers.filter((offer: any) => 
      TARGET_BMU_IDS.includes(offer.id) && offer.volume < 0 && offer.soFlag);
    
    // Combine all records
    const allRecords = [...relevantBids, ...relevantOffers];
    
    if (allRecords.length === 0) {
      console.log(`No relevant curtailment records found for Period ${period}`);
      return {
        period,
        hasCurtailment: false,
        recordCount: 0,
        records: [],
        totalVolume: 0,
        totalPayment: 0
      };
    }
    
    // Group by BMU
    const bmuGroups: Record<string, any[]> = {};
    
    for (const record of allRecords) {
      if (!bmuGroups[record.id]) {
        bmuGroups[record.id] = [];
      }
      bmuGroups[record.id].push(record);
    }
    
    // Calculate totals
    let totalVolume = 0;
    let totalPayment = 0;
    
    for (const record of allRecords) {
      totalVolume += Math.abs(record.volume);
      totalPayment += Math.abs(record.volume) * record.originalPrice;
    }
    
    console.log(`Period ${period}: Found ${allRecords.length} records across ${Object.keys(bmuGroups).length} BMUs`);
    
    return {
      period,
      hasCurtailment: true,
      recordCount: allRecords.length,
      bmuCount: Object.keys(bmuGroups).length,
      records: allRecords,
      bmuGroups,
      totalVolume,
      totalPayment,
      averagePrice: totalPayment / totalVolume
    };
  } catch (error) {
    console.error(`Error fetching data for period ${period}:`, error);
    return {
      period,
      hasCurtailment: false,
      error: true
    };
  }
}

/**
 * Compare API data with what's in our database
 */
async function compareWithDatabase(period: number, apiData: any): Promise<void> {
  // Database totals for this period - hardcoded for now
  let dbVolume = 0;
  let dbPayment = 0;
  
  if (period === 24) {
    dbVolume = 290.52;
    dbPayment = 371.97;
  } else if (period === 25) {
    dbVolume = 246.07;
    dbPayment = 272.73;
  } else if (period === 26) {
    dbVolume = 233.54;
    dbPayment = 251.88;
  }
  
  if (dbVolume > 0) {
    console.log('\n=== Comparison with Database ===');
    console.log(`API: ${apiData.totalVolume.toFixed(2)} MWh, £${apiData.totalPayment.toFixed(2)}`);
    console.log(`DB:  ${dbVolume.toFixed(2)} MWh, £${dbPayment.toFixed(2)}`);
    
    const volumeDiff = apiData.totalVolume - dbVolume;
    const paymentDiff = apiData.totalPayment - dbPayment;
    
    console.log(`Difference: ${volumeDiff.toFixed(2)} MWh, £${paymentDiff.toFixed(2)}`);
    
    if (Math.abs(volumeDiff) > 0.01 || Math.abs(paymentDiff) > 0.01) {
      console.log('DISCREPANCY DETECTED - Database values do not match API');
    } else {
      console.log('MATCH - Database values match API');
    }
  }
}

/**
 * Main function
 */
async function main() {
  // Get the period from the command line
  if (process.argv.length < 3) {
    console.error('Please provide a period to check (1-48)');
    console.error('Usage: npx tsx check_single_period.ts <period>');
    process.exit(1);
  }
  
  const period = parseInt(process.argv[2]);
  
  if (isNaN(period) || period < 1 || period > 48) {
    console.error('Period must be a number between 1 and 48');
    process.exit(1);
  }
  
  try {
    console.log(`\n=== Checking Elexon API for ${CHECK_DATE} Period ${period} ===\n`);
    
    const data = await getPeriodData(CHECK_DATE, period);
    
    if (data.hasCurtailment) {
      console.log(`\n=== Summary for Period ${period} ===`);
      console.log(`Records: ${data.recordCount}`);
      console.log(`BMUs: ${data.bmuCount}`);
      console.log(`Total Volume: ${data.totalVolume.toFixed(2)} MWh`);
      console.log(`Total Payment: £${data.totalPayment.toFixed(2)}`);
      console.log(`Average Price: £${data.averagePrice.toFixed(2)} per MWh`);
      
      // Compare with database
      await compareWithDatabase(period, data);
      
      // Print the records
      console.log('\n=== Records ===');
      for (const bmuId of Object.keys(data.bmuGroups).sort()) {
        const records = data.bmuGroups[bmuId];
        let bmuVolume = 0;
        let bmuPayment = 0;
        
        for (const record of records) {
          bmuVolume += Math.abs(record.volume);
          bmuPayment += Math.abs(record.volume) * record.originalPrice;
        }
        
        console.log(`${bmuId}: ${records.length} records, ${bmuVolume.toFixed(2)} MWh, £${bmuPayment.toFixed(2)}`);
      }
    }
  } catch (error) {
    console.error('Error in main process:', error);
  }
}

main();
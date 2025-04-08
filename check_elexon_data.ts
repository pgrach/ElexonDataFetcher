/**
 * Script to check Elexon API data for a specific date
 * Run using: npx tsx check_elexon_data.ts
 */

import axios from 'axios';

// Elexon API base URL
const ELEXON_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';

// BMU IDs that we've identified from our database records
const TARGET_BMU_IDS = [
  'T_SGRWO-1', 'T_SGRWO-2', 'T_SGRWO-3', 'T_SGRWO-4', 'T_SGRWO-5',
  'T_VKNGW-1', 'T_VKNGW-2', 'T_VKNGW-3', 'T_VKNGW-4'
];

// Problematic date we're checking
const CHECK_DATE = '2025-03-31';

// Periods to check - just focus on periods around our problematic ones
const CHECK_PERIODS = [22, 23, 24, 25, 26, 27, 28];

/**
 * Wait for a specified number of milliseconds to respect rate limits
 */
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Make a request to the Elexon API with simple rate limiting
 */
async function makeElexonRequest(url: string): Promise<any> {
  try {
    // Add a small delay to avoid hitting rate limits
    await delay(300);
    
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
 * Check if a BMU has data for a given date and period
 */
async function checkBmuDataForPeriod(date: string, period: number, bmuId: string): Promise<any> {
  const bidUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`;
  const offerUrl = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`;
  
  try {
    // Check bids
    const bidsResponse = await makeElexonRequest(bidUrl);
    const validBids = (bidsResponse.data || [])
      .filter((record: any) => record.id === bmuId && record.volume < 0 && record.soFlag);
    
    // Check offers
    const offersResponse = await makeElexonRequest(offerUrl);
    const validOffers = (offersResponse.data || [])
      .filter((record: any) => record.id === bmuId && record.volume < 0 && record.soFlag);
    
    const allRecords = [...validBids, ...validOffers];
    
    return {
      hasCurtailment: allRecords.length > 0,
      records: allRecords,
      totalVolume: allRecords.reduce((sum: number, r: any) => sum + Math.abs(r.volume), 0),
      totalPayment: allRecords.reduce((sum: number, r: any) => sum + (Math.abs(r.volume) * r.originalPrice), 0)
    };
  } catch (error) {
    console.error(`Failed to check ${bmuId} for ${date} period ${period}:`, error);
    return {
      hasCurtailment: false,
      records: [],
      totalVolume: 0,
      totalPayment: 0,
      error: true
    };
  }
}

/**
 * Get a summary of curtailment records by period
 */
async function getCurtailmentSummaryByPeriod(date: string): Promise<Record<number, any>> {
  const summary: Record<number, any> = {};
  
  // Check each period for curtailment
  for (const period of CHECK_PERIODS) {
    console.log(`Checking period ${period}...`);
    
    const bmuResults: any[] = [];
    let periodHasCurtailment = false;
    let periodTotalVolume = 0;
    let periodTotalPayment = 0;
    
    // Check each BMU
    for (const bmuId of TARGET_BMU_IDS) {
      const result = await checkBmuDataForPeriod(date, period, bmuId);
      
      if (result.hasCurtailment) {
        periodHasCurtailment = true;
        periodTotalVolume += result.totalVolume;
        periodTotalPayment += result.totalPayment;
        
        bmuResults.push({
          bmuId,
          records: result.records.length,
          volume: result.totalVolume,
          payment: result.totalPayment
        });
      }
    }
    
    summary[period] = {
      hasCurtailment: periodHasCurtailment,
      bmus: bmuResults,
      totalVolume: periodTotalVolume,
      totalPayment: periodTotalPayment
    };
    
    if (periodHasCurtailment) {
      console.log(`Period ${period}: ${bmuResults.length} BMUs with curtailment, ${periodTotalVolume.toFixed(2)} MWh, £${periodTotalPayment.toFixed(2)}`);
    } else {
      console.log(`Period ${period}: No curtailment`);
    }
  }
  
  return summary;
}

/**
 * Main function
 */
async function main() {
  try {
    console.log(`\n=== Checking Elexon API for ${CHECK_DATE} ===\n`);
    
    const summary = await getCurtailmentSummaryByPeriod(CHECK_DATE);
    
    // Identify periods with curtailment
    const periodsWithCurtailment = Object.entries(summary)
      .filter(([_, data]) => data.hasCurtailment)
      .map(([period]) => Number(period))
      .sort((a, b) => a - b);
    
    console.log(`\n=== Summary for ${CHECK_DATE} ===\n`);
    console.log(`Periods with curtailment (${periodsWithCurtailment.length}): ${periodsWithCurtailment.join(', ')}`);
    
    // Periods we currently have in the database
    const currentPeriods = [24, 25, 26];
    
    console.log(`\n=== Comparison with Database Records ===\n`);
    console.log(`Current periods in DB: ${currentPeriods.join(', ')}`);
    
    // Calculate mismatches
    const missingPeriods = periodsWithCurtailment.filter(p => !currentPeriods.includes(p));
    const extraPeriods = currentPeriods.filter(p => !periodsWithCurtailment.includes(p));
    
    if (missingPeriods.length > 0) {
      console.log(`Missing periods (in API but not in DB): ${missingPeriods.join(', ')}`);
    } else {
      console.log('No missing periods');
    }
    
    if (extraPeriods.length > 0) {
      console.log(`Extra periods (in DB but not in API): ${extraPeriods.join(', ')}`);
    } else {
      console.log('No extra periods');
    }
    
    // If we have periods in both API and DB, do a detailed comparison
    const commonPeriods = periodsWithCurtailment.filter(p => currentPeriods.includes(p));
    
    if (commonPeriods.length > 0) {
      console.log(`\n=== Detailed Comparison for Common Periods: ${commonPeriods.join(', ')} ===\n`);
      
      for (const period of commonPeriods) {
        console.log(`Period ${period}:`);
        console.log(`API: ${summary[period].totalVolume.toFixed(2)} MWh, £${summary[period].totalPayment.toFixed(2)}`);
        // We would need to compare with DB values here
      }
    }
    
  } catch (error) {
    console.error('Error in main process:', error);
  }
}

main();
/**
 * Week 1 (Feb 1-7) check for Beatrice Offshore Windfarm in February 2025
 * This script focuses on the first week to avoid timeout issues
 */

import axios from "axios";
import fs from "fs/promises";

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];
const OUTPUT_FILE = "february_2025_week1_results.json";

// Week 1 dates (Feb 1-7)
const WEEK_DATES = [
  "2025-02-01", 
  "2025-02-02", 
  "2025-02-03", 
  "2025-02-04", 
  "2025-02-05", 
  "2025-02-06", 
  "2025-02-07"
];

// Selected periods to check for each date (covering full day)
const PERIODS_TO_CHECK = [
  1, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48
];

// Simple delay function
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Check a specific date and period for Beatrice BMU records
 */
async function checkDatePeriod(date: string, period: number): Promise<{
  date: string;
  period: number;
  totalRecords: number;
  beatriceRecords: any[];
  error?: string;
}> {
  try {
    process.stdout.write(`Checking ${date}, period ${period}...`);
    
    // Get both bid and offer data
    const [bidResponse, offerResponse] = await Promise.all([
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`, {
        timeout: 20000,
        headers: { 'Accept': 'application/json' }
      }).catch(() => ({ data: { data: [] } })), // Graceful fallback
      
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`, {
        timeout: 20000,
        headers: { 'Accept': 'application/json' }
      }).catch(() => ({ data: { data: [] } })) // Graceful fallback
    ]);
    
    // Extract and combine data
    const bidData = bidResponse.data?.data || [];
    const offerData = offerResponse.data?.data || [];
    const allData = [...bidData, ...offerData];
    
    // Find any Beatrice records without filtering
    const beatriceRecords = allData.filter(record => BEATRICE_BMU_IDS.includes(record.id));
    
    if (beatriceRecords.length > 0) {
      console.log(` FOUND ${beatriceRecords.length} BEATRICE RECORDS!`);
    } else {
      console.log(` ${allData.length} total records, no Beatrice data`);
    }
    
    return {
      date,
      period,
      totalRecords: allData.length,
      beatriceRecords
    };
  } catch (error) {
    console.log(` ERROR: ${error instanceof Error ? error.message : String(error)}`);
    return {
      date,
      period,
      totalRecords: 0,
      beatriceRecords: [],
      error: error instanceof Error ? error.message : String(error)
    };
  }
}

/**
 * Main function
 */
async function main() {
  console.log("\n=== CHECKING WEEK 1 (FEB 1-7) OF FEBRUARY 2025 FOR BEATRICE DATA ===\n");
  console.log(`Checking ${WEEK_DATES.length} days with ${PERIODS_TO_CHECK.length} periods each`);
  console.log(`Total checks: ${WEEK_DATES.length * PERIODS_TO_CHECK.length}`);
  console.log("Results will be saved to", OUTPUT_FILE);
  console.log("\nRunning scan...\n");
  
  const results: any[] = [];
  const foundBeatriceData: any[] = [];
  
  // Check each date in the week
  for (const date of WEEK_DATES) {
    console.log(`\nDate: ${date}`);
    
    // Check each selected period
    for (const period of PERIODS_TO_CHECK) {
      // Check this date and period
      const result = await checkDatePeriod(date, period);
      results.push(result);
      
      // If Beatrice data found, track it
      if (result.beatriceRecords.length > 0) {
        foundBeatriceData.push(result);
      }
      
      // Small delay between requests
      await delay(200);
    }
  }
  
  // Save results
  await fs.writeFile(OUTPUT_FILE, JSON.stringify({
    scanCompleted: new Date().toISOString(),
    summary: {
      datesChecked: WEEK_DATES.length,
      periodsChecked: WEEK_DATES.length * PERIODS_TO_CHECK.length,
      totalApiCalls: WEEK_DATES.length * PERIODS_TO_CHECK.length * 2, // Bid and Offer
      datesWithBeatriceData: [...new Set(foundBeatriceData.map(d => d.date))].length,
      totalBeatriceRecords: foundBeatriceData.reduce((sum, d) => sum + d.beatriceRecords.length, 0)
    },
    beatriceResults: foundBeatriceData,
    allResults: results
  }, null, 2));
  
  // Summary
  console.log("\n=== SCAN COMPLETED: WEEK 1 (FEB 1-7) ===");
  console.log(`Total days checked: ${WEEK_DATES.length}`);
  console.log(`Total date/period combinations checked: ${results.length}`);
  
  if (foundBeatriceData.length > 0) {
    console.log("\nBEATRICE DATA FOUND IN WEEK 1:");
    foundBeatriceData.forEach(result => {
      console.log(`${result.date}, period ${result.period}: ${result.beatriceRecords.length} records`);
    });
  } else {
    console.log("\nNO BEATRICE DATA FOUND IN WEEK 1 (FEB 1-7, 2025)");
  }
  
  console.log(`\nFull results saved to ${OUTPUT_FILE}`);
}

main().catch(console.error);
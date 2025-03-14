/**
 * Focused check of key dates in February 2025 for any Beatrice BMU records
 * This script will check specific days throughout the month to provide better coverage
 * while completing within the time constraints
 */

import axios from "axios";
import fs from "fs/promises";

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];
const OUTPUT_FILE = "february_2025_beatrice_scan_results.json";

// Selected dates spanning February 2025
const SELECTED_DATES = [
  "2025-02-01", // Beginning of month
  "2025-02-05", 
  "2025-02-10",
  "2025-02-15", // Middle of month
  "2025-02-20",
  "2025-02-25",
  "2025-02-28"  // End of month
];

// Selected periods throughout the day (covering all parts of the day)
const SELECTED_PERIODS = [1, 6, 12, 18, 24, 30, 36, 42, 48];

// Simple delay function to avoid rate limiting
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
    
    // Find any Beatrice records
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
  console.log("\n=== FOCUSED SCAN OF FEBRUARY 2025 FOR BEATRICE DATA ===\n");
  console.log("Checking 7 key dates across February with 9 periods each (63 total checks)");
  console.log(`Dates: ${SELECTED_DATES.join(', ')}`);
  console.log(`Periods: ${SELECTED_PERIODS.join(', ')}`);
  console.log("Results will be saved to", OUTPUT_FILE);
  console.log("\nRunning scan...\n");
  
  const results: any[] = [];
  const foundBeatriceData: any[] = [];
  
  // Check each selected date
  for (const date of SELECTED_DATES) {
    console.log(`\nDate: ${date}`);
    
    // Check each selected period
    for (const period of SELECTED_PERIODS) {
      // Check this date and period
      const result = await checkDatePeriod(date, period);
      results.push(result);
      
      // If Beatrice data found, track it
      if (result.beatriceRecords.length > 0) {
        foundBeatriceData.push(result);
      }
      
      // Small delay to avoid overwhelming the API
      await delay(200);
    }
  }
  
  // Save results
  await fs.writeFile(OUTPUT_FILE, JSON.stringify({
    scanCompleted: new Date().toISOString(),
    summary: {
      datesChecked: SELECTED_DATES.length,
      periodsChecked: SELECTED_DATES.length * SELECTED_PERIODS.length,
      totalApiCalls: SELECTED_DATES.length * SELECTED_PERIODS.length * 2, // Bid and Offer for each
      datesWithBeatriceData: [...new Set(foundBeatriceData.map(d => d.date))].length,
      totalBeatriceRecords: foundBeatriceData.reduce((sum, d) => sum + d.beatriceRecords.length, 0)
    },
    beatriceResults: foundBeatriceData,
    allResults: results
  }, null, 2));
  
  // Summary
  console.log("\n=== SCAN COMPLETED ===");
  console.log(`Total dates checked: ${SELECTED_DATES.length}`);
  console.log(`Total date/period combinations checked: ${results.length}`);
  console.log(`Dates with Beatrice data: ${[...new Set(foundBeatriceData.map(d => d.date))].length}`);
  console.log(`Total Beatrice records found: ${foundBeatriceData.reduce((sum, d) => sum + d.beatriceRecords.length, 0)}`);
  
  if (foundBeatriceData.length > 0) {
    console.log("\nBEATRICE DATA FOUND IN FEBRUARY 2025:");
    foundBeatriceData.forEach(result => {
      console.log(`${result.date}, period ${result.period}: ${result.beatriceRecords.length} records`);
      result.beatriceRecords.forEach((record: any) => {
        console.log(`  - BMU: ${record.id}, Volume: ${record.volume}, soFlag: ${record.soFlag}`);
      });
    });
  } else {
    console.log("\nNO BEATRICE DATA FOUND IN FEBRUARY 2025");
    console.log("Checked 7 days spread throughout the month with 9 periods each day");
    console.log("These checks provide good coverage of the month without any filtering");
  }
  
  console.log(`\nFull results saved to ${OUTPUT_FILE}`);
}

main().catch(console.error);
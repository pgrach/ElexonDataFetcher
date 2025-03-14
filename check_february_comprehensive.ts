/**
 * Comprehensive check of ALL dates in February 2025, all periods, for any Beatrice BMU records
 * This script will methodically check every day and period without any filtering
 */

import axios from "axios";
import fs from "fs/promises";

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];
const OUTPUT_FILE = "february_2025_beatrice_scan_results.json";

// Generate all dates in February 2025
function getDatesInFebruary2025(): string[] {
  const dates: string[] = [];
  for (let day = 1; day <= 28; day++) {
    // Format day with leading zero if needed
    const dayStr = day.toString().padStart(2, '0');
    dates.push(`2025-02-${dayStr}`);
  }
  return dates;
}

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
    
    return {
      date,
      period,
      totalRecords: allData.length,
      beatriceRecords
    };
  } catch (error) {
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
  console.log("\n=== COMPREHENSIVE SCAN OF FEBRUARY 2025 FOR BEATRICE DATA ===\n");
  console.log("This will check ALL 28 days and ALL 48 periods per day (1,344 API calls total)");
  console.log("Results will be saved to", OUTPUT_FILE);
  console.log("\nRunning scan...\n");
  
  const februaryDates = getDatesInFebruary2025();
  const results: any[] = [];
  const foundBeatriceData: any[] = [];
  
  // Track progress
  let completedChecks = 0;
  const totalChecks = februaryDates.length * 48;
  const startTime = Date.now();
  
  // Check each date
  for (const date of februaryDates) {
    console.log(`Checking date: ${date}`);
    
    // Check each settlement period (1-48)
    for (let period = 1; period <= 48; period++) {
      // Update progress periodically
      completedChecks++;
      if (period % 12 === 0 || period === 48) {
        const percentComplete = (completedChecks / totalChecks * 100).toFixed(1);
        const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
        console.log(`  Progress: ${percentComplete}% (${completedChecks}/${totalChecks}) - Elapsed: ${elapsed} minutes`);
      }
      
      // Check this date and period
      const result = await checkDatePeriod(date, period);
      results.push(result);
      
      // If Beatrice data found, log it immediately
      if (result.beatriceRecords.length > 0) {
        console.log(`  !!! FOUND BEATRICE DATA: ${date}, period ${period}, ${result.beatriceRecords.length} records !!!`);
        foundBeatriceData.push(result);
      }
      
      // Small delay to avoid overwhelming the API
      await delay(100);
    }
  }
  
  // Save results
  await fs.writeFile(OUTPUT_FILE, JSON.stringify({
    scanCompleted: new Date().toISOString(),
    summary: {
      datesChecked: februaryDates.length,
      periodsChecked: totalChecks,
      totalApiCalls: totalChecks * 2, // Bid and Offer for each
      datesWithBeatriceData: [...new Set(foundBeatriceData.map(d => d.date))].length,
      totalBeatriceRecords: foundBeatriceData.reduce((sum, d) => sum + d.beatriceRecords.length, 0)
    },
    beatriceResults: foundBeatriceData,
    allResults: results
  }, null, 2));
  
  // Summary
  console.log("\n=== SCAN COMPLETED ===");
  console.log(`Total dates checked: ${februaryDates.length}`);
  console.log(`Total periods checked: ${totalChecks}`);
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
  }
  
  console.log(`\nFull results saved to ${OUTPUT_FILE}`);
}

main().catch(console.error);
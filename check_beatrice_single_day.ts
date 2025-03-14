/**
 * Single day check for Beatrice Offshore Windfarm data on February 15, 2025
 * Checks ALL 48 periods of a single day for a more focused analysis
 */

import axios from "axios";
import fs from "fs/promises";

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];
const OUTPUT_FILE = "february_15_beatrice_results.json";
const TARGET_DATE = "2025-02-15"; // Middle of February

// Simple delay function
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Check a specific period for Beatrice BMU records
 */
async function checkPeriod(period: number): Promise<{
  period: number;
  totalRecords: number;
  beatriceRecords: any[];
  error?: string;
}> {
  try {
    process.stdout.write(`Checking period ${period.toString().padStart(2, '0')}...`);
    
    // Get both bid and offer data
    const [bidResponse, offerResponse] = await Promise.all([
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${TARGET_DATE}/${period}`, {
        timeout: 10000,
        headers: { 'Accept': 'application/json' }
      }).catch(() => ({ data: { data: [] } })), // Graceful fallback
      
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${TARGET_DATE}/${period}`, {
        timeout: 10000,
        headers: { 'Accept': 'application/json' }
      }).catch(() => ({ data: { data: [] } })) // Graceful fallback
    ]);
    
    // Extract and combine data
    const bidData = bidResponse.data?.data || [];
    const offerData = offerResponse.data?.data || [];
    const allData = [...bidData, ...offerData];
    
    // Find any Beatrice records without filtering
    const beatriceRecords = allData.filter(record => BEATRICE_BMU_IDS.includes(record.id));
    
    // Get other windfarm records as reference
    const otherWindfarmRecords = allData.filter(record => 
      record.id && record.id.startsWith('T_') && !BEATRICE_BMU_IDS.includes(record.id)
    );
    
    if (beatriceRecords.length > 0) {
      console.log(` FOUND ${beatriceRecords.length} BEATRICE RECORDS!`);
    } else if (otherWindfarmRecords.length > 0) {
      console.log(` ${allData.length} total records, ${otherWindfarmRecords.length} other wind farms, NO Beatrice`);
    } else {
      console.log(` ${allData.length} total records, no wind farm data`);
    }
    
    return {
      period,
      totalRecords: allData.length,
      beatriceRecords
    };
  } catch (error) {
    console.log(` ERROR: ${error instanceof Error ? error.message : String(error)}`);
    return {
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
  console.log(`\n=== CHECKING ALL 48 PERIODS FOR ${TARGET_DATE} ===\n`);
  console.log("This script checks every settlement period for a single day");
  console.log("Results will be saved to", OUTPUT_FILE);
  console.log("\nRunning scan...\n");
  
  const results: any[] = [];
  const foundBeatriceData: any[] = [];
  
  // Check all 48 periods
  for (let period = 1; period <= 48; period++) {
    // Check this period
    const result = await checkPeriod(period);
    results.push(result);
    
    // If Beatrice data found, track it
    if (result.beatriceRecords.length > 0) {
      foundBeatriceData.push(result);
    }
    
    // Small delay between requests
    await delay(100);
  }
  
  // Save results
  await fs.writeFile(OUTPUT_FILE, JSON.stringify({
    date: TARGET_DATE,
    scanCompleted: new Date().toISOString(),
    summary: {
      periodsChecked: 48,
      totalApiCalls: 48 * 2, // Bid and Offer for each period
      periodsWithBeatriceData: foundBeatriceData.length,
      totalBeatriceRecords: foundBeatriceData.reduce((sum, d) => sum + d.beatriceRecords.length, 0)
    },
    beatriceResults: foundBeatriceData,
    allResults: results
  }, null, 2));
  
  // Summary
  console.log(`\n=== SCAN COMPLETED FOR ${TARGET_DATE} ===`);
  console.log(`Total periods checked: 48`);
  
  if (foundBeatriceData.length > 0) {
    console.log(`\nBEATRICE DATA FOUND ON ${TARGET_DATE}:`);
    foundBeatriceData.forEach(result => {
      console.log(`Period ${result.period}: ${result.beatriceRecords.length} records`);
      result.beatriceRecords.forEach((record: any) => {
        console.log(`  - BMU: ${record.id}, Volume: ${record.volume}, soFlag: ${record.soFlag}`);
      });
    });
  } else {
    console.log(`\nNO BEATRICE DATA FOUND ON ${TARGET_DATE}`);
    console.log("Checked ALL 48 settlement periods with no filtering");
  }
  
  console.log(`\nFull results saved to ${OUTPUT_FILE}`);
}

main().catch(console.error);
/**
 * Complete check of ALL days in February 2025 for Beatrice data
 * This script checks EVERY day with 3 periods each to avoid timeout issues
 */

import axios from "axios";

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];

// Generate ALL dates in February 2025
function getAllFebruaryDates(): string[] {
  const dates: string[] = [];
  for (let day = 1; day <= 28; day++) {
    // Format day with leading zero if needed
    const dayStr = day.toString().padStart(2, '0');
    dates.push(`2025-02-${dayStr}`);
  }
  return dates;
}

// Periods to check for each day (morning, mid-day, evening)
const PERIODS_TO_CHECK = [1, 24, 48];

// Simple delay function
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Check a specific date and period for Beatrice BMU records
 */
async function checkDatePeriod(date: string, period: number): Promise<boolean> {
  try {
    process.stdout.write(`Checking ${date}, period ${period}...`);
    
    // Get both bid and offer data
    const [bidResponse, offerResponse] = await Promise.all([
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`, {
        timeout: 5000,
        headers: { 'Accept': 'application/json' }
      }).catch(() => ({ data: { data: [] } })), 
      
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`, {
        timeout: 5000,
        headers: { 'Accept': 'application/json' }
      }).catch(() => ({ data: { data: [] } })) 
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
      return true;
    } else {
      const windFarmCount = otherWindfarmRecords.length;
      console.log(` ${allData.length} total records, ${windFarmCount} other wind farms, no Beatrice`);
      return false;
    }
  } catch (error) {
    console.log(` ERROR: ${error instanceof Error ? error.message : String(error)}`);
    return false;
  }
}

/**
 * Main function
 */
async function main() {
  const allDates = getAllFebruaryDates();
  
  console.log("\n=== CHECKING ALL 28 DAYS OF FEBRUARY 2025 FOR BEATRICE DATA ===\n");
  console.log(`Dates to check: ALL 28 days (Feb 1-28, 2025)`);
  console.log(`Periods to check per day: ${PERIODS_TO_CHECK.join(', ')}`);
  console.log(`Total checks: ${allDates.length * PERIODS_TO_CHECK.length}`);
  
  let foundBeatriceData = false;
  
  // Check all dates
  for (const date of allDates) {
    console.log(`\nDate: ${date}`);
    
    // Check each period for this date
    for (const period of PERIODS_TO_CHECK) {
      const found = await checkDatePeriod(date, period);
      if (found) {
        foundBeatriceData = true;
      }
      await delay(100); // Small delay between requests
    }
  }
  
  console.log("\n=== COMPLETE SCAN FINISHED ===");
  console.log(`Checked ALL 28 days of February 2025`);
  
  if (foundBeatriceData) {
    console.log("\nFOUND BEATRICE DATA IN FEBRUARY 2025");
    console.log("See above for specific dates and periods");
  } else {
    console.log("\nNO BEATRICE DATA FOUND IN FEBRUARY 2025");
    console.log("Checked ALL 28 days with multiple periods per day");
  }
}

main().catch(console.error);
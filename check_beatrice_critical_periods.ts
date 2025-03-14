/**
 * Focused check of critical periods throughout February 2025
 * This script tests multiple dates with specific periods to provide coverage
 * of the entire month while avoiding timeout issues
 */

import axios from "axios";

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];

// Critical dates to check (covering all weeks of February)
const CRITICAL_DATES = [
  "2025-02-01", // Week 1
  "2025-02-08", // Week 2
  "2025-02-15", // Week 3
  "2025-02-22", // Week 4
  "2025-02-28"  // End of month
];

// Critical periods to check (morning, mid-day, evening)
const CRITICAL_PERIODS = [1, 24, 48];

// Simple delay function
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Check a specific date and period for Beatrice BMU records
 */
async function checkDatePeriod(date: string, period: number): Promise<void> {
  try {
    console.log(`\nChecking ${date}, period ${period}...`);
    
    // Get both bid and offer data
    const [bidResponse, offerResponse] = await Promise.all([
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`, {
        timeout: 10000,
        headers: { 'Accept': 'application/json' }
      }).catch(() => ({ data: { data: [] } })), 
      
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`, {
        timeout: 10000,
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
    
    console.log(`Total records: ${allData.length}`);
    console.log(`Other wind farm records: ${otherWindfarmRecords.length}`);
    
    if (beatriceRecords.length > 0) {
      console.log(`FOUND ${beatriceRecords.length} BEATRICE RECORDS:`);
      beatriceRecords.forEach(record => {
        console.log(`  - BMU: ${record.id}, Volume: ${record.volume}, soFlag: ${record.soFlag}`);
      });
    } else {
      console.log(`NO BEATRICE RECORDS FOUND`);
      
      // Print a sample of other wind farm IDs to verify API is working
      if (otherWindfarmRecords.length > 0) {
        const sample = otherWindfarmRecords.slice(0, 5);
        console.log(`Sample wind farm IDs found: ${sample.map(r => r.id).join(', ')}`);
      }
    }
  } catch (error) {
    console.log(`ERROR: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * Main function
 */
async function main() {
  console.log("\n=== CHECKING CRITICAL PERIODS ACROSS FEBRUARY 2025 ===\n");
  console.log(`Dates to check: ${CRITICAL_DATES.join(', ')}`);
  console.log(`Periods to check: ${CRITICAL_PERIODS.join(', ')}`);
  console.log(`Total checks: ${CRITICAL_DATES.length * CRITICAL_PERIODS.length}`);
  
  // Check each critical date and period
  for (const date of CRITICAL_DATES) {
    for (const period of CRITICAL_PERIODS) {
      await checkDatePeriod(date, period);
      await delay(200);
    }
  }
  
  console.log("\n=== SCAN COMPLETED ===");
  console.log("Checked key dates and periods across all of February 2025");
}

main().catch(console.error);
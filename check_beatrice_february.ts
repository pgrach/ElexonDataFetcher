/**
 * Script to check all days in February 2025 for Beatrice wind farm data
 * This performs a comprehensive check of the Elexon API for the entire month
 * using a more direct endpoint that is faster and avoids timeouts
 */

import axios from "axios";
import * as fs from 'fs';

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMUS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];
const LOG_FILE = 'beatrice_february_check.log';

// Function to delay execution to avoid rate limiting
async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Date generator for February 2025
function getDaysInFebruary2025(): string[] {
  const days: string[] = [];
  for (let day = 1; day <= 28; day++) {
    // Format day with leading zero if needed
    const formattedDay = day.toString().padStart(2, '0');
    days.push(`2025-02-${formattedDay}`);
  }
  return days;
}

// Log to both console and file
function log(message: string): void {
  console.log(message);
  fs.appendFileSync(LOG_FILE, message + '\n');
}

// Check a specific BMU for a given date using a more direct endpoint
async function checkBMUForDate(date: string, bmuId: string): Promise<boolean> {
  try {
    // We're using the BOALF endpoint which is a more direct way to check for a specific BMU
    const url = `${ELEXON_BASE_URL}/datasets/BOALF?bmUnit=${bmuId}&settlementDate=${date}`;
    
    const response = await axios.get(url, {
      headers: { 'Accept': 'application/json' },
      timeout: 15000 // Shorter timeout to avoid hanging
    });

    if (response.data && response.data.data && response.data.data.length > 0) {
      log(`✅ FOUND DATA for BMU ${bmuId} on ${date}: ${response.data.data.length} records`);
      
      // Log the first record as an example
      const firstRecord = response.data.data[0];
      log(`   Sample data: Time ${firstRecord.settlementPeriod}, Volume: ${firstRecord.volume}, Price: ${firstRecord.price}`);
      
      return true;
    } else {
      log(`❌ No data for BMU ${bmuId} on ${date}`);
      return false;
    }
  } catch (error: any) {
    if (error.response && error.response.status === 404) {
      log(`❌ No data for BMU ${bmuId} on ${date} (404 Not Found)`);
    } else {
      log(`❗ Error checking BMU ${bmuId} on ${date}: ${error.message}`);
    }
    return false;
  }
}

// Check all Beatrice BMUs for a single date
async function checkAllBMUsForDate(date: string): Promise<number> {
  log(`\n📅 Checking date: ${date}`);
  let foundCount = 0;
  
  for (const bmu of BEATRICE_BMUS) {
    const found = await checkBMUForDate(date, bmu);
    if (found) foundCount++;
    
    // Add a small delay to avoid rate limiting
    await delay(1500);
  }
  
  if (foundCount === 0) {
    log(`❌ No Beatrice data found for ${date}`);
  } else {
    log(`✅ Found data for ${foundCount} Beatrice BMUs on ${date}`);
  }
  
  log('-'.repeat(60));
  return foundCount;
}

// Main function to check all February days sequentially
async function checkAllFebruaryDays(): Promise<void> {
  const februaryDays = getDaysInFebruary2025();
  let totalFoundDays = 0;
  let daysWithData: string[] = [];
  
  // Initialize log file
  fs.writeFileSync(LOG_FILE, `Beatrice Wind Farm February 2025 Check - Started at ${new Date().toISOString()}\n`);
  fs.appendFileSync(LOG_FILE, '-'.repeat(80) + '\n');
  
  log(`🔍 Starting comprehensive check for Beatrice wind farm in February 2025`);
  log(`📊 Will check all ${februaryDays.length} days`);
  
  for (let i = 0; i < februaryDays.length; i++) {
    const date = februaryDays[i];
    log(`\n🔄 Processing day ${i + 1} of ${februaryDays.length}: ${date}`);
    
    const foundCount = await checkAllBMUsForDate(date);
    if (foundCount > 0) {
      totalFoundDays++;
      daysWithData.push(date);
    }
    
    // Progress update
    const percentComplete = Math.round(((i + 1) / februaryDays.length) * 100);
    log(`📈 Progress: ${percentComplete}% complete (${i + 1}/${februaryDays.length} days checked)`);
    
    // Add a delay between days
    await delay(2000);
  }
  
  // Final summary
  log(`\n📊 FINAL SUMMARY FOR FEBRUARY 2025 CHECK:`);
  log(`📆 Total days checked: ${februaryDays.length}`);
  log(`✅ Days with Beatrice data: ${totalFoundDays}`);
  log(`❌ Days without Beatrice data: ${februaryDays.length - totalFoundDays}`);
  
  if (daysWithData.length > 0) {
    log(`\n📆 Days with Beatrice data found:`);
    daysWithData.forEach(date => log(`   - ${date}`));
  } else {
    log(`\n🚫 NO BEATRICE WIND FARM DATA FOUND FOR ANY DAY IN FEBRUARY 2025`);
  }
  
  log(`\n✅ Complete check finished at ${new Date().toISOString()}`);
  log(`📝 Full log saved to ${LOG_FILE}`);
}

// Run the check
console.log("Starting comprehensive check for all days in February 2025...");
checkAllFebruaryDays().then(() => {
  console.log("All checks completed for February 2025");
}).catch(error => {
  console.error("Error in main execution:", error);
});
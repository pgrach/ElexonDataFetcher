/**
 * API Data Check Script
 * 
 * This script fetches data from the Elexon API for a specific date and period,
 * then logs detailed information about the records for debugging.
 */

import { format } from 'date-fns';
import * as fs from 'fs/promises';
import * as path from 'path';

// Import the Elexon fetch function directly from process_all_periods
import { fetchBidsOffers } from './server/services/elexon';

// Global variable to cache the BMU mapping
let bmuMapping: any[] | null = null;

/**
 * Load the BMU mapping file
 */
async function loadBmuMapping(): Promise<any[]> {
  if (bmuMapping) return bmuMapping;
  
  try {
    // Try the server BMU mapping first
    try {
      console.log('Loading BMU mapping from server/data/bmuMapping.json...');
      const mappingFile = await fs.readFile(path.join('server', 'data', 'bmuMapping.json'), 'utf-8');
      bmuMapping = JSON.parse(mappingFile);
      console.log(`Loaded ${Object.keys(bmuMapping).length} BMU mappings`);
      return bmuMapping;
    } catch (serverError) {
      console.warn('Could not load server BMU mapping, falling back to data directory version:', serverError);
      
      // Fallback to the data directory version
      console.log('Loading BMU mapping from data/bmu_mapping.json...');
      const mappingFile = await fs.readFile(path.join('data', 'bmu_mapping.json'), 'utf-8');
      bmuMapping = JSON.parse(mappingFile);
      console.log(`Loaded ${Object.keys(bmuMapping).length} BMU mappings`);
      return bmuMapping;
    }
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

/**
 * Filter for valid wind farm BMUs
 */
async function loadWindFarmIds(): Promise<Set<string>> {
  const mapping = await loadBmuMapping();
  const windFarmIds = new Set<string>();
  
  // Extract the elexonBmUnit IDs from the mapping
  for (const bmu of mapping) {
    if (bmu.elexonBmUnit) {
      windFarmIds.add(bmu.elexonBmUnit);
    }
    
    // Also add nationalGridBmUnit as some IDs may match this format
    if (bmu.nationalGridBmUnit) {
      windFarmIds.add(bmu.nationalGridBmUnit);
    }
  }
  
  console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
  return windFarmIds;
}

/**
 * Main function - Check data for a specific date and period
 */
async function checkApiData(date: string, period: number) {
  try {
    console.log(`\n=== Checking API Data for ${date} P${period} ===\n`);
    
    // Load wind farm IDs for filtering
    const validWindFarmIds = await loadWindFarmIds();
    
    // Fetch data from API
    console.log(`Fetching data from Elexon API...`);
    const records = await fetchBidsOffers(date, period);
    
    // Basic stats
    console.log(`\n--- Basic Statistics ---`);
    console.log(`Total Records: ${records ? records.length : 0}`);
    
    if (!records || records.length === 0) {
      console.log(`No records found for ${date} P${period}`);
      return;
    }
    
    // Count records by filter criteria
    let negativeVolumeCount = 0;
    let flaggedCount = 0;
    let windFarmCount = 0;
    let validRecordsCount = 0;
    
    for (const record of records) {
      const hasNegativeVolume = record.volume < 0;
      const isFlagged = record.soFlag || record.cadlFlag;
      const isValidWindFarm = validWindFarmIds.has(record.id);
      
      if (hasNegativeVolume) negativeVolumeCount++;
      if (isFlagged) flaggedCount++;
      if (isValidWindFarm) windFarmCount++;
      
      if (hasNegativeVolume && isFlagged && isValidWindFarm) {
        validRecordsCount++;
      }
    }
    
    // Log filter counts
    console.log(`\n--- Filter Criteria Counts ---`);
    console.log(`Records with Negative Volume: ${negativeVolumeCount}`);
    console.log(`Records with Flags (SO or CADL): ${flaggedCount}`);
    console.log(`Records with Valid Wind Farm IDs: ${windFarmCount}`);
    console.log(`Records meeting ALL criteria: ${validRecordsCount}`);
    
    // Check if there's a problem with the data
    if (validRecordsCount === 0) {
      console.log(`\n!!! PROBLEM DETECTED: No records meet all filtering criteria !!!`);
      
      // Print sample records for debugging
      console.log(`\n--- Sample Records ---`);
      const sampleSize = Math.min(5, records.length);
      records.slice(0, sampleSize).forEach((record, index) => {
        console.log(`\nRecord ${index + 1}:`);
        console.log(`- ID: ${record.id}`);
        console.log(`- Volume: ${record.volume}`);
        console.log(`- SO Flag: ${record.soFlag}`);
        console.log(`- CADL Flag: ${record.cadlFlag}`);
        console.log(`- Valid Wind Farm: ${validWindFarmIds.has(record.id)}`);
        
        // Calculate payment based on volume and price
        const payment = Math.abs(record.volume) * record.originalPrice * -1;
        console.log(`- Payment: £${payment.toFixed(2)} (calculated)`);
        console.log(`- Original Price: ${record.originalPrice}`);
        console.log(`- Final Price: ${record.finalPrice}`);
        console.log(`- Lead Party: ${record.leadPartyName || 'unknown'}`);
      });
      
      // Find records close to valid (missing only one criteria)
      console.log(`\n--- Near-Valid Records ---`);
      let nearValidFound = false;
      
      for (const record of records) {
        const hasNegativeVolume = record.volume < 0;
        const isFlagged = record.soFlag || record.cadlFlag;
        const isValidWindFarm = validWindFarmIds.has(record.id);
        
        const criteriaCount = [hasNegativeVolume, isFlagged, isValidWindFarm].filter(Boolean).length;
        
        if (criteriaCount === 2) {
          nearValidFound = true;
          console.log(`\nNear-Valid Record:`);
          console.log(`- ID: ${record.id}`);
          console.log(`- Volume: ${record.volume}`);
          console.log(`- SO Flag: ${record.soFlag}`);
          console.log(`- CADL Flag: ${record.cadlFlag}`);
          console.log(`- Valid Wind Farm: ${validWindFarmIds.has(record.id)}`);
          console.log(`- Missing Criteria: ${!hasNegativeVolume ? 'Negative Volume' : !isFlagged ? 'Flags' : 'Valid Wind Farm ID'}`);
        }
      }
      
      if (!nearValidFound) {
        console.log(`No near-valid records found (missing only one criteria)`);
      }
    } else {
      // Log valid records info
      console.log(`\n--- Valid Records Summary ---`);
      const validRecords = records.filter(r => 
        r.volume < 0 && 
        (r.soFlag || r.cadlFlag) && 
        validWindFarmIds.has(r.id)
      );
      
      const totalVolume = validRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      // Calculate payment based on volume and price
      const totalPayment = validRecords.reduce((sum, r) => 
        sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);
      console.log(`Valid Records: ${validRecords.length}`);
      console.log(`Total Volume: ${totalVolume.toFixed(2)} MWh`);
      console.log(`Total Payment: £${totalPayment.toFixed(2)}`);
    }
    
  } catch (error) {
    console.error(`Error checking API data:`, error);
  }
}

/**
 * Main function
 */
async function main() {
  try {
    // Get the date and period from command-line arguments or use defaults
    const date = process.argv[2] || format(new Date(), 'yyyy-MM-dd');
    const period = parseInt(process.argv[3], 10) || 1;
    
    await checkApiData(date, period);
  } catch (error) {
    console.error('Error in check_api_data:', error);
    process.exit(1);
  }
}

main();
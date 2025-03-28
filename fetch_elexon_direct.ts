/**
 * Fetch Elexon Data Directly
 * 
 * This script fetches data directly from the Elexon API for 2025-03-27
 * to verify the true values from the source.
 */

import axios from 'axios';
import fs from 'fs/promises';
import { existsSync } from 'fs';
import path from 'path';

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const date = '2025-03-27';
const LOG_FILE = `elexon_direct_${date}.log`;
// Try both possible locations
const BMU_MAPPING_PATH = existsSync(path.join(process.cwd(), 'data', 'bmu_mapping.json'))
  ? path.join(process.cwd(), 'data', 'bmu_mapping.json')
  : path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');

async function logToFile(message: string): Promise<void> {
  try {
    await fs.appendFile(LOG_FILE, `${message}\n`);
  } catch (error) {
    console.error('Error writing to log file:', error);
  }
}

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Load BMU mappings (wind farm IDs and lead party names)
async function loadBmuMappings(): Promise<Set<string>> {
  try {
    console.log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`);
    
    const data = await fs.readFile(BMU_MAPPING_PATH, 'utf-8');
    const mapping = JSON.parse(data);
    
    // Extract wind farm IDs - filter wind farms
    const filteredBmus = mapping.filter((item: any) => 
      item.fuelType === 'WIND'
    );
    
    // Create a Set of IDs for faster lookups
    const windFarmIds = new Set<string>();
    filteredBmus.forEach((item: any) => {
      windFarmIds.add(item.elexonBmUnit);
    });
    
    console.log(`Found ${windFarmIds.size} wind farm BMUs`);
    return windFarmIds;
  } catch (error) {
    console.error(`Error loading BMU mappings:`, error);
    return new Set();
  }
}

// Get data from Elexon API for all periods
async function fetchAllPeriodsData(): Promise<{
  totalVolume: number;
  totalPayment: number;
  recordCount: number;
}> {
  let totalVolume = 0;
  let totalPayment = 0;
  let recordCount = 0;

  console.log(`Fetching data for all periods on ${date}...`);
  await fs.writeFile(LOG_FILE, `=== Elexon API Data for ${date} ===\n`);
  
  // Load wind farm BMU IDs
  const windFarmIds = await loadBmuMappings();

  for (let period = 1; period <= 12; period++) {
    try {
      console.log(`Processing period ${period}...`);
      
      // Create URLs for bids and offers endpoints
      const bidUrl = `${API_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`;
      const offerUrl = `${API_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`;
      
      await logToFile(`\n--- Period ${period} ---`);
      
      // Execute both requests
      const [bidsResponse, offersResponse] = await Promise.all([
        axios.get(bidUrl, { 
          headers: { 'Accept': 'application/json' },
          timeout: 30000 // 30 second timeout
        }),
        axios.get(offerUrl, { 
          headers: { 'Accept': 'application/json' },
          timeout: 30000 // 30 second timeout
        })
      ]).catch(error => {
        console.error(`Error fetching data for period ${period}:`, error.message);
        return [{ data: { data: [] } }, { data: { data: [] } }];
      });
      
      let periodVolume = 0;
      let periodPayment = 0;
      let periodRecords = 0;
      
      // Process bids response
      if (bidsResponse.data && Array.isArray(bidsResponse.data.data)) {
        // Filter for valid wind farm bids (negative volume, soFlag, and in our mapping)
        const validBids = bidsResponse.data.data.filter((record: any) => 
          record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
        );
        
        validBids.forEach((record: any) => {
          const volume = Math.abs(record.volume);
          const payment = volume * record.originalPrice;
          periodVolume += volume;
          periodPayment += payment;
          periodRecords++;
        });
        
        await logToFile(`Bids: ${validBids.length} records (wind farms only)`);
      }
      
      // Process offers response
      if (offersResponse.data && Array.isArray(offersResponse.data.data)) {
        // Filter for valid wind farm offers (negative volume, soFlag, and in our mapping)
        const validOffers = offersResponse.data.data.filter((record: any) => 
          record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
        );
        
        validOffers.forEach((record: any) => {
          const volume = Math.abs(record.volume);
          const payment = volume * record.originalPrice;
          periodVolume += volume;
          periodPayment += payment;
          periodRecords++;
        });
        
        await logToFile(`Offers: ${validOffers.length} records (wind farms only)`);
      }
      
      await logToFile(`Period ${period} Total: ${periodVolume.toFixed(2)} MWh, £${periodPayment.toFixed(2)}, ${periodRecords} records`);
      
      totalVolume += periodVolume;
      totalPayment += periodPayment;
      recordCount += periodRecords;
      
      // Add a small delay to avoid rate limiting
      await delay(200);
    } catch (error) {
      console.error(`Error processing period ${period}:`, error);
      await logToFile(`Error processing period ${period}: ${error}`);
    }
  }
  
  // Log final totals
  const summary = `\n=== Summary for ${date} ===
  Total Volume: ${totalVolume.toFixed(2)} MWh
  Total Payment: £${totalPayment.toFixed(2)}
  Total Records: ${recordCount}`;
  
  console.log(summary);
  await logToFile(summary);
  
  return {
    totalVolume,
    totalPayment,
    recordCount
  };
}

// Run the script
fetchAllPeriodsData()
  .then((result) => {
    console.log(`Script completed. Results saved to ${LOG_FILE}`);
    process.exit(0);
  })
  .catch((error) => {
    console.error('Unhandled error:', error);
    process.exit(1);
  });
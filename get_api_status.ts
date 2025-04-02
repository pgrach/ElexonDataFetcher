/**
 * API Status Check
 * 
 * This script checks if we can reach the Elexon API for a specific period
 * and verifies if any wind farm data is available
 */

import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// ES module support for __dirname equivalent
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const TARGET_DATE = '2025-03-28';
const TARGET_PERIOD = 11;

// Load BMU mapping for wind farms
async function loadBmuMappings(): Promise<Set<string>> {
  try {
    console.log(`Loading BMU mapping from: ${BMU_MAPPING_PATH}`);
    const data = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(data);
    
    // Extract the Elexon BMU IDs (used to identify wind farms in API responses)
    const windFarmIds = new Set<string>();
    for (const bmu of bmuMapping) {
      windFarmIds.add(bmu.elexonBmUnit);
    }
    
    console.log(`Found ${windFarmIds.size} wind farm BMUs`);
    return windFarmIds;
  } catch (error) {
    console.error(`Error loading BMU mapping: ${error}`);
    return new Set();
  }
}

async function checkApiStatus(): Promise<void> {
  // Load wind farm IDs
  const windFarmIds = await loadBmuMappings();
  
  // Try the actual endpoints used in the processor
  const bidUrl = `${API_BASE_URL}/balancing/settlement/stack/all/bid/${TARGET_DATE}/${TARGET_PERIOD}`;
  const offerUrl = `${API_BASE_URL}/balancing/settlement/stack/all/offer/${TARGET_DATE}/${TARGET_PERIOD}`;
  
  console.log(`\nChecking Elexon API endpoints:`);
  console.log(`Bid URL: ${bidUrl}`);
  console.log(`Offer URL: ${offerUrl}`);
  
  try {
    // Check bid endpoint
    console.log('\nChecking BID endpoint...');
    const bidResponse = await axios.get(bidUrl, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });
    console.log('Bid API response status:', bidResponse.status);
    console.log('Bid data available:', !!bidResponse.data);
    console.log('Bid response data:', bidResponse.data ? 
                (bidResponse.data.data ? `${bidResponse.data.data.length} records` : 'No data array') : 'No data');
    
    // Find wind farm records in bid data
    let bidWindFarmRecords = [];
    if (bidResponse.data && bidResponse.data.data) {
      bidWindFarmRecords = bidResponse.data.data.filter((record: any) => {
        return windFarmIds.has(record.id) && record.volume < 0;
      });
      
      console.log(`Found ${bidWindFarmRecords.length} wind farm records with negative volume in bid data`);
      
      if (bidWindFarmRecords.length > 0) {
        console.log('Sample wind farm bid data (first 2 records):');
        console.log(JSON.stringify(bidWindFarmRecords.slice(0, 2), null, 2));
      }
    }
    
    // Check offer endpoint
    console.log('\nChecking OFFER endpoint...');
    const offerResponse = await axios.get(offerUrl, {
      headers: { 'Accept': 'application/json' },
      timeout: 30000
    });
    console.log('Offer API response status:', offerResponse.status);
    console.log('Offer data available:', !!offerResponse.data);
    console.log('Offer response data:', offerResponse.data ? 
                (offerResponse.data.data ? `${offerResponse.data.data.length} records` : 'No data array') : 'No data');
                
    // Find wind farm records in offer data
    let offerWindFarmRecords = [];
    if (offerResponse.data && offerResponse.data.data) {
      offerWindFarmRecords = offerResponse.data.data.filter((record: any) => {
        return windFarmIds.has(record.id) && record.volume < 0;
      });
      
      console.log(`Found ${offerWindFarmRecords.length} wind farm records with negative volume in offer data`);
      
      if (offerWindFarmRecords.length > 0) {
        console.log('Sample wind farm offer data (first 2 records):');
        console.log(JSON.stringify(offerWindFarmRecords.slice(0, 2), null, 2));
      }
    }
    
    // Report total wind farm records with negative volume
    const totalWindFarmRecords = bidWindFarmRecords.length + offerWindFarmRecords.length;
    console.log(`\nTotal wind farm records with negative volume: ${totalWindFarmRecords}`);
    
    if (totalWindFarmRecords === 0) {
      console.log('\nWarning: No wind farm curtailment records found for this period!');
    }
  } catch (error) {
    console.error('Error accessing Elexon API:', error.message);
    if (error.response) {
      console.error('Status:', error.response.status);
      console.error('Response:', error.response.data);
    }
  }
}

// Execute the function
checkApiStatus().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
/**
 * Script to analyze data from the Bid-Offer Stack endpoint
 * to determine if it's suitable for curtailment data
 */

import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// Get current file directory (ESM equivalent of __dirname)
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const DATE = "2025-03-27";
const PERIOD = 35;
const BMU_MAPPING_PATH = path.join(__dirname, '..', 'server', 'data', 'bmuMapping.json');

// Analyze bid-offer stack data
async function analyzeBidOfferStack() {
  console.log(`Analyzing Bid-Offer Stack data for ${DATE} period ${PERIOD}...\n`);
  
  try {
    // 1. Load BMU mappings to identify wind farms
    console.log("Loading BMU mappings...");
    const bmuMappingData = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(bmuMappingData);
    
    // Create a set of wind farm IDs for quick lookup
    const windFarmIds = new Set();
    const bmuDetails = new Map();
    
    for (const bmu of bmuMapping) {
      if (bmu.elexonBmUnit) {
        windFarmIds.add(bmu.elexonBmUnit);
        bmuDetails.set(bmu.elexonBmUnit, {
          name: bmu.bmUnitName,
          capacity: bmu.generationCapacity,
          leadParty: bmu.leadPartyName
        });
      }
    }
    
    console.log(`Found ${windFarmIds.size} wind farm BMUs in mapping file`);
    
    // 2. Fetch data from the Bid-Offer Stack endpoint
    console.log("\nFetching data from Bid-Offer Stack endpoint...");
    const url = `${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${DATE}/${PERIOD}`;
    const response = await axios.get(url);
    
    if (!response.data || !response.data.data || !Array.isArray(response.data.data)) {
      console.log("Error: Invalid response format");
      return;
    }
    
    const stackData = response.data.data;
    console.log(`Retrieved ${stackData.length} records from Bid-Offer Stack`);
    
    // 3. Analyze the data structure
    if (stackData.length > 0) {
      console.log("\nSample data structure:");
      console.log(JSON.stringify(stackData[0], null, 2));
    }
    
    // 4. Check for wind farm records
    const windFarmRecords = stackData.filter(record => {
      // Assuming the ID field is named similarly to the other endpoint
      return windFarmIds.has(record.bmUnit) || windFarmIds.has(record.bMUnit) || 
             windFarmIds.has(record.id) || windFarmIds.has(record.bmUnitId);
    });
    
    console.log(`\nFound ${windFarmRecords.length} possible wind farm records in the data`);
    
    if (windFarmRecords.length > 0) {
      console.log("\nSample wind farm record:");
      console.log(JSON.stringify(windFarmRecords[0], null, 2));
      
      // If we found records, show what fields would be relevant for curtailment
      if (windFarmRecords.length > 0) {
        const record = windFarmRecords[0];
        console.log("\nRelevant fields for curtailment:");
        
        // ID field - might have different names
        const idField = record.bmUnit || record.bMUnit || record.id || record.bmUnitId;
        console.log(`ID Field: ${idField} (${bmuDetails.get(idField)?.name || 'Unknown'})`);
        
        // Price field - might have different names
        const priceField = record.price || record.bidPrice || record.offerPrice;
        console.log(`Price Field: ${priceField || 'Not found'}`);
        
        // Volume field - might have different names
        const volumeField = record.volume || record.bidVolume || record.offerVolume;
        console.log(`Volume Field: ${volumeField || 'Not found'}`);
      }
    } else {
      console.log("\nNo wind farm records found. Checking for possible ID mismatches...");
      
      // Get unique IDs to help identify potential matches
      const uniqueIds = new Set();
      stackData.forEach(record => {
        const id = record.bmUnit || record.bMUnit || record.id || record.bmUnitId;
        if (id) uniqueIds.add(id);
      });
      
      console.log(`Found ${uniqueIds.size} unique unit IDs in data`);
      console.log("\nSample Unit IDs:");
      
      let count = 0;
      uniqueIds.forEach(id => {
        if (count < 10) {
          console.log(`- ${id}`);
          count++;
        }
      });
    }
    
  } catch (error) {
    console.error("Error during analysis:", error);
  }
}

// Run the analysis
analyzeBidOfferStack().catch(console.error);
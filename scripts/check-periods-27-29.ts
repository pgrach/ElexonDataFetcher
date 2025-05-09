/**
 * Script to specifically check periods 27-29 for May 8th 2025
 * 
 * This focused script only checks the periods where we previously saw curtailment data
 * to troubleshoot why the data might not be getting ingested.
 */

import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Constants
const TARGET_DATE = '2025-05-08';
const PERIODS_TO_CHECK = [27, 28, 29];
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const SERVER_BMU_MAPPING_PATH = path.join(__dirname, "../server/data/bmuMapping.json");
const DATA_BMU_MAPPING_PATH = path.join(__dirname, "../server/data/bmuMapping.json");

// Create a unified set of wind farm BMU IDs from both mapping files
async function getUnifiedWindFarmIds(): Promise<Set<string>> {
  console.log("Loading BMU mappings from multiple sources...");
  
  try {
    // Load server BMU mapping
    console.log(`Reading from ${SERVER_BMU_MAPPING_PATH}`);
    const serverMappingContent = await fs.readFile(SERVER_BMU_MAPPING_PATH, 'utf8');
    const serverBmuMapping = JSON.parse(serverMappingContent);
    const serverWindFarmIds = new Set(
      serverBmuMapping
        .filter((bmu: any) => bmu.fuelType === "WIND")
        .map((bmu: any) => bmu.elexonBmUnit)
    );
    console.log(`Found ${serverWindFarmIds.size} wind farm BMUs in server mapping`);
    
    // Load data BMU mapping (if exists)
    let dataWindFarmIds = new Set<string>();
    try {
      console.log(`Reading from ${DATA_BMU_MAPPING_PATH}`);
      const dataMappingContent = await fs.readFile(DATA_BMU_MAPPING_PATH, 'utf8');
      const dataBmuMapping = JSON.parse(dataMappingContent);
      dataWindFarmIds = new Set(
        dataBmuMapping
          .filter((bmu: any) => bmu.fuelType === "WIND")
          .map((bmu: any) => bmu.elexonBmUnit)
      );
      console.log(`Found ${dataWindFarmIds.size} wind farm BMUs in data mapping`);
    } catch (error) {
      console.log(`Data BMU mapping not found or invalid, using only server mapping`);
    }
    
    // Combine both sets
    const unifiedWindFarmIds = new Set([...serverWindFarmIds, ...dataWindFarmIds]);
    console.log(`Created unified set with ${unifiedWindFarmIds.size} unique wind farm BMUs`);
    
    return unifiedWindFarmIds;
  } catch (error) {
    console.error(`Error loading BMU mappings:`, error);
    throw new Error(`Failed to load BMU mappings: ${error.message}`);
  }
}

/**
 * Make API request with retries
 */
async function makeRequest(url: string): Promise<any> {
  const MAX_RETRIES = 3;
  let retries = 0;
  
  while (retries < MAX_RETRIES) {
    try {
      console.log(`Making API request to: ${url}`);
      const response = await axios.get(url, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000 // 30 second timeout
      });
      
      return response;
    } catch (error) {
      retries++;
      if (retries < MAX_RETRIES) {
        console.log(`Request failed, retrying... (${retries}/${MAX_RETRIES})`);
        await new Promise(resolve => setTimeout(resolve, 5000)); // Wait 5 seconds between retries
      } else {
        throw error;
      }
    }
  }
}

/**
 * Script to check specific periods for curtailment data
 */
async function checkSpecificPeriods() {
  console.log(`\n=== Checking Specific Periods for ${TARGET_DATE} ===\n`);
  
  try {
    const validWindFarmIds = await getUnifiedWindFarmIds();
    
    // Check each period
    for (const period of PERIODS_TO_CHECK) {
      console.log(`\n--- Checking Period ${period} ---`);
      
      // Fetch bids and offers
      const [bidsResponse, offersResponse] = await Promise.all([
        makeRequest(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${TARGET_DATE}/${period}`),
        makeRequest(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${TARGET_DATE}/${period}`)
      ]);
      
      // First, get all records regardless of filtering
      const allBids = bidsResponse.data?.data || [];
      const allOffers = offersResponse.data?.data || [];
      
      console.log(`Raw bids: ${allBids.length}, Raw offers: ${allOffers.length}`);
      
      // Check for all negative volume records
      const negativeBids = allBids.filter((record: any) => record.volume < 0);
      const negativeOffers = allOffers.filter((record: any) => record.volume < 0);
      
      console.log(`Negative volume bids: ${negativeBids.length}, Negative volume offers: ${negativeOffers.length}`);
      
      // Check for records with SO/CADL flags
      const flaggedBids = allBids.filter((record: any) => record.soFlag || record.cadlFlag);
      const flaggedOffers = allOffers.filter((record: any) => record.soFlag || record.cadlFlag);
      
      console.log(`Flagged bids (SO/CADL): ${flaggedBids.length}, Flagged offers (SO/CADL): ${flaggedOffers.length}`);
      
      // Check for wind farm records
      const windFarmBids = allBids.filter((record: any) => validWindFarmIds.has(record.id));
      const windFarmOffers = allOffers.filter((record: any) => validWindFarmIds.has(record.id));
      
      console.log(`Wind farm bids: ${windFarmBids.length}, Wind farm offers: ${windFarmOffers.length}`);
      
      // All filters combined - these are the records we'd actually ingest
      const validBids = allBids.filter((record: any) => 
        record.volume < 0 && 
        (record.soFlag || record.cadlFlag) && 
        validWindFarmIds.has(record.id)
      );
      
      const validOffers = allOffers.filter((record: any) => 
        record.volume < 0 && 
        (record.soFlag || record.cadlFlag) && 
        validWindFarmIds.has(record.id)
      );
      
      console.log(`Valid curtailment bids: ${validBids.length}, Valid curtailment offers: ${validOffers.length}`);
      
      // Display details of any valid curtailment records
      const allValidRecords = [...validBids, ...validOffers];
      
      if (allValidRecords.length > 0) {
        console.log("\nValid curtailment records found:");
        
        for (const record of allValidRecords) {
          console.log(`  - BMU ID: ${record.id}`);
          console.log(`    Volume: ${record.volume} MWh`);
          console.log(`    Original Price: £${record.originalPrice}`);
          console.log(`    SO Flag: ${record.soFlag}`);
          console.log(`    CADL Flag: ${record.cadlFlag || false}`);
          console.log(`    Is in Wind Farm BMU set: ${validWindFarmIds.has(record.id)}`);
          console.log("");
        }
        
        const totalVolume = allValidRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
        const totalPayment = allValidRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice), 0);
        
        console.log(`Period Total: ${totalVolume.toFixed(2)} MWh, £${totalPayment.toFixed(2)}`);
      } else {
        console.log("No valid curtailment records found for this period");
        
        // Look for almost valid records - those that fail just one of our criteria
        console.log("\nChecking for near matches:");
        
        // Wind farm records with negative volume but no flags
        const almostValid1 = allBids.filter((record: any) => 
          record.volume < 0 && 
          validWindFarmIds.has(record.id) &&
          !(record.soFlag || record.cadlFlag)
        );
        
        if (almostValid1.length > 0) {
          console.log(`Found ${almostValid1.length} wind farm records with negative volume but no SO/CADL flags`);
        }
        
        // Records with negative volume and flags but not in wind farm set
        const almostValid2 = allBids.filter((record: any) => 
          record.volume < 0 && 
          (record.soFlag || record.cadlFlag) &&
          !validWindFarmIds.has(record.id)
        );
        
        if (almostValid2.length > 0) {
          console.log(`Found ${almostValid2.length} records with negative volume and flags but not in wind farm set`);
          console.log("BMU IDs:", almostValid2.map((r: any) => r.id).join(", "));
        }
      }
    }
    
    console.log("\n=== Checking Complete ===");
    
  } catch (error) {
    console.error(`\nError during check:`, error);
    process.exit(1);
  }
}

// Run the check
checkSpecificPeriods();
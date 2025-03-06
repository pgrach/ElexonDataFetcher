/**
 * Debug Period 41 - Focused debugging tool for March 5th, 2025, Period 41
 * 
 * This script performs direct API calls to Elexon for Period 41 with optimized handling
 * to prevent timeouts and identify the source of the data discrepancy.
 */

import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';
import { db } from './db';
import { sql } from 'drizzle-orm';

const TARGET_DATE = '2025-03-05';
const TARGET_PERIOD = 41;

// Initialize paths directly
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BMU_MAPPING_PATH = path.join(__dirname, "server/data/bmuMapping.json");

async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    const windFarmIds = new Set(bmuMapping.map((bmu: any) => bmu.elexonBmUnit));
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    return windFarmIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

async function getDirectApiData(): Promise<void> {
  console.log(`\nMaking direct API calls to Elexon for ${TARGET_DATE} period ${TARGET_PERIOD}...`);
  
  try {
    // Get the correct API URLs
    const baseUrl = 'https://data.elexon.co.uk/bmrs/api/v1';
    const bidsUrl = `${baseUrl}/balancing/settlement/stack/all/bid/${TARGET_DATE}/${TARGET_PERIOD}`;
    const offersUrl = `${baseUrl}/balancing/settlement/stack/all/offer/${TARGET_DATE}/${TARGET_PERIOD}`;
    
    console.log(`Bid URL: ${bidsUrl}`);
    console.log(`Offer URL: ${offersUrl}`);
    
    // Load wind farm IDs
    const windFarmIds = await loadWindFarmIds();
    
    // Make separate API calls (not Promise.all) to avoid timeouts
    console.log('Fetching bid data...');
    let validBids: any[] = [];
    try {
      const bidsResponse = await axios.get(bidsUrl, { 
        headers: { 'Accept': 'application/json' },
        timeout: 30000 // 30 second timeout
      });
      console.log('Bids response status:', bidsResponse.status);
      
      if (bidsResponse.data?.data && Array.isArray(bidsResponse.data.data)) {
        validBids = bidsResponse.data.data.filter((record: any) => 
          record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
        );
        console.log(`Valid bid records: ${validBids.length}`);
      } else {
        console.log('Unexpected bids API response format:', bidsResponse.data);
      }
    } catch (error) {
      console.error('Error fetching bid data:', error.message);
    }
    
    console.log('Fetching offer data...');
    let validOffers: any[] = [];
    try {
      const offersResponse = await axios.get(offersUrl, { 
        headers: { 'Accept': 'application/json' },
        timeout: 30000 // 30 second timeout
      });
      console.log('Offers response status:', offersResponse.status);
      
      if (offersResponse.data?.data && Array.isArray(offersResponse.data.data)) {
        validOffers = offersResponse.data.data.filter((record: any) => 
          record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
        );
        console.log(`Valid offer records: ${validOffers.length}`);
      } else {
        console.log('Unexpected offers API response format:', offersResponse.data);
      }
    } catch (error) {
      console.error('Error fetching offer data:', error.message);
    }
    
    const allRecords = [...validBids, ...validOffers];
    console.log(`\nTotal valid records from API: ${allRecords.length}`);
    
    if (allRecords.length > 0) {
      const periodVolume = allRecords.reduce((sum: number, r: any) => sum + Math.abs(r.volume), 0);
      const periodPayment = allRecords.reduce((sum: number, r: any) => 
        sum + (Math.abs(r.volume) * r.originalPrice * -1), 0
      );
      
      console.log(`API Period volume: ${periodVolume.toFixed(2)} MWh`);
      console.log(`API Period payment: £${periodPayment.toFixed(2)}`);
      
      // Count unique farm IDs 
      const uniqueFarmIds = new Set(allRecords.map((r: any) => r.id));
      console.log(`Unique farm IDs: ${uniqueFarmIds.size}`);
      
      // Print sample
      if (allRecords.length > 5) {
        console.log('\nSample records (first 5):');
        allRecords.slice(0, 5).forEach((r: any, index: number) => {
          console.log(`Record ${index + 1}: BMU ID ${r.id}, Volume ${Math.abs(r.volume)} MWh, Price £${r.originalPrice}`);
        });
      }
    } else {
      console.log('No valid records found in API.');
    }
  } catch (error) {
    console.error('Error in API data retrieval:', error);
  }
}

async function getDbData(): Promise<void> {
  console.log(`\nChecking database records for ${TARGET_DATE} period ${TARGET_PERIOD}...`);
  
  try {
    // Query the database for period 41 records
    const records = await db.execute(sql`
      SELECT farm_id, volume, payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE} AND settlement_period = ${TARGET_PERIOD}
    `);
    
    const recordsArray = records as unknown as any[];
    console.log(`Total records in database: ${recordsArray.length}`);
    
    if (recordsArray.length > 0) {
      // Calculate totals
      let totalVolume = 0;
      let totalPayment = 0;
      
      for (const record of recordsArray) {
        totalVolume += parseFloat(record.volume);
        totalPayment += parseFloat(record.payment);
      }
      
      console.log(`Database volume: ${Math.abs(totalVolume).toFixed(2)} MWh`);
      console.log(`Database payment: £${Math.abs(totalPayment).toFixed(2)}`);
      
      // Count unique farm IDs
      const uniqueFarmIds = new Set(recordsArray.map(r => r.farm_id));
      console.log(`Unique farm IDs in database: ${uniqueFarmIds.size}`);
      
      // Sample records
      if (recordsArray.length > 5) {
        console.log('\nSample database records (first 5):');
        recordsArray.slice(0, 5).forEach((r, index) => {
          console.log(`Record ${index + 1}: BMU ID ${r.farm_id}, Volume ${r.volume} MWh, Payment £${r.payment}`);
        });
      }
    } else {
      console.log('No database records found.');
    }
  } catch (error) {
    console.error('Error checking database records:', error);
  }
}

async function main() {
  console.log(`=== Debugging Period 41 for ${TARGET_DATE} ===\n`);
  
  try {
    await getDirectApiData();
    await getDbData();
    console.log('\nDebug complete.');
  } catch (error) {
    console.error('Error in main function:', error);
  }
  // No need to close db connection with Drizzle
}

main().catch(console.error);
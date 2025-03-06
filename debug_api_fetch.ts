/**
 * Debug API fetching for March 5th, 2025
 * 
 * This script fetches data from the Elexon API for a single period with enhanced debugging.
 */

import { fetchBidsOffers } from './server/services/elexon';
import axios from 'axios';

const TARGET_DATE = '2025-03-05';
const TARGET_PERIOD = 41; // We'll check period 41 which had the least records

async function directApiCall() {
  console.log(`Making direct API calls to Elexon for ${TARGET_DATE} period ${TARGET_PERIOD}...`);
  
  try {
    // Get the correct API URLs used by fetchBidsOffers - we need to fetch both bids and offers
    const baseUrl = 'https://data.elexon.co.uk/bmrs/api/v1';
    const bidsUrl = `${baseUrl}/balancing/settlement/stack/all/bid/${TARGET_DATE}/${TARGET_PERIOD}`;
    const offersUrl = `${baseUrl}/balancing/settlement/stack/all/offer/${TARGET_DATE}/${TARGET_PERIOD}`;
    
    console.log(`Bid URL: ${bidsUrl}`);
    console.log(`Offer URL: ${offersUrl}`);
    
    // Load the wind farm IDs for filtering
    const fs = await import('fs/promises');
    const path = await import('path');
    const { fileURLToPath } = await import('url');
    
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = path.default.dirname(__filename);
    const BMU_MAPPING_PATH = path.default.join(__dirname, "server/data/bmuMapping.json");
    
    console.log('Loading BMU mapping from:', BMU_MAPPING_PATH);
    const mappingContent = await fs.default.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    const windFarmIds = new Set(bmuMapping.map((bmu: any) => bmu.elexonBmUnit));
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    
    // Make the API calls
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(bidsUrl, { headers: { 'Accept': 'application/json' } }),
      axios.get(offersUrl, { headers: { 'Accept': 'application/json' } })
    ]);
    
    console.log('Bids response status:', bidsResponse.status);
    console.log('Offers response status:', offersResponse.status);
    
    // Process the responses
    const validBids = bidsResponse.data?.data?.filter((record: any) => 
      record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
    ) || [];
    
    const validOffers = offersResponse.data?.data?.filter((record: any) => 
      record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
    ) || [];
    
    console.log(`Valid bid records: ${validBids.length}`);
    console.log(`Valid offer records: ${validOffers.length}`);
    
    const allRecords = [...validBids, ...validOffers];
    console.log(`Total valid records: ${allRecords.length}`);
    
    if (allRecords.length > 0) {
      const periodVolume = allRecords.reduce((sum: number, r: any) => sum + Math.abs(r.volume), 0);
      const periodPayment = allRecords.reduce((sum: number, r: any) => 
        sum + (Math.abs(r.volume) * r.originalPrice * -1), 0
      );
      
      console.log(`Period volume: ${periodVolume.toFixed(2)} MWh`);
      console.log(`Period payment: £${periodPayment.toFixed(2)}`);
      
      console.log('\nSample records:');
      allRecords.slice(0, 5).forEach((r: any, index: number) => {
        console.log(`Record ${index + 1}:`);
        console.log(`  BMU ID: ${r.id}`);
        console.log(`  Volume: ${Math.abs(r.volume)} MWh`);
        console.log(`  Original Price: £${r.originalPrice}`);
        console.log(`  Final Price: £${r.finalPrice}`);
        console.log(`  Payment: £${Math.abs(r.volume) * r.originalPrice * -1}`);
        console.log(`  SO Flag: ${r.soFlag}`);
        console.log(`  Lead Party: ${r.leadPartyName || 'Unknown'}`);
      });
      
      // Also show the raw data for one record for inspection
      console.log('\nRaw record example:');
      console.log(JSON.stringify(allRecords[0], null, 2));
    } else {
      console.log('No valid records found.');
    }
  } catch (error) {
    console.error('Error making direct API calls:', error);
    if (axios.isAxiosError(error)) {
      console.error('Response data:', error.response?.data);
    }
  }
}

async function fetchUsingService() {
  console.log(`\nFetching using service for ${TARGET_DATE} period ${TARGET_PERIOD}...`);
  
  try {
    const records = await fetchBidsOffers(TARGET_DATE, TARGET_PERIOD);
    console.log(`Total records from service: ${records.length}`);
    
    // Filter to include only curtailed records (soFlag true, volume > 0)
    const curtailedRecords = records.filter(r => r.soFlag && r.volume > 0);
    console.log(`Curtailed records: ${curtailedRecords.length}`);
    
    if (curtailedRecords.length > 0) {
      const periodVolume = curtailedRecords.reduce((sum, r) => sum + r.volume, 0);
      const periodPayment = curtailedRecords.reduce((sum, r) => sum + (r.volume * r.finalPrice), 0);
      
      console.log(`Period volume: ${periodVolume.toFixed(2)} MWh`);
      console.log(`Period payment: £${periodPayment.toFixed(2)}`);
    } else {
      console.log('No curtailed records found.');
    }
  } catch (error) {
    console.error('Error fetching using service:', error);
  }
}

async function checkDbRecords() {
  console.log(`\nChecking database records for ${TARGET_DATE} period ${TARGET_PERIOD}...`);
  
  try {
    const { db } = await import('./db');
    const { sql } = await import('drizzle-orm');
    
    const records = await db.execute(sql`
      SELECT farm_id, volume, payment
      FROM curtailment_records
      WHERE settlement_date = ${TARGET_DATE} AND settlement_period = ${TARGET_PERIOD}
    `);
    
    console.log(`Total records in database: ${records.length}`);
    
    if (records.length > 0) {
      // Calculate totals
      let totalVolume = 0;
      let totalPayment = 0;
      
      for (const record of records) {
        totalVolume += parseFloat(record.volume as string);
        totalPayment += parseFloat(record.payment as string);
      }
      
      console.log(`Database volume: ${Math.abs(totalVolume).toFixed(2)} MWh`);
      console.log(`Database payment: £${Math.abs(totalPayment).toFixed(2)}`);
      
      console.log('\nSample database records:');
      records.slice(0, 5).forEach((r, index) => {
        console.log(`Record ${index + 1}:`);
        console.log(`  BMU: ${r.farm_id}`);
        console.log(`  Volume: ${r.volume} MWh`);
        console.log(`  Payment: ${r.payment}`);
      });
    } else {
      console.log('No database records found.');
    }
  } catch (error) {
    console.error('Error checking database records:', error);
  }
}

async function main() {
  console.log(`=== Debugging API fetch for ${TARGET_DATE} period ${TARGET_PERIOD} ===\n`);
  
  await directApiCall();
  await fetchUsingService();
  await checkDbRecords();
}

main().catch(console.error);
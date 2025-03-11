/**
 * Script to verify details about periods 47-48 for 2025-03-10
 */

import axios from 'axios';
import fs from 'fs/promises';
import path from 'path';
import { fileURLToPath } from 'url';

// Handle ESM module dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Configuration
const API_BASE_URL = 'https://data.elexon.co.uk/bmrs/api/v1';
const BMU_MAPPING_PATH = path.join(__dirname, 'server', 'data', 'bmuMapping.json');
const date = '2025-03-10';
const PERIODS_TO_CHECK = [47, 48];

async function checkPeriod(period: number) {
  console.log(`\nChecking details for ${date} period ${period}...`);
  
  try {
    // First, load all wind farm BMU IDs
    const bmuMappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(bmuMappingContent);
    const windFarmIds = new Set<string>();
    
    for (const bmu of bmuMapping) {
      windFarmIds.add(bmu.id);
    }
    
    console.log(`Loaded ${windFarmIds.size} wind farm BMU IDs`);
    
    // Now fetch data from all endpoints
    console.log(`Fetching from all Elexon API endpoints for period ${period}...`);
    
    const [bidsResponse, offersResponse, acceptedResponse] = await Promise.all([
      axios.get(`${API_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`).catch(() => ({ data: { data: [] } })),
      axios.get(`${API_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`).catch(() => ({ data: { data: [] } })),
      axios.get(`${API_BASE_URL}/balancing/bid-offer/accepted/settlement-period/${period}/settlement-date/${date}`).catch(() => ({ data: { data: [] } }))
    ]);
    
    // Extract data
    const bidsData = bidsResponse.data?.data || [];
    const offersData = offersResponse.data?.data || [];
    const acceptedData = acceptedResponse.data?.data || [];
    
    console.log(`Retrieved ${bidsData.length} bids, ${offersData.length} offers, ${acceptedData.length} accepted records`);
    
    // List IDs in each data set
    const bidIds = new Set(bidsData.map((bid: any) => bid.id));
    const offerIds = new Set(offersData.map((offer: any) => offer.id));
    const acceptedIds = new Set(acceptedData.map((accepted: any) => accepted.id));
    
    console.log(`Unique bid IDs: ${bidIds.size}`);
    console.log(`Unique offer IDs: ${offerIds.size}`);
    console.log(`Unique accepted IDs: ${acceptedIds.size}`);
    
    // Check for wind farm IDs
    const windFarmBidIds = [...bidIds].filter(id => windFarmIds.has(id));
    const windFarmOfferIds = [...offerIds].filter(id => windFarmIds.has(id));
    const windFarmAcceptedIds = [...acceptedIds].filter(id => windFarmIds.has(id));
    
    console.log(`Wind farm bid IDs: ${windFarmBidIds.length > 0 ? windFarmBidIds.join(', ') : 'none'}`);
    console.log(`Wind farm offer IDs: ${windFarmOfferIds.length > 0 ? windFarmOfferIds.join(', ') : 'none'}`);
    console.log(`Wind farm accepted IDs: ${windFarmAcceptedIds.length > 0 ? windFarmAcceptedIds.join(', ') : 'none'}`);
    
    // Now filter for curtailment records
    const validBids = bidsData.filter((record: any) => 
      record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
    );
    
    const validOffers = offersData.filter((record: any) => 
      record.volume < 0 && record.soFlag && windFarmIds.has(record.id)
    );
    
    const validAccepted = acceptedData.filter((record: any) => 
      record.volume < 0 && windFarmIds.has(record.id)
    );
    
    console.log(`Curtailment bid records: ${validBids.length}`);
    console.log(`Curtailment offer records: ${validOffers.length}`);
    console.log(`Curtailment accepted records: ${validAccepted.length}`);
    
    // Check if we have any wind farm records with negative volume but missing soFlag
    const soFlagMissingBids = bidsData.filter((record: any) => 
      record.volume < 0 && !record.soFlag && windFarmIds.has(record.id)
    );
    
    const soFlagMissingOffers = offersData.filter((record: any) => 
      record.volume < 0 && !record.soFlag && windFarmIds.has(record.id)
    );
    
    console.log(`Wind farm bid records with negative volume but missing SO flag: ${soFlagMissingBids.length}`);
    console.log(`Wind farm offer records with negative volume but missing SO flag: ${soFlagMissingOffers.length}`);
    
    // Check if we have any wind farm records with positive volume
    const posVolBids = bidsData.filter((record: any) => 
      record.volume >= 0 && windFarmIds.has(record.id)
    );
    
    const posVolOffers = offersData.filter((record: any) => 
      record.volume >= 0 && windFarmIds.has(record.id)
    );
    
    console.log(`Wind farm bid records with positive volume: ${posVolBids.length}`);
    console.log(`Wind farm offer records with positive volume: ${posVolOffers.length}`);
    
    // Check total number of records with negative volume (from any BMU)
    const negVolBids = bidsData.filter((record: any) => record.volume < 0);
    const negVolOffers = offersData.filter((record: any) => record.volume < 0);
    
    console.log(`Total bid records with negative volume: ${negVolBids.length}`);
    console.log(`Total offer records with negative volume: ${negVolOffers.length}`);
    
    console.log(`\nDetailed analysis for period ${period} completed.`);
    
    return {
      hasCurtailmentData: validBids.length > 0 || validOffers.length > 0 || validAccepted.length > 0
    };
    
  } catch (error) {
    console.error(`Error analyzing period ${period}:`, error);
    return { hasCurtailmentData: false };
  }
}

async function main() {
  console.log(`Starting detailed analysis for periods ${PERIODS_TO_CHECK.join(', ')} on ${date}`);
  
  for (const period of PERIODS_TO_CHECK) {
    const result = await checkPeriod(period);
    
    if (result.hasCurtailmentData) {
      console.log(`Period ${period} HAS curtailment data that should be captured`);
    } else {
      console.log(`Period ${period} does NOT have any curtailment data`);
    }
    
    // Add a delay between periods
    await new Promise(resolve => setTimeout(resolve, 1500));
  }
  
  console.log('\nDetailed analysis completed');
}

// Run the script
main().catch(console.error);
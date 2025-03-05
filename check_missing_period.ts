/**
 * Analyze missing period 16 records for 2025-03-04
 * 
 * This script retrieves data from the Elexon API for period 16 on 2025-03-04
 * to identify missing records and compare expected values with our database.
 */

import axios from 'axios';
import { db } from "./db";
import { curtailmentRecords } from "./db/schema";
import { eq, and } from "drizzle-orm";
import * as fs from 'fs';
import * as path from 'path';

// Wind farms that need to be checked for period 16
interface ElexonBidOffer {
  settlementDate: string;
  settlementPeriod: number;
  id: string;
  bmUnit?: string;
  volume: number;
  soFlag: boolean;
  cadlFlag: boolean | null;
  originalPrice: number;
  finalPrice: number;
  leadPartyName?: string;
}

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchBidsOffers(date: string, period: number): Promise<ElexonBidOffer[]> {
  const url = `https://data.elexon.co.uk/bmrs/api/v1/balancing/bid-offer-acceptance/fixed-period?settlementDate=${date}&settlementPeriod=${period}&format=json`;
  
  try {
    console.log(`Fetching data from Elexon API for ${date} period ${period}...`);
    console.log(`URL: ${url}`);
    
    const response = await axios.get(url);
    console.log(`API Response status: ${response.status}`);
    
    if (response.data && response.data.data) {
      console.log(`Found ${response.data.data.length} records in the response`);
      return response.data.data;
    }
    
    console.log('No data returned from the API');
    return [];
  } catch (error) {
    console.error(`Error fetching from Elexon API: ${error.message}`);
    return [];
  }
}

async function loadWindFarmIds(): Promise<Set<string>> {
  try {
    // Make sure we're looking in the right place
    const filePath = path.resolve('./server/data/bmuMapping.json');
    console.log('Loading BMU mapping from:', filePath);
    
    if (!fs.existsSync(filePath)) {
      console.error(`BMU mapping file not found at ${filePath}`);
      // Try alternate paths
      const alternatives = [
        './data/bmuMapping.json',
        '../server/data/bmuMapping.json',
        '/home/runner/workspace/data/bmuMapping.json'
      ];
      
      for (const alt of alternatives) {
        const altPath = path.resolve(alt);
        console.log(`Trying alternative path: ${altPath}`);
        if (fs.existsSync(altPath)) {
          console.log(`Found BMU mapping at ${altPath}`);
          const bmuMappingFile = await fs.promises.readFile(altPath, 'utf8');
          const bmuMapping = JSON.parse(bmuMappingFile);
          
          const windFarmBMUs = new Set<string>(
            bmuMapping
              .filter((bmu: any) => bmu.assetType === 'WIND')
              .map((bmu: any) => bmu.bmUnitId)
          );
          
          console.log(`Found ${windFarmBMUs.size} wind farm BMUs`);
          return windFarmBMUs;
        }
      }
      
      // If no alternative paths work, create a manual set of known wind farm BMUs
      console.log('Creating manual set of wind farm BMUs');
      const manualWindFarmBMUs = new Set<string>([
        'T_SGRWO-1', 'T_SGRWO-2', 'T_SGRWO-3', 'T_SGRWO-4', 'T_SGRWO-5', 'T_SGRWO-6',
        'T_VKNGW-1', 'T_VKNGW-2', 'T_VKNGW-3', 'T_VKNGW-4',
        'T_GORDW-1', 'T_GORDW-2',
        'T_DOREW-1', 'T_DOREW-2',
        'T_MOWEO-1', 'T_MOWEO-2', 'T_MOWEO-3',
        'T_MOWWO-1', 'T_MOWWO-2', 'T_MOWWO-3', 'T_MOWWO-4',
        'E_BTUIW-3', 'T_HALSW-1', 'T_BROCW-1',
        'T_NNGAO-1', 'T_NNGAO-2',
        'E_BLARW-1', 'T_CUMHW-1', 'T_TWSHW-1'
      ]);
      console.log(`Using ${manualWindFarmBMUs.size} manually defined BMUs`);
      return manualWindFarmBMUs;
    }
    
    const bmuMappingFile = await fs.promises.readFile(filePath, 'utf8');
    const bmuMapping = JSON.parse(bmuMappingFile);
    
    const windFarmBMUs = new Set<string>(
      bmuMapping
        .filter((bmu: any) => bmu.assetType === 'WIND')
        .map((bmu: any) => bmu.bmUnitId)
    );
    
    console.log(`Found ${windFarmBMUs.size} wind farm BMUs`);
    return windFarmBMUs;
  } catch (error) {
    console.error(`Error loading BMU mapping: ${error.message}`);
    
    // Fallback to some known wind farm IDs
    console.log('Using fallback wind farm BMUs');
    const fallbackWindFarmBMUs = new Set<string>([
      'T_SGRWO-1', 'T_SGRWO-2', 'T_SGRWO-3', 'T_SGRWO-4', 'T_SGRWO-5', 'T_SGRWO-6',
      'T_VKNGW-1', 'T_VKNGW-2', 'T_VKNGW-3', 'T_VKNGW-4',
      'T_GORDW-1', 'T_GORDW-2',
      'T_DOREW-1', 'T_DOREW-2',
      'T_MOWEO-1', 'T_MOWEO-2', 'T_MOWEO-3',
      'T_MOWWO-1', 'T_MOWWO-2', 'T_MOWWO-3', 'T_MOWWO-4',
      'E_BTUIW-3', 'T_HALSW-1', 'T_BROCW-1',
      'T_NNGAO-1', 'T_NNGAO-2',
      'E_BLARW-1', 'T_CUMHW-1', 'T_TWSHW-1'
    ]);
    console.log(`Using ${fallbackWindFarmBMUs.size} fallback BMUs`);
    return fallbackWindFarmBMUs;
  }
}

async function checkExistingRecords(date: string, period: number): Promise<void> {
  try {
    // Get records from our database for the specific date and period
    const records = await db.select().from(curtailmentRecords)
      .where(and(
        eq(curtailmentRecords.settlementDate, date),
        eq(curtailmentRecords.settlementPeriod, period)
      ));
    
    console.log(`\nFound ${records.length} records in our database for ${date} period ${period}`);
    
    if (records.length > 0) {
      const totalVolume = records.reduce((sum, record) => sum + Number(record.volume), 0);
      const totalPayment = records.reduce((sum, record) => sum + Number(record.payment), 0);
      const uniqueFarms = new Set(records.map(r => r.farmId)).size;
      
      console.log(`Total volume: ${totalVolume.toFixed(2)} MWh`);
      console.log(`Total payment: £${totalPayment.toFixed(2)}`);
      console.log(`Unique farms: ${uniqueFarms}`);
    }
  } catch (error) {
    console.error(`Error checking existing records: ${error.message}`);
  }
}

async function analyzeElexonData(date: string, period: number): Promise<void> {
  try {
    // Load the wind farm IDs
    const windFarmBMUs = await loadWindFarmIds();
    
    // Fetch data from Elexon API
    const apiRecords = await fetchBidsOffers(date, period);
    
    // Filter records for wind farms only
    const windFarmRecords = apiRecords.filter(record => {
      if (!record.bmUnit) return false;
      // Check if this BMU is in our wind farm BMUs set
      return windFarmBMUs.has(record.bmUnit);
    });
    
    console.log(`\nFound ${windFarmRecords.length} wind farm records in the API response for ${date} period ${period}`);
    
    if (windFarmRecords.length > 0) {
      // Calculate total volume and payment if records found
      const totalVolume = windFarmRecords.reduce((sum, record) => sum + Number(record.volume), 0);
      const totalPayment = windFarmRecords.reduce((sum, record) => sum + Math.abs(Number(record.volume) * Number(record.finalPrice)), 0);
      const uniqueFarms = new Set(windFarmRecords.map(r => r.bmUnit)).size;
      
      console.log(`API Total volume: ${totalVolume.toFixed(2)} MWh`);
      console.log(`API Estimated total payment: £${totalPayment.toFixed(2)}`);
      console.log(`API Unique farms: ${uniqueFarms}`);
      
      // Show all wind farm records for this period
      console.log('\nWind farm records from API:');
      windFarmRecords.forEach(record => {
        console.log(`${record.bmUnit}: ${record.volume.toFixed(2)} MWh, £${(Math.abs(record.volume * record.finalPrice)).toFixed(2)}`);
      });
    }
  } catch (error) {
    console.error(`Error analyzing Elexon data: ${error.message}`);
  }
}

async function checkUnprocessedPeriod16Data(): Promise<void> {
  const date = '2025-03-04';
  const period = 16;
  
  console.log(`\n=== Analyzing missing data for ${date} period ${period} ===\n`);
  
  // Check what's in our database for this period
  await checkExistingRecords(date, period);
  
  // Check what's in the Elexon API for this period
  await analyzeElexonData(date, period);
}

async function main() {
  try {
    await checkUnprocessedPeriod16Data();
  } catch (error) {
    console.error('Error:', error);
  } finally {
    process.exit(0);
  }
}

main();
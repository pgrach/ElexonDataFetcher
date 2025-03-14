/**
 * Check all periods on December 5, 2024 to find all Beatrice curtailment records
 */

import axios from "axios";

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];
const TARGET_DATE = "2024-12-05"; // Date when we already found some Beatrice data

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

/**
 * Get all Beatrice records for the specified period
 */
async function checkPeriod(period: number): Promise<ElexonBidOffer[]> {
  try {
    // Get both bid and offer data
    const [bidResponse, offerResponse] = await Promise.all([
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${TARGET_DATE}/${period}`),
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${TARGET_DATE}/${period}`)
    ]);
    
    // Extract and combine data
    const bidData = bidResponse.data?.data || [];
    const offerData = offerResponse.data?.data || [];
    const allData = [...bidData, ...offerData];
    
    // Find Beatrice records
    return allData.filter(record => BEATRICE_BMU_IDS.includes(record.id));
  } catch (error) {
    console.error(`Error fetching period ${period}: ${error instanceof Error ? error.message : String(error)}`);
    return [];
  }
}

/**
 * Main function
 */
async function main() {
  console.log(`\n=== CHECKING ALL PERIODS FOR ${TARGET_DATE} ===\n`);
  
  const allBeatriceRecords: ElexonBidOffer[] = [];
  
  // Check all 48 settlement periods
  for (let period = 1; period <= 48; period++) {
    process.stdout.write(`Checking period ${period}...`);
    
    const beatriceRecords = await checkPeriod(period);
    
    if (beatriceRecords.length > 0) {
      console.log(` Found ${beatriceRecords.length} Beatrice records!`);
      allBeatriceRecords.push(...beatriceRecords);
    } else {
      console.log(` No Beatrice records.`);
    }
  }
  
  // Summary by BMU
  console.log(`\n=== SUMMARY OF BEATRICE RECORDS FOR ${TARGET_DATE} ===\n`);
  console.log(`Total Beatrice records found: ${allBeatriceRecords.length}`);
  
  // Group by BMU
  const byBmu = BEATRICE_BMU_IDS.map(bmuId => {
    const records = allBeatriceRecords.filter(r => r.id === bmuId);
    const totalVolume = records.reduce((sum, r) => sum + r.volume, 0);
    return { bmuId, recordCount: records.length, totalVolume };
  });
  
  byBmu.forEach(bmu => {
    console.log(`${bmu.bmuId}: ${bmu.recordCount} records, total volume: ${bmu.totalVolume.toFixed(2)} MWh`);
  });
  
  // Show all individual records
  console.log(`\n=== DETAILED BEATRICE RECORDS ===\n`);
  
  // Sort by period and BMU
  allBeatriceRecords.sort((a, b) => {
    if (a.settlementPeriod !== b.settlementPeriod) {
      return a.settlementPeriod - b.settlementPeriod;
    }
    return a.id.localeCompare(b.id);
  });
  
  allBeatriceRecords.forEach(record => {
    console.log(`Period ${record.settlementPeriod}, ${record.id}: ${record.volume.toFixed(2)} MWh, soFlag=${record.soFlag}, price=${record.originalPrice}`);
  });
}

main().catch(console.error);
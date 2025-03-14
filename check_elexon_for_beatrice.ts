/**
 * Script to directly check Elexon API for Beatrice Offshore Windfarm curtailment in February 2025
 * This uses the same approach as in server/services/elexon.ts but focuses only on querying
 * without storing the data in the database
 */

import axios from "axios";

// Constants
const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BEATRICE_BMU_IDS = ['T_BEATO-1', 'T_BEATO-2', 'T_BEATO-3', 'T_BEATO-4'];
const TARGET_DATES = ["2025-02-15", "2025-02-01", "2025-02-28"]; // Beginning, middle, and end of February 2025

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

// Different filter combinations to try
interface FilterCombination {
  name: string;
  filter: (record: any) => boolean;
}

const filterCombinations: FilterCombination[] = [
  {
    name: "Standard filter (Beatrice + soFlag=true + volume<0)",
    filter: (record) => BEATRICE_BMU_IDS.includes(record.id) && record.volume < 0 && record.soFlag === true
  },
  {
    name: "Beatrice + volume<0 (any soFlag)",
    filter: (record) => BEATRICE_BMU_IDS.includes(record.id) && record.volume < 0
  },
  {
    name: "Any Beatrice BMU records (any volume, any flag)",
    filter: (record) => BEATRICE_BMU_IDS.includes(record.id)
  }
];

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function fetchBidsOffers(date: string, period: number): Promise<ElexonBidOffer[]> {
  try {
    console.log(`Fetching data for ${date}, period ${period}...`);
    
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      }),
      axios.get(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`, {
        headers: { 'Accept': 'application/json' },
        timeout: 30000
      })
    ]);
    
    const bids = bidsResponse.data?.data || [];
    const offers = offersResponse.data?.data || [];
    
    return [...bids, ...offers];
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error(`API error: ${error.response?.data?.message || error.message}`);
    } else {
      console.error(`Error: ${error}`);
    }
    return [];
  }
}

async function checkElexon() {
  console.log(`\n=== CHECKING ELEXON API FOR BEATRICE DATA (February 2025) ===\n`);
  
  // More complete set of periods to check
  const periods = [1, 12, 24, 36, 48];
  const results = new Map<string, number>();
  
  // Initialize results
  filterCombinations.forEach(combo => {
    results.set(combo.name, 0);
  });
  
  // Also check a known good date
  const knownGoodDate = "2024-12-01";
  const datesAndPeriods = [
    ...TARGET_DATES.map(date => ({ date, periods })),
    { date: knownGoodDate, periods: [1, 2] } // Fewer periods for known good date
  ];

  for (const { date, periods } of datesAndPeriods) {
    console.log(`\nChecking date: ${date}`);
    
    for (const period of periods) {
      const data = await fetchBidsOffers(date, period);
      
      console.log(`Retrieved ${data.length} total records for period ${period}`);
      
      // Apply each filter combination
      for (const combo of filterCombinations) {
        const filteredData = data.filter(combo.filter);
        const count = filteredData.length;
        
        // Update the result count
        results.set(combo.name, (results.get(combo.name) || 0) + count);
        
        // Print details for any found records
        if (count > 0) {
          console.log(`  [${combo.name}] Found ${count} records for period ${period}:`);
          filteredData.forEach(record => {
            console.log(`    - BMU: ${record.id}, Volume: ${record.volume}, soFlag: ${record.soFlag}, Price: ${record.originalPrice}`);
          });
        }
      }
      
      // Add a short delay between API calls
      await delay(500);
    }
  }
  
  // Summary
  console.log("\n=== SUMMARY ===");
  console.log(`Target dates: ${TARGET_DATES.join(", ")}`);
  console.log(`Reference date: ${knownGoodDate}\n`);
  
  filterCombinations.forEach(combo => {
    console.log(`${combo.name}: ${results.get(combo.name)} records found`);
  });
}

// Run the check
checkElexon().catch(console.error);
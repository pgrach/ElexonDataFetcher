import axios from "axios";
import fs from "fs/promises";
import path from "path";
import { ElexonBidOffer, ElexonResponse } from "../types/elexon";

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";
const BMU_MAPPING_PATH = path.join(process.cwd(), 'server', 'data', 'bmuMapping.json');

let windFarmIds: Set<string> | null = null;

async function loadWindFarmIds(): Promise<Set<string>> {
  if (windFarmIds !== null) {
    return windFarmIds;
  }

  try {
    const mappingContent = await fs.readFile(BMU_MAPPING_PATH, 'utf8');
    const bmuMapping = JSON.parse(mappingContent);
    windFarmIds = new Set(bmuMapping.map((bmu: any) => bmu.elexonBmUnit));
    console.log(`Loaded ${windFarmIds.size} wind farm IDs from mapping`);
    return windFarmIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    // Fallback to default patterns if mapping file is not available
    console.warn('Falling back to default wind farm patterns');
    return new Set(['SGRWO', 'SGRWN', 'SGRE'].map(pattern => `T_${pattern}`));
  }
}

export async function fetchBidsOffers(date: string, period: number): Promise<ElexonBidOffer[]> {
  try {
    // Load wind farm IDs if not already loaded
    const validWindFarmIds = await loadWindFarmIds();

    // Fetch bids and offers in parallel
    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get<ElexonResponse>(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`),
      axios.get<ElexonResponse>(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`)
    ]);

    // Make sure we have data arrays, even if empty
    const bids = bidsResponse.data?.data || [];
    const offers = offersResponse.data?.data || [];

    console.log(`Processing ${bids.length} bids and ${offers.length} offers for ${date} period ${period}`);

    return [...bids, ...offers].filter(record => {
      if (!record || typeof record !== 'object') {
        console.log('Invalid record:', record);
        return false;
      }

      // Check if it's a wind farm based on the mapping
      const isWindFarm = record.id && validWindFarmIds.has(record.id);

      // Check for curtailment (negative volume)
      const isNegativeVolume = typeof record.volume === 'number' && record.volume < 0;

      // Additional logging for debugging
      if (isWindFarm) {
        console.log(`Found wind farm record: ${record.id}, volume: ${record.volume}`);
      }

      return isWindFarm && isNegativeVolume;
    });
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error(`Elexon API error for ${date} period ${period}:`, error.response?.data || error.message);
      throw new Error(`Elexon API error: ${error.response?.data?.error || error.message}`);
    }
    throw error;
  }
}

export async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
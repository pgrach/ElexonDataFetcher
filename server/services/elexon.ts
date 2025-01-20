import axios from "axios";
import { ElexonBidOffer, ElexonResponse } from "../types/elexon";

const ELEXON_BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1";

// Wind farm identifiers based on actual data patterns
const WIND_FARM_PATTERNS = ['SGRWO', 'SGRWN', 'SGRE'];

export async function fetchBidsOffers(date: string, period: number): Promise<ElexonBidOffer[]> {
  try {
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

      // Check if it's a wind farm based on known patterns
      const isWindFarm = record.id && 
        WIND_FARM_PATTERNS.some(pattern => record.id.includes(pattern));

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
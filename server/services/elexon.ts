import { db } from "@db";
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
    return windFarmIds;
  } catch (error) {
    console.error('Error loading BMU mapping:', error);
    throw error;
  }
}

export async function fetchBidsOffers(date: string, period: number): Promise<ElexonBidOffer[]> {
  try {
    const validWindFarmIds = await loadWindFarmIds();
    console.log(`[${date} P${period}] Fetching bids and offers...`);

    const [bidsResponse, offersResponse] = await Promise.all([
      axios.get<ElexonResponse>(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/bid/${date}/${period}`).catch(error => {
        console.error(`[${date} P${period}] Error fetching bids:`, error.response?.data || error.message);
        return { data: { data: [] } };
      }),
      axios.get<ElexonResponse>(`${ELEXON_BASE_URL}/balancing/settlement/stack/all/offer/${date}/${period}`).catch(error => {
        console.error(`[${date} P${period}] Error fetching offers:`, error.response?.data || error.message);
        return { data: { data: [] } };
      })
    ]);

    if (!bidsResponse.data?.data || !offersResponse.data?.data) {
      console.error(`[${date} P${period}] Invalid API response format`);
      return [];
    }

    const bids = bidsResponse.data.data;
    const offers = offersResponse.data.data;

    // Process bids and offers with minimal logging
    const validBids = bids.filter(record => 
      record.volume < 0 && record.soFlag && validWindFarmIds.has(record.id)
    );

    const validOffers = offers.filter(record => 
      record.volume < 0 && record.soFlag && validWindFarmIds.has(record.id)
    );

    const allRecords = [...validBids, ...validOffers];

    // Log only summary information
    if (allRecords.length > 0) {
      const periodTotal = allRecords.reduce((sum, r) => sum + Math.abs(r.volume), 0);
      const periodPayment = allRecords.reduce((sum, r) => sum + (Math.abs(r.volume) * r.originalPrice * -1), 0);
      console.log(`[${date} P${period}] Found ${allRecords.length} valid curtailment records (${validBids.length} bids, ${validOffers.length} offers)`);
      console.log(`[${date} P${period}] Period totals: ${periodTotal.toFixed(2)} MWh, £${periodPayment.toFixed(2)}`);
    }

    return allRecords;
  } catch (error) {
    if (axios.isAxiosError(error)) {
      console.error(`[${date} P${period}] Elexon API error:`, error.response?.data || error.message);
      throw new Error(`Elexon API error: ${error.response?.data?.error || error.message}`);
    }
    console.error(`[${date} P${period}] Unexpected error:`, error);
    throw error;
  }
}

export async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}